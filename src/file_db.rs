pub(crate) mod file_db {
    use std::{cell::RefCell, collections::HashMap, convert::TryInto, path::PathBuf};

    use anyhow::{anyhow, Result};
    #[allow(unused_imports)]
    use log::{debug, error, info, trace, warn};
    use rusqlite::{
        ffi, params, params_from_iter, types::Null, Connection, DropBehavior, Error, ErrorCode,
        ToSql, Transaction,
    };

    use crate::{file_data::file_data::FileData, types::*};

    pub(crate) static TABLE_NAME: &str = "files";

    pub(crate) fn init_connection(options: &mut Options) -> Result<Connection> {
        let conn = if options.db_file == ":memory:" {
            Connection::open_in_memory()
        } else {
            Connection::open(&options.db_file)
        }?;

        // much bigger cache
        conn.execute("PRAGMA cache_size = 40000", [])?;

        // Create a table for files and a lookup for their directories.
        // Don't add indices, they are implicit as the "rowid" column:
        for &(table_name, table_spec) in [("directories", "(directory TEXT NOT NULL UNIQUE)"),
            (&TABLE_NAME, "(basename TEXT NOT NULL, dir_id INTEGER64 NOT NULL, inode INTEGER64 NOT NULL, \
                size INTEGER64 NOT NULL, deviceno INTEGER64 NOT NULL, shortchecksum BLOB, checksum BLOB, \
                UNIQUE(basename, dir_id))")].iter()
        {
            if !options.no_truncate_db {
                conn.execute(&format!("DROP TABLE IF EXISTS {}", table_name), [])?;

                // never truncate twice if experimenting with multiple connections:
                options.no_truncate_db = true;
            }

            let ifnotexists = if !options.no_truncate_db { String::from("") } else { "IF NOT EXISTS".to_string() };
            let statement = format!("CREATE TABLE {} {} {}", ifnotexists, table_name, table_spec);
            trace!("Statement: {}", statement);
            conn.execute(&statement, [])?;
        }

        Ok(conn)
    }

    pub(crate) fn transaction(conn: &Connection) -> Result<Transaction> {
        // Use unchecked_transaction because we can't release a mutable Connection borrow
        // in the middle of a loop when we want to commit and restart a transaction.
        let mut t = conn.unchecked_transaction()?;
        t.set_drop_behavior(DropBehavior::Commit); // We don't roll back transactions
        Ok(t)
    }

    pub(crate) fn add_file(
        conn: &Transaction,
        basename: &Basename,
        directory: &Directory,
        inode: Inode,
        deviceno: Deviceno,
        size: Size,
    ) -> Result<RowId> {
        type DirectoryCache = RefCell<HashMap<PathBuf, DirectoryId>>;
        thread_local! {
            // This function is only called from one thread, and cache misses still wouldn't break anything
            static DIRECTORY_CACHE: DirectoryCache = Default::default();
        }
        let basename = basename
            .0
            .to_str()
            .ok_or_else(|| anyhow!("Directory is not a valid string: {:#?}", basename))?;

        let directory_id = DIRECTORY_CACHE
            .with(|cache: &DirectoryCache| cache.borrow().get(&directory.0).cloned())
            .ok_or(anyhow!("not found"));

        let directory_id = directory_id.or_else(|_err| -> Result<DirectoryId> {
            let result = {
                let mut dir_stmt = conn.prepare_cached(
                    "INSERT INTO directories \
                        (directory) VALUES (:directory)",
                )?;
                dir_stmt.execute(params![directory.to_str()?])
            };

            // Handle directory IDs that already exist:
            let directory_id = match result {
                Ok(_) => conn.last_insert_rowid(),
                Err(Error::SqliteFailure(ffi::Error { code, .. }, ..))
                    if code == ErrorCode::ConstraintViolation =>
                {
                    // Row exists. This is not a real problem:
                    let mut statement =
                        conn.prepare_cached("SELECT rowid FROM directories WHERE directory = ?1")?;
                    statement.query_row(params![directory.to_str()?], |row| row.get(0))?
                }
                Err(err) => panic!("Error adding directory: {}", err),
            };

            let directory_id = DirectoryId(directory_id as u64);

            DIRECTORY_CACHE.with(|cache| {
                cache
                    .borrow_mut()
                    .insert(directory.0.to_owned(), directory_id)
            });
            Ok(directory_id)
        })?;

        let mut file_stmt = conn.prepare_cached(
            "INSERT INTO files \
                (basename, dir_id, inode, deviceno, size) \
                VALUES (?, ?, ?, ?, ?)",
        )?;

        let result = file_stmt.execute(params![
            basename,
            directory_id.0,
            inode.0,
            deviceno.0,
            size.0,
        ]);
        drop(file_stmt);

        // Handle file IDs that already exist:
        let file_id = match result {
            Ok(_) => RowId(conn.last_insert_rowid() as u64),

            Err(Error::SqliteFailure(
                ffi::Error {
                    code: ErrorCode::ConstraintViolation,
                    ..
                },
                _,
            )) => {
                // Row exists. Get its ID:
                let mut statement = conn.prepare_cached(
                    "SELECT rowid FROM files WHERE basename = ?1 AND dir_id == ?2",
                )?;
                let row_id = RowId(
                    statement.query_row(params![basename, directory_id.0], |row| row.get(0))?,
                );
                drop(statement);

                // Update fields we know about and null the others, mostly to make debugging easier.
                update_record(
                    conn,
                    row_id,
                    &["inode", "deviceno", "size", "shortchecksum", "checksum"],
                    params![inode.0, deviceno.0, size.0, Null, Null],
                )?;
                row_id
            }
            Err(err) => panic!("Error adding file row: {}", err),
        };

        trace!("Inserted row {}", file_id);

        Ok(file_id)
    }

    pub(crate) fn get_files<F>(
        conn: &Connection,
        file_rows: &Vec<RowId>,
        mut callback: F,
    ) -> Result<()>
    where
        F: FnMut(FileData),
    {
        trace!("Getting DB rows for {} files.", file_rows.len());
        //trace!("Row IDs: {:?}", file_rows);

        let mut count = 0;

        // limit the query to 500 rows. Repeat it if necessary.
        let mut iter = file_rows[..].chunks(500);
        while let Some(ids) = iter.next() {
            let question_marks = n_question_marks(ids.len());
            let mut statement = conn.prepare_cached(&format!(
                "SELECT files.rowid, \
                            inode, size, deviceno, shortchecksum, checksum, \
                            directory, basename \
                            FROM files INNER JOIN directories ON files.dir_id = directories.rowid \
                            WHERE files.rowid IN ({})",
                question_marks
            ))?;

            let mut rows = statement.query(params_from_iter(ids.iter().map(|r| r.0)))?;
            while let Some(row) = rows.next()? {
                let shortchecksum: Option<Checksum> = row
                    .get::<_, Option<Vec<u8>>>(4)
                    .transpose()
                    .map(|bytes| {
                        bytes?
                            .try_into()
                            .map_err(|_| anyhow!("short checksum has wrong byte length"))
                    })
                    .transpose()?;

                let checksum: Option<Checksum> = row
                    .get::<_, Option<Vec<u8>>>(5)
                    .transpose()
                    .map(|bytes| {
                        bytes?
                            .try_into()
                            .map_err(|_| anyhow!("checksum has wrong byte length"))
                    })
                    .transpose()?;

                let file = FileData::new(
                    RowId(row.get(0)?),
                    Some(Directory::from(row.get::<_, String>(6)?)),
                    Some(Basename::from(row.get::<_, String>(7)?)),
                    Deviceno(row.get(3)?),
                    Inode(row.get(1)?),
                    Size(row.get(2)?),
                    shortchecksum,
                    checksum,
                );
                callback(file);
                count += 1;
            }
        }
        trace!("Fetched {} rows", count);

        // If files failed to be read, we won't be able to get a row with the needed data.
        // This error is useful for testing, but in the real world, file reads do fail:
        //if cfg!(debug_assertions) {
        debug_assert!(count == file_rows.len());

        Ok(())
    }

    fn n_question_marks(n: usize) -> String {
        let mut str = "?,".repeat(n);
        str.pop(); // remove last comma
        str
    }

    pub(crate) fn update_record(
        conn: &Transaction,
        id: RowId,
        fields: &[&str],
        values: &[&dyn ToSql],
    ) -> Result<()> {
        trace!("Updating {:?} on row {}.", fields, id);

        assert!(fields.len() > 0);
        assert!(fields.len() == values.len());
        // The row ID is param 1, so these will start at 2:
        let update_parts = fields
            .iter()
            .zip(2..)
            .map(|(&field, n)| format!("{} = ?{}", field, n))
            .collect::<Vec<_>>()
            .join(", ");

        let mut statement = conn.prepare_cached(&format!(
            "UPDATE files SET {} WHERE rowid = ?1",
            update_parts
        ))?;

        let params = std::iter::once::<&dyn ToSql>(&id.0).chain(values.iter().map(|val| *val));
        statement.execute(params_from_iter(params))?;

        Ok(())
    }
}
