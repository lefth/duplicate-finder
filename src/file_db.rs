pub(crate) mod file_db {
    use std::{
        cell::RefCell,
        collections::HashMap,
        path::PathBuf,
        sync::{
            atomic::{AtomicU64, Ordering::Relaxed},
            Arc,
        },
        time::Instant,
    };

    use anyhow::{anyhow, Result};
    use fallible_iterator::FallibleIterator;
    #[allow(unused_imports)]
    use log::{debug, error, info, trace, warn};
    use rusqlite::{
        ffi, params, params_from_iter, types::Null, Connection, DropBehavior, Error, ErrorCode,
        OpenFlags, ToSql, Transaction,
    };

    use crate::{file_data::FileData, types::*};

    pub(crate) fn init_connection(options: &mut Options) -> Result<Connection> {
        let conn = if options.db_file == ":memory:" {
            Connection::open_in_memory()
        } else if options.db_must_exist {
            let mut flags: OpenFlags = OpenFlags::default();
            flags.remove(OpenFlags::SQLITE_OPEN_CREATE);
            // Give an error if there's no current DB file:
            Connection::open_with_flags(&options.db_file, flags)
        } else {
            Connection::open(&options.db_file)
        }?;

        // much bigger cache
        conn.execute("PRAGMA encoding = \"UTF-8\"", [])?;

        // Create a table for files and a lookup for their directories. Don't add indices;
        // they are implicit as the "rowid" column. Also note that these types are just annotations--
        // any column can hold any type, so we can store binary path names in TEXT fields.
        for &(table_name, table_spec) in [("directories", "(directory TEXT NOT NULL UNIQUE)"),
            ("files", "(basename TEXT NOT NULL, dir_id INTEGER64 NOT NULL, inode INTEGER64 NOT NULL, \
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
        options: &Options,
    ) -> Result<RowId> {
        type DirectoryCache = RefCell<HashMap<PathBuf, DirectoryId>>;
        thread_local! {
            // This function is only called from one thread, and cache misses still wouldn't break anything
            static DIRECTORY_CACHE: DirectoryCache = Default::default();
        }

        let directory_id = DIRECTORY_CACHE
            .with(|cache: &DirectoryCache| cache.borrow().get(&directory.0).cloned())
            .ok_or(anyhow!("not found"));

        let directory_id = directory_id.or_else(|_err| -> Result<DirectoryId> {
            let result = {
                let mut dir_stmt = conn.prepare_cached(
                    "INSERT INTO directories \
                        (directory) VALUES (:directory)",
                )?;
                dir_stmt.execute(params![directory])
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
                    statement.query_row(params![directory], |row| row.get(0))?
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
                    "SELECT rowid, size FROM files WHERE basename = ?1 AND dir_id = ?2",
                )?;
                let (row_id, db_size) = statement
                    .query_row(params![basename, directory_id.0], |row| {
                        Ok((RowId(row.get(0)?), Size(row.get(1)?)))
                    })?;
                drop(statement);

                if options.remember_checksums {
                    if size == db_size {
                        trace!("Remembered saved checksum of row: {}", row_id);
                        return Ok(row_id);
                    } else {
                        debug!(
                            "Ignoring --remember-checksums because file has changed size. Id: {}",
                            row_id
                        );
                    }
                }

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

    pub(crate) fn get_matching_checksums(
        conn: &Connection,
        column_name: &str,
        options: &Options,
    ) -> Result<Vec<Vec<RowId>>> {
        debug!("Reading checksums from DB.");

        let total_count = conn.query_row(
            &format!(
                "SELECT COUNT() FROM files
                WHERE {} IS NOT NULL AND size > ?",
                column_name
            ),
            [options.min_size.0],
            |row| row.get::<_, u64>(0),
        )?;
        let completed_count = Arc::new(AtomicU64::new(0));
        let _handler_guard = {
            let start_time = Instant::now();
            let completed_count = Arc::clone(&completed_count);
            options.push_interrupt_handler(move || {
                eprintln!(
                    "\nRead {}/{} checksums from DB. Elapsed: {:?}",
                    completed_count.load(Relaxed),
                    total_count,
                    start_time.elapsed()
                )
            })
        };
        let mut statement = conn.prepare(&format!(
            "SELECT rowid, {} FROM files
                WHERE {} IS NOT NULL AND size > ?
                ORDER BY {}",
            column_name, column_name, column_name,
        ))?;

        let rows = statement.query([options.min_size.0])?.map(|row| {
            let row_id = RowId(row.get(0)?);
            let checksum: Result<Checksum, rusqlite::Error> = row.get(1);
            completed_count.fetch_add(1, Relaxed);
            Ok((row_id, checksum))
        });

        debug!("Grouping matching checksums from DB.");
        let _handler_guard = {
            let start_time = Instant::now();
            options.push_interrupt_handler(move || {
                eprintln!(
                    "\nGrouping matching checksums from DB. Elapsed: {:?}",
                    start_time.elapsed()
                )
            })
        };

        let mut curr_checksum = None;
        let groups = rows.fold(vec![], |mut accum: Vec<Vec<RowId>>, (row_id, checksum)| {
            let checksum = match checksum {
                Ok(checksum) => checksum,
                Err(error) => {
                    warn!("Could not get {}: {}", column_name, error);
                    return Ok(accum); // continue
                }
            };

            let mut curr_group = match accum.pop() {
                Some(curr_row) => curr_row,
                None => {
                    curr_checksum = Some(checksum);
                    accum.push(vec![row_id]);
                    return Ok(accum); // continue, this is the first group
                }
            };

            if checksum
                == curr_checksum.expect("Checksum must exist at this point, if there's a row")
            {
                curr_group.push(row_id);
                accum.push(curr_group);
            } else {
                // curr_group (actually the prev group now) is valid only if the size > 1
                if curr_group.len() > 1 {
                    accum.push(curr_group);
                }

                // There's a new group, and a new current checksum
                curr_checksum = Some(checksum);
                accum.push(vec![row_id])
            }
            Ok(accum)
        });
        groups.map_err(|err| anyhow!(err))
    }

    pub(crate) fn get_files<F>(
        conn: &Connection,
        file_rows: &Vec<RowId>,
        get_checksums: bool,
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
                "SELECT files.rowid \
                            , inode, size, deviceno \
                            , directory, basename \
                            {} \
                            FROM files INNER JOIN directories ON files.dir_id = directories.rowid \
                            WHERE files.rowid IN ({})",
                if get_checksums {
                    ", shortchecksum, checksum"
                } else {
                    ""
                },
                question_marks
            ))?;

            let mut rows = statement.query(params_from_iter(ids.iter().map(|r| r.0)))?;
            while let Some(row) = rows.next()? {
                let (short_checksum, checksum) = if get_checksums {
                    (row.get(6)?, row.get(7)?)
                } else {
                    (None, None)
                };
                let file = FileData::new(
                    RowId(row.get(0)?),
                    Some(row.get(4)?),
                    Some(row.get(5)?),
                    Deviceno(row.get(3)?),
                    Inode(row.get(1)?),
                    Size(row.get(2)?),
                    short_checksum,
                    checksum,
                );
                callback(file);
                count += 1;
            }
        }
        trace!("Fetched {} rows", count);

        // If files failed to be read, we won't be able to get a row with the needed data.
        // This error is useful for testing, but in the real world, file reads do fail:
        debug_assert_eq!(count, file_rows.len()); // XXX

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
