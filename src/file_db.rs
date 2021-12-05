use std::{
    cell::RefCell,
    collections::HashMap,
    path::PathBuf,
    rc::Rc,
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc,
    },
    time::Instant,
};

use anyhow::{anyhow, bail, Context, Result};
use fallible_iterator::FallibleIterator;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use rusqlite::{
    ffi, params, params_from_iter, types::Null, Connection, DropBehavior, Error, ErrorCode,
    OpenFlags, ToSql, Transaction,
};

use crate::{file_data::FileData, options::Options, types::*};

const SCHEMA_VERSION: u32 = 3;

pub(crate) fn init_connection(options: &mut Options) -> anyhow::Result<Connection> {
    let mut conn = if options.db_file == ":memory:" {
        Connection::open_in_memory()
    } else if options.db_must_exist {
        let mut flags: OpenFlags = OpenFlags::default();
        flags.remove(OpenFlags::SQLITE_OPEN_CREATE);
        // Give an error if there's no current DB file:
        Connection::open_with_flags(&options.db_file, flags)
    } else {
        Connection::open(&options.db_file)
    }?;

    let tx = conn.transaction()?;
    tx.execute("PRAGMA encoding = \"UTF-8\"", [])?;

    if options.no_truncate_db && !options.migrate_db {
        // A previous nonempty DB is being used, so we should make sure the schema version is okay
        check_schema_not_old(&tx)?;
    }

    // Create a table for files and a lookup for their directories. Don't add indexes;
    // they are implicit as the "rowid" column. Also note that these types are just annotations--
    // any column can hold any type, so we can store binary path names in TEXT fields.
    for &(table_name, table_spec) in [
        ("directories", "(directory TEXT NOT NULL UNIQUE)"),
        (
            "files",
            "(basename TEXT NOT NULL, dir_id INTEGER64 NOT NULL, \
            inode INTEGER64 NOT NULL, deviceno INTEGER64 NOT NULL, \
            UNIQUE(basename, dir_id))",
        ),
        (
            "metadata",
            "(inode INTEGER64 NOT NULL, deviceno INTEGER64 NOT NULL, size INTEGER64 NOT NULL, \
            shortchecksum BLOB, checksum BLOB, \
            UNIQUE(inode, deviceno))",
        ),
        (
            "global_info",
            // only one row allowed:
            "(id INTEGER PRIMARY KEY CHECK (id = 0), schema_version INTEGER NOT NULL)",
        ),
    ]
    .iter()
    {
        if !options.no_truncate_db {
            tx.execute(&format!("DROP TABLE IF EXISTS {}", table_name), [])?;

            // never truncate twice if experimenting with multiple connections:
            options.no_truncate_db = true;
        }

        let ifnotexists = if !options.no_truncate_db {
            String::from("")
        } else {
            "IF NOT EXISTS".to_string()
        };
        let stmt = format!("CREATE TABLE {} {} {}", ifnotexists, table_name, table_spec);
        trace!("Statement: {}", stmt);
        tx.execute(&stmt, [])?;
    }

    set_schema_version(&tx)?;

    create_indexes(&tx)?;

    tx.commit()?;

    Ok(conn)
}

fn create_indexes(tx: &Transaction) -> Result<()> {
    tx.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_inode ON files (inode)",
        [],
    )?;
    tx.execute(
        "CREATE INDEX IF NOT EXISTS idx_metadata_deviceno ON metadata (deviceno)",
        [],
    )?;
    tx.execute(
        "CREATE INDEX IF NOT EXISTS idx_metadata_inode ON metadata (inode)",
        [],
    )?;
    tx.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_deviceno ON files (deviceno)",
        [],
    )?;
    tx.execute(
        "CREATE INDEX IF NOT EXISTS idx_directories_directory ON directories (directory)",
        [],
    )?;
    tx.execute(
        "CREATE INDEX IF NOT EXISTS idx_metadata_size ON metadata (size)",
        [],
    )?;

    Ok(())
}

// TODO: convert this to return Result, but still assume the newest version
// if `files` doesn't exist or the oldest version if `global_info` doesn't exist.
fn get_schema_version(conn: &Connection) -> u32 {
    // if a prior DB exists, make sure the schema is up to date:

    // Treat errors as nonexistence:
    let files_table_exists = matches!(
        conn.prepare("SELECT EXISTS(SELECT 1 FROM files)")
            .map(|mut stmt| stmt.query_row([], |row| row.get(0))),
        Ok(Ok(true))
    );

    if !files_table_exists {
        // An empty database is a database that's about to be created in the newest schema:
        trace!("files table doesn't exist. This must be a new DB");
        return SCHEMA_VERSION;
    }

    match conn
        .prepare("SELECT schema_version FROM global_info")
        .map(|mut stmt| {
            stmt.query_row([], |row| {
                let schema_version: rusqlite::Result<u32, _> = row.get(0);
                schema_version
            })
        }) {
        Ok(Ok(schema_version)) => {
            trace!("Found schema version: {}", schema_version);
            return schema_version;
        }
        Err(err) | Ok(Err(err)) => {
            // DB errors mean the schema table isn't present--it's an old version
            trace!(
                "Got DB error while getting schema, so assuming it's version 1. {:?}",
                err
            );
            return 1;
        }
    }
}

fn set_schema_version(conn: &Connection) -> Result<()> {
    // make sure the table has one row:
    let _ = conn.execute(
        "INSERT INTO global_info (schema_version) VALUES (?)",
        params![SCHEMA_VERSION],
    );

    // Set the schema version:
    conn.execute(
        &format!("UPDATE global_info SET schema_version = {}", SCHEMA_VERSION),
        [],
    )?;
    Ok(())
}

fn check_schema_not_old(conn: &Connection) -> Result<()> {
    let schema_version = get_schema_version(conn);

    if schema_version > SCHEMA_VERSION {
        bail!(
            "This program is too old to operate on schema {}. Please upgrade or create a new DB.",
            schema_version
        );
    } else if schema_version < SCHEMA_VERSION {
        bail!(
            "The database's schema version {} is too old. Run with --migrate-db to fix.",
            schema_version
        );
    } else {
        trace!("Database schema number matches");
        Ok(())
    }
}

/// Create a transaction with good performance characteristics,
/// and runtime checking for exclusivity.
pub(crate) fn transaction(conn: &Connection) -> Result<Transaction> {
    // Use unchecked_transaction because we can't release a mutable Connection borrow
    // in the middle of a loop when we want to commit and restart a transaction.
    let mut t = conn.unchecked_transaction()?;
    t.set_drop_behavior(DropBehavior::Commit); // We don't roll back transactions, or won't use this function if so
    Ok(t)
}

pub(crate) fn add_file(
    conn: &Transaction,
    basename: &Basename,
    directory: &Directory,
    file_ident: &FileIdent,
    size: Size,
    options: &Options,
) -> Result<()> {
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
            Ok(_) => Ok(conn.last_insert_rowid()),
            Err(Error::SqliteFailure(ffi::Error { code, .. }, ..))
                if code == ErrorCode::ConstraintViolation =>
            {
                // Row exists. This is not a real problem:
                let mut stmt =
                    conn.prepare_cached("SELECT rowid FROM directories WHERE directory = ?")?;
                Ok(stmt.query_row(params![directory], |row| row.get(0))?)
            }
            Err(err) => Err(err).context("Error adding directory"),
        }?;

        let directory_id = DirectoryId(directory_id as u64);

        DIRECTORY_CACHE.with(|cache| {
            cache
                .borrow_mut()
                .insert(directory.0.to_owned(), directory_id)
        });
        Ok(directory_id)
    })?;

    let mut stmt = conn.prepare_cached(
        "INSERT INTO files \
            (basename, dir_id, inode, deviceno) \
            VALUES (?, ?, ?, ?)",
    )?;
    let result = stmt.execute(params![
        basename,
        directory_id,
        file_ident.inode,
        file_ident.deviceno
    ]);
    drop(stmt);

    // Handle file IDs that already exist:
    let file_id = match result {
        Ok(_) => Ok(RowId(conn.last_insert_rowid() as u64)),

        Err(
            _err
            @
            Error::SqliteFailure(
                ffi::Error {
                    code: ErrorCode::ConstraintViolation,
                    ..
                },
                _,
            ),
        ) => {
            //trace!("File row exists. Err: {:?}", _err);

            // Row exists. Get its ID:
            let mut stmt =
                conn.prepare_cached("SELECT rowid FROM files WHERE basename = ?1 AND dir_id = ?2")?;
            Ok(RowId(
                stmt.query_row(params![basename, directory_id.0], |row| row.get(0))?,
            ))
        }
        Err(err) => Err(err), // re-throw other errors
    }?;

    let mut stmt = conn.prepare_cached(
        "INSERT INTO metadata \
        (inode, deviceno, size, shortchecksum, checksum) \
        VALUES (?, ?, ?, ?, ?)",
    )?;
    let result = stmt.execute(params![
        file_ident.inode,
        file_ident.deviceno,
        size.0,
        Null,
        Null
    ]);
    drop(stmt);

    match result {
        Ok(_) => Ok(()),
        Err(
            _err
            @
            Error::SqliteFailure(
                ffi::Error {
                    code: ErrorCode::ConstraintViolation,
                    ..
                },
                _,
            ),
        ) => {
            //trace!("Metadata row exists. Err: {:?}", _err);

            // Note: not cleaning up metadata for old inodes, because it could apply to directories that
            // aren't part of this operation.

            if !options.no_remember_checksums {
                let mut stmt = conn.prepare_cached(
                    "SELECT size FROM metadata WHERE deviceno = ?1 AND inode = ?2",
                )?;
                let db_size = stmt
                    .query_row(params![file_ident.deviceno, file_ident.inode], |row| {
                        Ok(Size(row.get(0)?))
                    })?;
                drop(stmt);

                if size == db_size {
                    trace!("Remembered saved checksum of row: {}", file_id);
                } else {
                    debug!(
                        "Not remembering checksum because file has changed size. {:?}",
                        file_ident
                    );
                    update_metadata(
                        conn,
                        file_ident,
                        &["size", "shortchecksum", "checksum"],
                        params![size.0, Null, Null],
                    )?;
                }
            } else {
                update_metadata(
                    conn,
                    file_ident,
                    &["size", "shortchecksum", "checksum"],
                    params![size.0, Null, Null],
                )?;
            }
            Ok(())
        }
        Err(err) => Err(err), // re-throw other errors
    }?;

    trace!("Inserted row and metadata {:?} for {}", file_ident, file_id);
    Ok(())
}

/// Return the file row IDs of files that contain checksums for this column.
pub(crate) fn get_with_checksum(
    conn: &Connection,
    column_name: &str,
    options: &Options,
) -> Result<Vec<Vec<FileIdent>>> {
    debug!("Reading checksums from DB.");
    //TODO: this should filter based on the directory paths

    let completed_count = Arc::new(AtomicU64::new(0));
    let _handler_guard = {
        let start_time = Instant::now();
        let completed_count = Arc::clone(&completed_count);
        options.push_interrupt_handler(move || {
            eprintln!(
                "\nRead {} checksums from DB. Elapsed: {:?}",
                completed_count.load(Relaxed),
                start_time.elapsed()
            )
        })
    };
    let mut stmt = conn.prepare(&format!(
        "SELECT inode, deviceno, {} FROM metadata WHERE {} IS NOT NULL AND size > ?
            ORDER BY {}",
        column_name, column_name, column_name,
    ))?;

    let rows = stmt.query([options.min_size.0])?.map(|row| {
        let inode = Inode(row.get(0)?);
        let deviceno = Deviceno(row.get(1)?);
        let checksum: Result<Checksum, _> = row.get(2);
        completed_count.fetch_add(1, Relaxed);
        Ok((FileIdent::new(inode, deviceno), checksum))
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
    let groups = rows.fold(
        vec![],
        |mut accum: Vec<Vec<FileIdent>>, (file_ident, checksum)| {
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
                    accum.push(vec![file_ident]);
                    return Ok(accum); // continue, this is the first group
                }
            };

            if checksum == curr_checksum.expect("Checksum must exist at this point") {
                curr_group.push(file_ident);
                accum.push(curr_group);
            } else {
                // curr_group (actually the prev group now) is valid only if the size > 1
                if curr_group.len() > 1 {
                    accum.push(curr_group);
                }

                // There's a new group, and a new current checksum
                curr_checksum = Some(checksum);
                accum.push(vec![file_ident])
            }
            Ok(accum)
        },
    );
    groups.map_err(|err| anyhow!(err))
}

/// Migrate a database from the old format to a newer, more relational format.
pub(crate) fn migrate_db(conn: &mut Connection) -> Result<()> {
    let schema_version = get_schema_version(conn);
    if schema_version == SCHEMA_VERSION {
        trace!(
            "Skipping DB migration since schema is already {}",
            schema_version
        );
        return Ok(());
    }
    trace!("Migrating DB to new format with a separate metadata table");

    let tx = conn.transaction()?;

    tx.execute(
        "INSERT INTO metadata (inode, deviceno, size, shortchecksum, checksum) \
            SELECT DISTINCT inode, deviceno, size, shortchecksum, checksum FROM files",
        [],
    )?;

    // we can't get rid of the old constraints on the old columns, so instead, delete the DB
    // and move the data:
    tx.execute(
        "CREATE TABLE files2 (basename TEXT NOT NULL, dir_id INTEGER64 NOT NULL, \
        inode INTEGER64 NOT NULL, deviceno INTEGER64 NOT NULL, \
        UNIQUE(basename, dir_id))",
        [],
    )?;
    tx.execute(
        "INSERT INTO files2 (basename, dir_id, inode, deviceno) \
        SELECT basename, dir_id, inode, deviceno FROM files",
        [],
    )?;
    tx.execute("DROP TABLE files", [])?;
    tx.execute("ALTER TABLE files2 RENAME TO files", [])?;

    create_indexes(&tx)?; // recreate the indexes

    tx.execute(
        "UPDATE global_info SET schema_version = ?",
        params![SCHEMA_VERSION],
    )?;

    tx.commit()?;
    conn.execute("VACUUM", [])?;

    Ok(())
}

pub(crate) fn get_files<F>(
    conn: &Connection,
    file_idents: &Vec<FileIdent>,
    get_checksums: bool,
    single_file: bool,
    mut callback: F,
) -> Result<()>
where
    F: FnMut(FileData),
{
    trace!(
        "Getting DB rows for {} idents. Single file per inode?: {}",
        file_idents.len(),
        single_file
    );
    //trace!("Row IDs: {:?}", file_rows);

    let count = Rc::new(AtomicU64::new(0));
    let count_ = Rc::clone(&count);
    let callback = |file_data: FileData| {
        debug_assert!(
            file_data.path().is_ok(),
            "File should have been created with path info"
        );

        callback(file_data);
        count_.fetch_add(1, Relaxed);
    };

    if single_file {
        get_files_single(conn, file_idents, get_checksums, callback)?;
    } else {
        get_files_(conn, file_idents, get_checksums, callback)?;
    }

    trace!("Fetched {} rows", count.load(Relaxed));

    Ok(())
}

fn get_files_<F>(
    conn: &Connection,
    file_idents: &[FileIdent],
    get_checksums: bool,
    mut callback: F,
) -> Result<()>
where
    F: FnMut(FileData),
{
    // limit the query to 500 rows. Repeat it if necessary.
    for ids in file_idents[..].chunks(500) {
        let question_marks = n_question_marks(ids.len());
        // TODO: try this with a temporary table instead of building a set of string keys.
        // TODO: try this with multiple queries but being sure files.inode and files.deviceno are indexes.
        // TODO: try this query assuming inodes usually aren't repeated on multiple devices, then afterwards
        //       filter that (deviceno, inode) must match.
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT files.rowid \
            , metadata.inode, metadata.deviceno, metadata.size \
            , directory, basename \
            {} \
            FROM files
            INNER JOIN directories ON files.dir_id = directories.rowid \
            INNER JOIN metadata ON files.inode = metadata.inode AND files.deviceno = metadata.deviceno \
            WHERE files.inode || ',' || files.deviceno IN ({})",
            if get_checksums {
                ", metadata.shortchecksum, metadata.checksum"
            } else {
                ""
            },
            question_marks
        ))?;

        let mut rows = stmt.query(params_from_iter(
            ids.iter()
                .map(|r| format!("{},{}", r.inode.0, r.deviceno.0)),
        ))?;
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
                Deviceno(row.get(2)?),
                Inode(row.get(1)?),
                Size(row.get(3)?),
                short_checksum,
                checksum,
            );
            callback(file);
        }
    }

    Ok(())
}

/// Get files, but only return one file per FileIdent.
fn get_files_single<F>(
    conn: &Connection,
    file_idents: &Vec<FileIdent>,
    get_checksums: bool,
    mut callback: F,
) -> Result<()>
where
    F: FnMut(FileData),
{
    // TODO: compare this to the performance of using some technique to mimic
    // `DISTINCT ON (columns)`: https://www.sisense.com/blog/4-ways-to-join-only-the-first-row-in-sql/
    // and select where inode,device in (?,?,?,...,?).
    for file_ident in file_idents {
        trace!(
            "Getting file info {} for ident: {:?}",
            if get_checksums {
                "with checksums"
            } else {
                "without checksums"
            },
            file_ident
        );

        let mut stmt = conn.prepare_cached(&format!(
            "SELECT files.rowid \
            , metadata.size \
            , directory, basename \
            {} \
            FROM files
            INNER JOIN directories ON files.dir_id = directories.rowid \
            INNER JOIN metadata ON files.inode = metadata.inode AND files.deviceno = metadata.deviceno \
            WHERE files.inode = ? AND files.deviceno = ? \
            LIMIT 1",
            if get_checksums {
                ", metadata.shortchecksum, metadata.checksum"
            } else {
                ""
            },
        ))?;

        stmt.query_row(params![file_ident.inode, file_ident.deviceno], |row| {
            let (short_checksum, checksum) = if get_checksums {
                (row.get(4)?, row.get(5)?)
            } else {
                (None, None)
            };
            let file = FileData::new(
                RowId(row.get(0)?),
                Some(row.get(2)?),
                Some(row.get(3)?),
                file_ident.deviceno,
                file_ident.inode,
                Size(row.get(1)?),
                short_checksum,
                checksum,
            );
            callback(file);
            Ok(())
        })?;
    }
    Ok(())
}

/// Create a string of question marks, such as: ?,?,?,?,?
fn n_question_marks(n: usize) -> String {
    let mut str = "?,".repeat(n);
    str.pop(); // remove last comma
    str
}

/// Update the given fields for all hard linked files (with the same inode/device).
pub(crate) fn update_metadata(
    conn: &Transaction,
    file_ident: &FileIdent,
    fields: &[&str],
    values: &[&dyn ToSql],
) -> Result<()> {
    trace!("Updating {:?} on {:?}.", fields, file_ident);

    assert!(fields.len() > 0);
    assert!(fields.len() == values.len());
    // The inode/deviceno are params 1 and 2, so these will start at 3:
    let update_parts = fields
        .iter()
        .zip(3..)
        .map(|(&field, n)| format!("{} = ?{}", field, n))
        .collect::<Vec<_>>()
        .join(", ");

    let mut stmt = conn.prepare_cached(&format!(
        "UPDATE metadata SET {} WHERE inode = ?1 AND deviceno = ?2",
        update_parts
    ))?;

    let params = params![file_ident.inode, file_ident.deviceno];
    let params = params.into_iter().chain(values.into_iter().map(|val| val));
    let update_count = stmt.execute(params_from_iter(params))?;
    trace!(
        "Updated {:?} for {} rows matching: {:?}",
        fields,
        update_count,
        file_ident,
    );

    Ok(())
}
