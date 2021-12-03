use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    convert::TryInto,
    fs::{self, symlink_metadata, File},
    io::{BufWriter, Read},
    path::PathBuf,
    sync::{
        atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering::*},
        mpsc::{self, channel},
        Arc,
    },
    time::Instant,
};

use anyhow::Result;
use bytesize::ByteSize;
use memmap::Mmap;
use multi_semaphore::Semaphore;
use rusqlite::{params, Connection};
use serde::{ser::SerializeSeq, Serializer};
use threadpool::ThreadPool;

use crate::{
    duplicate_group::DuplicateGroup,
    file_data::FileData,
    file_db::{self, *},
    types::{Basename, Checksum, Deviceno, Directory, Inode, Options, RowId, Size},
    JobId,
};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

// A file is uniquely identified by its device/inode.
type FileId = (Inode, Deviceno);

const SHORT_CHUNK_SIZE: usize = 4096;

/// This trait is an interface that takes a list of lists of potentially duplicate files--for
/// example, if they have the same size but may not match in other ways--and returns a more refined
/// list of lists of potentially duplicate files. For example, groups of files with the same size
/// and the same first bytes.
pub(crate) trait ProcessMatches {
    /// This method refines lists of possible duplicates to smaller lists that pass more checks.
    /// Do not call this directly; call [`process_matches`] instead.
    fn process_matches_(
        candidate_groups: Option<Vec<Vec<RowId>>>,
        conn: &mut Connection,
        options: &Arc<Options>,
    ) -> Result<Box<dyn Iterator<Item = Vec<RowId>>>>;

    /// Wrapper for [`process_matches_`].
    fn process_matches(
        candidate_groups: Option<Vec<Vec<RowId>>>,
        conn: &mut Connection,
        options: &Arc<Options>,
    ) -> Result<Vec<Vec<RowId>>> {
        Ok(Self::process_matches_(candidate_groups, conn, options)?
            .into_iter()
            // It's not a duplicate group if just one file:
            .filter(|files| files.len() > 1)
            .collect())
    }
}

/// Get all files, grouped only by size.
pub(crate) struct GetFiles {}

impl ProcessMatches for GetFiles {
    /// Find all files in the given paths (of the required size) and add their info to the database.
    /// Returns their row IDs.
    fn process_matches_(
        candidate_groups: Option<Vec<Vec<RowId>>>,
        conn: &mut Connection,
        options: &Arc<Options>,
    ) -> Result<Box<dyn Iterator<Item = Vec<RowId>>>> {
        #[cfg(unix)]
        use std::os::unix::prelude::MetadataExt;
        #[cfg(windows)]
        use std::{fs::Metadata, os::windows::fs::MetadataExt};

        assert!(candidate_groups.is_none()); // This is the first stage, there should be no candidate files yet.

        info!("Stage 1: Getting all files and file stat info");

        let start_time = Instant::now();
        let count = Arc::new(AtomicI32::new(0));
        let _handler_guard = {
            let count = Arc::clone(&count);

            options.push_interrupt_handler(move || {
                eprintln!(
                    "\nGetting file/stat info. Processed {} files. {:?} elapsed.",
                    count.load(Relaxed),
                    start_time.elapsed()
                );
            })
        };

        debug!("Finding files in paths");

        let mut row_ids: HashMap<Size, Vec<RowId>> = HashMap::new();

        let mut transaction = Box::new(transaction(conn)?);

        let mut known_directories = HashSet::new();
        let mut dirs_to_explore: Vec<PathBuf> =
            options.starting_paths.clone().into_iter().collect();
        while let Some(dir) = dirs_to_explore.pop() {
            assert!(dir.is_dir());
            trace!("Exploring dir: {:?}", dir);

            let canonical_path = match dir.canonicalize() {
                Ok(canonical_path) => canonical_path,
                Err(err) => {
                    warn!("Could not get canonical path for: {:?}: {}", dir, err);
                    continue;
                }
            };

            if !known_directories.insert(canonical_path) {
                // A directory argument may contain another directory
                debug!("Skipping already known directory: {:?}", dir);
                continue;
            }

            let entries = match fs::read_dir(&dir) {
                Ok(value) => value,
                Err(err) => {
                    warn!("Could not open directory {:?}: {}", dir, err);
                    continue;
                }
            };

            for dir_entry in entries {
                let dir_entry = match dir_entry {
                    Ok(value) => value,
                    Err(err) => {
                        warn!("Could not process path: {}", err);
                        continue;
                    }
                };

                if (count.fetch_add(1, Relaxed) + 1) % 1000 == 0 {
                    // Commit and start a new transaction, in case the operation is interrupted
                    transaction.commit()?;
                    transaction = Box::new(file_db::transaction(conn)?);
                }

                let path = dir_entry.path();

                // NOTE: do not use dir_entry.metadata() because it follows symlinks and will
                // lead to infinite loops. It also doesn't contain all the information if used
                // on Windows.
                let md = match symlink_metadata(&path) {
                    Ok(md) => md,
                    Err(err) => {
                        warn!("Error getting metadata for {:?}: {}", path, err);
                        continue;
                    }
                };

                if md.is_dir() {
                    dirs_to_explore.push(path);
                    continue;
                }

                match dir_entry.file_type() {
                    Ok(file_type) => assert!(!file_type.is_dir()),
                    Err(err) => {
                        warn!("Could not get file type for {:?}: {}", path, err);
                        continue;
                    }
                }

                // Could be: link, pipe, char device, block device, socket, !plain file
                if !md.is_file() {
                    debug!("Skipping irregular file {:?}", path);
                    continue;
                }

                #[cfg(not(any(unix, windows)))]
                compile_error!("Platform must be Unix or Windows.");
                let size = Size(
                    #[cfg(unix)]
                    md.size(),
                    #[cfg(windows)]
                    md.file_size(),
                );

                if options.min_size.0 > 0 && size < options.min_size {
                    trace!("Skipping file {:?}, was only {} bytes.", path, size);
                } else if options.max_size.0 > 0 && size > options.max_size {
                    trace!("Skipping file {:?}, too big at {} bytes.", path, size);
                } else {
                    #[cfg(windows)]
                    fn get_device_no_win(md: &Metadata) -> u64 {
                        md.volume_serial_number()
                            .expect("Metadata must not be created with `DirEntry::metadata`")
                            as u64
                    }
                    let row_id = add_file(
                        &transaction,
                        &Basename::from(&dir_entry.file_name()),
                        &Directory::from(path.parent().unwrap()), // can't be empty; will be "."
                        Inode(
                            #[cfg(unix)]
                            md.ino(),
                            // This needs to be built with nightly on Windows--
                            // https://github.com/rust-lang/rust/issues/63010
                            #[cfg(windows)]
                            md.file_index()
                                .expect("Metadata must not be created with `DirEntry::metadata`"),
                        ),
                        Deviceno(
                            #[cfg(unix)]
                            md.dev(),
                            #[cfg(windows)]
                            get_device_no_win(&md),
                        ),
                        size,
                        options,
                    );
                    match row_id {
                        Ok(row_id) => row_ids.entry(size).or_default().push(row_id),
                        Err(error) => warn!("Could not add file: {}", error),
                    }
                }
            }
        }

        transaction.commit()?;

        debug!(
            "Found {} files while walking tree. Stage 1 took: {:?}",
            row_ids.len(),
            start_time.elapsed()
        );

        Ok(Box::new(row_ids.into_iter().map(|(_key, val)| val)))
    }
}

pub(crate) struct GroupByShortChecksum {}

impl ProcessMatches for GroupByShortChecksum {
    fn process_matches_(
        candidate_groups: Option<Vec<Vec<RowId>>>,
        conn: &mut Connection,
        options: &Arc<Options>,
    ) -> Result<Box<dyn Iterator<Item = Vec<RowId>>>> {
        // Stage 2: new candidates are old candidates with the same md5sum of the
        // first chunk_size bytes. This will prevent the need to check the whole file's
        // md5sum for large files that just happen to have identical size.

        get_store_checksums(
            "Stage 2",
            "short checksums",
            "shortchecksum",
            SHORT_CHUNK_SIZE,
            candidate_groups,
            conn,
            options,
        )
    }
}

pub(crate) struct GroupByFullChecksum {}

impl ProcessMatches for GroupByFullChecksum {
    fn process_matches_(
        candidate_groups: Option<Vec<Vec<RowId>>>,
        conn: &mut Connection,
        options: &Arc<Options>,
    ) -> Result<Box<dyn Iterator<Item = Vec<RowId>>>> {
        // Stage 3: current candidates are those with matching size and short checksum.
        // New groups (not candidates) will be those with matching size and full checksums.

        get_store_checksums(
            "Stage 3",
            "checksums",
            "checksum",
            0,
            candidate_groups,
            conn,
            options,
        )
    }
}

/// Helper method to process and store both short and long checksums.
/// Since their logic is identical. Returns candidate groups based on
/// the checksums.
fn get_store_checksums(
    routine_description: &str,
    checksum_description: &'static str,
    checksum_column_name: &str,
    chunk_size: usize,
    candidate_groups: Option<Vec<Vec<RowId>>>,
    conn: &mut Connection,
    options: &Arc<Options>,
) -> Result<Box<dyn Iterator<Item = Vec<RowId>>>> {
    assert!(candidate_groups.is_some());
    let candidate_groups =
        candidate_groups.expect("get_store_checksums needs to be given an existing list");

    // The techniques used here are hash-based, never based on sorting and comparing equality
    // with the previous file. So the candidate groups can be merged into bigger chunks
    // for more efficient (or easier) parallelization.
    //
    // Flatten and split the candidate groups into bigger batches, but without breaking up any
    // individual candidate group. Breaking up a group could cause us to compute a checksum more than once
    // if the files are hard linked.
    //
    // One single very large batch might use too much memory on very cluttered filesystems,
    // so the size will be limited.
    let total_candidate_count = candidate_groups.iter().flatten().count();
    let candidate_groups =
        candidate_groups
            .into_iter()
            .fold(Vec::new(), |mut accum: Vec<Vec<RowId>>, group| {
                match accum.last_mut() {
                    Some(last) if last.len() + group.len() < 10_000 => {
                        // Extend the previous candidate group, as though these files were also
                        // potential duplicates with them.
                        last.extend(group)
                    }
                    _ => accum.push(group),
                }
                accum
            });

    // Confirm the element count is the same:
    assert!(total_candidate_count == candidate_groups.iter().flatten().count(),);

    info!(
        "{}: Computing the {} of the {} candidates",
        routine_description, checksum_description, total_candidate_count
    );

    let start_time = Instant::now();
    let counter: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    let _handler_guard = {
        let counter = Arc::clone(&counter);
        options.push_interrupt_handler(move || {
            eprintln!(
                "Got and stored {}/{} {}. Elapsed: {:?}",
                counter.load(Relaxed),
                total_candidate_count,
                checksum_description,
                start_time.elapsed()
            );
        })
    };

    let mut checksums = HashMap::new();
    for file_ids in candidate_groups {
        assert!(file_ids.len() > 1);
        trace!(
            "Getting {} candidate files from DB to compute {}",
            file_ids.len(),
            checksum_description
        );
        let loop_start_time = Instant::now();

        let mut need_checksums = Vec::new();
        get_files(conn, &file_ids, options.remember_checksums, |file| {
            trace!("Added candidate file: {:?}", file);
            need_checksums.push(file);
        })?;

        trace!(
            "Constructed {} files from DB rows. {:?} elapsed.",
            file_ids.len(),
            loop_start_time.elapsed()
        );

        checksums.extend(store_checksums(
            &need_checksums,
            Arc::clone(&counter),
            chunk_size,
            checksum_column_name,
            conn,
            options,
        )?);
    }

    let mut candidate_groups: HashMap<Checksum, Vec<RowId>> = HashMap::new();
    for (row_id, checksum) in checksums {
        candidate_groups.entry(checksum).or_default().push(row_id);
    }

    debug!("{} took: {:?}", routine_description, start_time.elapsed());
    Ok(Box::new(
        candidate_groups.into_iter().map(|(_key, val)| val),
    ))
}

/// Compute the checksums of multiple files using threads.
// This function does most of the heavy lifting.
fn store_checksums(
    files: &Vec<FileData>,
    counter: Arc<AtomicU32>,
    chunk_size: usize,
    checksum_column_name: &str,
    conn: &mut Connection,
    options: &Arc<Options>,
) -> Result<HashMap<RowId, Checksum>> {
    // Each element is list of links that refer to the same file.
    // One checksum job is spawned for those hard links.
    let mut files_in_queue: HashMap<JobId, Vec<RowId>> = HashMap::new();

    let pool = ThreadPool::default();
    let io_lock = Arc::new(Semaphore::new(options.max_io_threads as isize)); // Just one thread is fastest when doing I/O

    let available_memory = Arc::new(AtomicU64::new(options.total_buffer_max));
    let (tx, rx) = channel();
    for file in files {
        let tx = tx.clone();
        let counter = Arc::clone(&counter);
        let available_memory = Arc::clone(&available_memory);
        let pair = (file.inode, file.deviceno);
        files_in_queue
            .entry(pair)
            .and_modify(|waitlist| {
                // This inode is already in the job queue
                waitlist.push(file.row_id);
            })
            .or_insert_with(|| {
                // This is a new inode/device combination

                let existing_checksum = if options.remember_checksums {
                    if chunk_size == SHORT_CHUNK_SIZE {
                        file.short_checksum
                    } else {
                        file.checksum
                    }
                } else {
                    None
                };

                if let Some(existing_checksum) = existing_checksum {
                    // Because this file may share an inode with another, it should share the same logic flow.
                    // It's just that the "calculate the checksum" procedure is a noop.
                    trace!("Remembed checksum result: {}", file.row_id);
                    let result = Ok(existing_checksum);
                    counter.fetch_add(1, Relaxed);
                    tx.send((pair, result)).unwrap();
                } else {
                    trace!("Sending checksum job to a thread worker: {}", file.row_id);
                    let path = file.path().unwrap();
                    let io_lock = Arc::clone(&io_lock);
                    let filesize = file.size;
                    let max_io = options.max_io_threads;
                    let options = Arc::clone(options);
                    pool.execute(move || {
                        trace!("Starting job for file {:?}: {:?}", &path, pair);
                        let result = compute_checksum(
                            io_lock,
                            max_io,
                            available_memory,
                            path,
                            filesize,
                            chunk_size,
                            pair,
                            options,
                        );
                        counter.fetch_add(1, Relaxed);
                        tx.send((pair, result)).unwrap();
                    });
                }

                // FIXME: won't this be a problem?
                vec![file.row_id]
            });
    }

    debug!("Now waiting for thread results");

    let mut transaction = Box::new(transaction(conn)?);
    let mut checksum_data = HashMap::new();
    for n in 1..=files_in_queue.len() {
        if n % 1000 == 0 {
            // Commit and start a new transaction, in case the operation is interrupted
            transaction.commit()?;
            transaction = Box::new(file_db::transaction(conn)?);
        }
        trace!("Receiving result {} of {}", n, files_in_queue.len());
        let (pair, result) = rx.recv()?;
        trace!(
            "Got result {} of {} ({})",
            n,
            files_in_queue.len(),
            if result.is_ok() { "okay" } else { "error" }
        );
        let checksum = match result {
            Ok(result) => result,
            Err(_) => continue,
        };

        trace!("Found checksum result {} for job {:?}", checksum, pair);
        let waiting_for_checksum = files_in_queue.get_mut(&pair).unwrap();
        assert!(!waiting_for_checksum.is_empty()); // something should be waiting for this

        for rowid in waiting_for_checksum.iter() {
            checksum_data.insert(*rowid, checksum);
        }

        trace!(
            "Updating {} records for: {:?}",
            waiting_for_checksum.len(),
            pair
        );
        let (inode, deviceno) = pair;
        update_records(
            &transaction,
            inode,
            deviceno,
            &[&checksum_column_name],
            params![checksum],
        )?;
    }
    pool.join();
    assert!(rx.try_recv().is_err()); // the pool should have been finished already, after we got enough data
    transaction.commit()?;

    Ok(checksum_data)
}

// This function includes I/O bound parts (reading from disk) and
// CPU bound parts (computing a checksum).
fn compute_checksum(
    io_lock: Arc<Semaphore>,
    max_io: u32,
    available_memory: Arc<AtomicU64>,
    path: PathBuf,
    filesize: Size,
    chunk_size: usize,
    job_id: FileId,
    options: Arc<Options>,
) -> Result<Checksum, std::io::Error> {
    let read_amount = if chunk_size == 0 {
        filesize.0
    } else {
        min(filesize.0, chunk_size as u64)
    };

    let io_lock_guard = if read_amount >= (1 << 20) / 2 {
        io_lock.access_many(max_io as isize)
    } else {
        io_lock.access()
    };

    let digest = if options.mmap && read_amount >= (1 << 20) {
        let file = File::open(&path)?;
        let mmap = unsafe { Mmap::map(&file) }?;
        blake3::hash(&mmap)
    } else {
        // We should prefer to read files into one big buffer, so we can release the I/O lock
        // quickly. But we can only do that if it wouldn't make the program use too much memory.
        let has_memory = read_amount < options.buffer_max
            && available_memory
                .fetch_update(Acquire, Acquire, |available_memory| {
                    if available_memory > read_amount {
                        Some(available_memory - read_amount)
                    } else {
                        None
                    }
                })
                .is_ok();

        trace!(
            "Not mmapping. Read amount is {}, will read into memory?: {}",
            read_amount,
            has_memory
        );

        if has_memory {
            // We can read the whole file at once
            let mut file = File::open(&path)?;
            let mut buf = vec![0u8; read_amount as usize];
            file.read_exact(&mut buf)?;
            drop(io_lock_guard);
            let digest = blake3::hash(&buf);
            drop(buf);
            available_memory.fetch_add(read_amount, Relaxed);
            digest
        } else {
            let mut hasher = blake3::Hasher::new();
            let buffer_size = 2 * (1 << 20);
            let mut buf = vec![0u8; buffer_size];
            let mut file = File::open(&path)?;
            loop {
                let bytes_read = file.read(&mut buf)?;
                if bytes_read == 0 {
                    break;
                }
                hasher.update_with_join::<blake3::join::RayonJoin>(&buf[0..bytes_read]);
            }
            drop(io_lock_guard);
            hasher.finalize()
        }
    };
    let bytes: [u8; blake3::OUT_LEN] = (*digest.as_bytes())
        .try_into()
        .expect("checksum: wrong length (Infallible)");
    let checksum = Checksum(bytes);
    trace!("Sending successful result for job {:?}", job_id);
    Ok(checksum)
}
pub(crate) struct PrintMatches {}

// This does NOT implement ProcessMatches because the args and return type are different.
impl PrintMatches {
    pub(crate) fn process_matches(
        groups: Vec<Vec<RowId>>, // not candidate_groups--these should be the final matches
        conn: &mut Connection,
        options: &Arc<Options>,
        tx: mpsc::SyncSender<DuplicateGroup>,
    ) -> Result<()> {
        debug!("Processing matches for printing or consolidation.");

        let file_count = Arc::new(AtomicU64::new(0));

        let mut redundant_bytes = 0;

        let _handler_guard = {
            let start_time = Instant::now();
            let file_count = Arc::clone(&file_count);
            options.push_interrupt_handler(move || {
                eprintln!(
                    "\nRead {} files from DB. Elapsed: {:?}",
                    file_count.load(Relaxed),
                    start_time.elapsed()
                )
            })
        };

        let mut ser;
        let mut seq = if let Some(ref save_json_filename) = options.save_json_filename {
            debug!("Writing JSON: {:?}", save_json_filename);
            let file = File::create(save_json_filename)?;
            let writer = BufWriter::new(file);
            ser = serde_json::Serializer::pretty(writer);
            let seq = ser.serialize_seq(None)?;
            Some(seq)
        } else {
            None
        };

        let mut warned_newlines = false;

        for group_ids in groups.iter() {
            // TODO: skip groups with length==1.
            // Drop the fully hard-linked groups:

            let mut group = Vec::new();
            get_files(conn, group_ids, options.remember_checksums, |file| {
                file_count.fetch_add(1, Relaxed);

                // If not writing to JSON, it's more important that the stdout output be correct:
                if !warned_newlines
                    && !seq.is_some()
                    && file.path().unwrap().to_string_lossy().contains("\n")
                {
                    warn!(
                        "One of the duplicate filenames contains a newline. Try --write-json \
                        for parsable output"
                    );
                    warned_newlines = true;
                }

                group.push(file);
            })?;

            if group.len() <= 1 {
                continue; // skip invalid groups--can't be a duplicate
            }

            if let Some(group) = DuplicateGroup::new(group, options)? {
                redundant_bytes += group.redundant_bytes;
                if let Some(ref mut seq) = seq {
                    seq.serialize_element(&group)?;
                }

                if options.print_duplicates {
                    // Display this group of duplicate files:
                    for hardlinked_subgroup in &group.duplicates {
                        if hardlinked_subgroup.len() > 1 {
                            println!("Hard linked duplicates:");
                            print_path_list(hardlinked_subgroup, "\t- ");
                        } else {
                            print_path_list(hardlinked_subgroup, "- ");
                        }
                    }

                    println!(""); // print a newline
                }

                tx.send(group)?;
            }
        }

        if let Some(seq) = seq {
            seq.end()?;
        }

        debug!("Note: got {} files.", file_count.load(Relaxed));
        info!("{} can be reclaimed", ByteSize::b(redundant_bytes));

        Ok(())
    }
}

/// See also warn_path_list().
fn print_path_list(paths: &[PathBuf], prefix: &str) {
    for path in paths {
        if let Some(path) = path.to_str() {
            println!("{}{}", prefix, path);
        } else {
            println!("{}{:?}", prefix, path);
        }
    }
}
