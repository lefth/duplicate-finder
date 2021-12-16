use std::{
    ffi::OsString,
    io::{self, BufRead},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Result};
use globset::{Glob, GlobSet};
use structopt::*;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::types::Size;

type HandlerList = Mutex<Vec<Box<dyn Fn() + Send>>>;

#[derive(StructOpt)]
#[structopt()]
/// Find duplicate files, especially on large filesystems
pub struct Options {
    #[structopt(long)]
    /// Keep the database file from the previous run
    pub no_truncate_db: bool,

    #[structopt(long)]
    /// Resume a previous operation at stage 3: this computes any necessary full checksums,
    /// based on candidates (with matching short checksums) in an existing database file.
    /// Implies --no-truncate-db
    pub resume_stage3: bool,

    #[structopt(long)]
    /// Resume a previous operation at stage 4: this prints results based on checksums that
    /// have already been computed, and consolidates the duplicates if requested.
    /// Implies --no-truncate-db
    pub resume_stage4: bool,

    #[structopt(short = "m", long, default_value = "4096")]
    /// Minimum size (bytes) of files to check
    pub min_size: Size,

    #[structopt(short = "M", long, default_value = "100000000000")]
    /// Skip files greater than this size, as they are probably not real files.
    /// 0 bytes means skip nothing
    pub max_size: Size, // to prevent accidentally reading something like /proc/kcore.

    #[structopt(default_value = ".", parse(try_from_os_str = get_existing_pathbuf))]
    /// Paths to search for duplicates
    pub starting_paths: Vec<PathBuf>,

    #[structopt(long, default_value = "metadata.sqlite")]
    /// Choose a file, new or existing, for the database. :memory: is a special value
    pub db_file: String,

    #[structopt(short = "f", long)]
    /// Show duplicates that are already fully hardlinked (so no further space savings are
    /// possible)
    pub show_fully_hardlinked: bool,

    /// Migrate a saved DB from version 0.0.1 or 0.0.2 to the current format.
    /// Implies --no-truncate-db, as well as --keep-db if used without another operation.
    #[structopt(long)]
    pub migrate_db: bool,

    #[structopt(short, long)]
    /// Print files that are duplicated on disk, but not already hard linked.
    pub print_duplicates: bool,

    #[structopt(short, long, parse(from_occurrences))]
    /// Reduces level of verbosity.
    pub quiet: i32,

    #[structopt(short, long, parse(from_occurrences))]
    /// Increases level of verbosity
    pub verbose: i32,

    #[structopt(short = "j", long = "write-json")]
    /// Save output to a file as JSON
    pub save_json_filename: Option<OsString>,

    /// Attempt to hard link duplicate files to reclaim space. This is a testing feature
    /// and you should back up your files before using it.
    #[structopt(long)]
    pub consolidate: bool,

    /// Don't consolidate files, but print what would be done.
    #[structopt(long, short = "n")]
    pub dry_run: bool,

    #[structopt(short, long)]
    /// Don't delete the sqlite file after operations are complete.
    pub keep_db: bool,

    #[structopt(
        short = "t",
        long,
        default_value = "8",
        parse(try_from_str = get_positive_int),
    )]
    /// How many threads should read small files from disk at a time?
    /// Large files use one thread at a time.
    pub max_io_threads: u32,

    #[structopt(long)]
    /// Use mmap. This increases performance of reading large files on SSDs,
    /// but decreases performance on spinning drives. There is also a possibility
    /// of crashes when files are modified during reading.
    // The author of rigrep says mmap causes random SIGSEGV or SIGBUS
    // when files are changed during reading. Unlikely to be an issue.
    pub mmap: bool,

    /// Tell the program how much memory it can use as buffers for reading files. This is
    /// not necessary if using --mmap since large files won't be read to buffers anyway.
    /// Small buffers are allowed but will slow down operation: --buffer-megabytes 0.2
    #[structopt(long)]
    pub buffer_megabytes: Option<f64>,

    /// Don't disable extra logger info on release builds. Undocumented option for debugging.
    #[structopt(long, hidden = true)]
    pub log: bool,

    /// Don't show any prompts. Hidden option, not recommended for anything but profiling.
    #[structopt(long, hidden = true)]
    pub no_prompt: bool,

    /// Don't redo checksums that are already stored in a database.
    /// Useful for resuming an operation without knowing at what stage it stopped,
    /// or adding additional paths to an operation that was completed.
    /// This option only makes sense with --no-truncate-db.
    #[structopt(long)]
    pub no_remember_checksums: bool,

    #[structopt(long)]
    /// Continue consolidation even if there are other linked files that were not detected.
    /// This means space will not be saved in some cases, but that's a necessity if running
    /// with a backup copy (the backup copy normally being created with hard links).
    pub allow_incomplete_consolidation: bool,

    #[structopt(long)]
    /// This option can be passed more than once. These paths should be excluded from duplicate
    /// checking and consolidation. These are glob patterns,
    /// where "**" matches zero or more directories, "*" matches a part of a path component,
    /// and "?" matches a single character.
    // See further rules at: https://docs.rs/globset/latest/globset/index.html
    exclude_: Vec<OsString>,
    #[structopt(skip)]
    pub exclude: Option<GlobSet>,

    // Shared state that's not from program arguments:
    #[structopt(skip)]
    pub interrupt_handlers: Arc<HandlerList>,

    #[structopt(skip)]
    pub db_must_exist: bool,

    #[structopt(skip)]
    /// The max total buffer memory for reading all files
    pub total_buffer_max: u64,

    #[structopt(skip)]
    /// The max buffer for reading a file
    pub buffer_max: u64,
}

impl Options {
    /// Set the interrupt handler, and restore the previous one at the end of the scope.
    /// This temporary handler will still exit if there are two interrupts sent in a short time.
    #[must_use = "Must keep the guard or the interrupt handler will immediately be popped."]
    pub fn push_interrupt_handler<F: 'static + Fn() + Send>(&self, handler: F) -> HandlerGuard {
        let mut handlers = self.interrupt_handlers.lock().unwrap();
        handlers.push(Box::new(handler));
        HandlerGuard(Arc::clone(&self.interrupt_handlers))
    }

    /// Get the number of operations requested by the user
    pub fn operation_count(&self) -> u8 {
        self.consolidate as u8
            + self.print_duplicates as u8
            + self.migrate_db as u8
            + self.save_json_filename.is_some() as u8
    }

    /// Check for errors and make needed automatic changes due to implications from different options.
    pub fn validate(&mut self) -> Result<()> {
        // Parse the exclude list into glob patterns (but don't expand them):
        let mut glob_set = globset::GlobSetBuilder::new();
        for pattern in self.exclude_.iter() {
            if let Some(pattern) = pattern.to_str() {
                match Glob::new(pattern) {
                    Ok(glob) => {
                        glob_set.add(glob);
                    }
                    Err(err) => {
                        bail!("Could not process --exclude: {}", err);
                    }
                }
            } else {
                bail!("Could not process --exclude as UTF-8 path: {:?}", pattern);
            }
        }
        match glob_set.build() {
            Ok(glob_set) => self.exclude = Some(glob_set),
            Err(err) => bail!("Error building exclude patterns: {}", err),
        }

        if self.resume_stage3 || self.resume_stage4 {
            self.no_truncate_db = true;
            self.db_must_exist = true;
        }

        if self.no_remember_checksums && !self.no_truncate_db {
            info!("--no-remember-checksums has no effect since --no-truncate-db was not used.");
            self.no_truncate_db = true;
        }

        if self.no_truncate_db && !self.keep_db {
            eprintln!("Do you really want to remember keep the previous database now but delete it afterwards? [y/N]");
            if !self.no_prompt && {
                let input_line = io::stdin().lock().lines().next().unwrap().unwrap();
                !input_line.to_lowercase().starts_with("y")
            } {
                std::process::exit(1);
            }
        }

        if self.dry_run {
            if self.consolidate {
                info!("--dry-run requested, consolidation will not change files.");
            } else {
                warn!("--dry-run being ignored, since consolidation was not requested.");
            }
        }

        if self.migrate_db {
            self.db_must_exist = true;
            if !self.no_truncate_db {
                info!("Assuming --no-truncate-db since --migrate-db was used.");
                self.no_truncate_db = true;
            }
            if !self.keep_db && self.operation_count() == 1 {
                info!("Assuming --keep-db since --migrate-db was used without another operation.");
                self.keep_db = true;
            }
        }

        if self.operation_count() == 0 {
            bail!(
                "No operation chosen! Must use at least one of: \
                    --consolidate, --print-duplicates, --write-json <filename>, --migrate-db"
            );
        }

        // TODO: Tune these parameters. We don't want to run out of memory.
        // We don't want to give all buffers the same size since we may have memory to spare
        // and few large files. We don't want one very large file to prevent any other files
        // from being read into a buffer.
        // And all the I/O should be reevaluated after blake3 gets a parallel API that does not
        // thrash on spinning hard drives.
        self.total_buffer_max = if let Some(buffer_megabytes) = self.buffer_megabytes {
            (buffer_megabytes * (1 << 20) as f64) as u64
        } else if cfg!(target_arch = "x86_64") {
            4_294_967_296
        } else {
            10 * 1 << 20
        };
        self.buffer_max = self.total_buffer_max / 3;

        Ok(())
    }
}

fn get_existing_pathbuf(path: &std::ffi::OsStr) -> Result<PathBuf, OsString> {
    match std::fs::metadata(path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                Ok(PathBuf::from(path))
            } else {
                Err(OsString::from(format!(
                    "{} is not a directory",
                    path.to_string_lossy()
                )))
            }
        }
        Err(err) => Err(OsString::from(err.to_string())),
    }
}

fn get_positive_int(s: &str) -> Result<u32, String> {
    match s.parse::<u32>() {
        Ok(val) if val > 0 => Ok(val),
        _ => Err(format!("{}: Value must be a number greater than 0", s)),
    }
}

pub struct HandlerGuard(Arc<HandlerList>);
impl Drop for HandlerGuard {
    /// Restore the previous interrupt handler.
    fn drop(&mut self) {
        let mut lock = self.0.lock().unwrap();
        lock.pop();
    }
}
