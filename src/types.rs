use std::{
    array::TryFromSliceError,
    collections::HashMap,
    convert::{TryFrom, TryInto},
    ffi::{OsStr, OsString},
    fmt::Display,
    num::ParseIntError,
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Result};
use rusqlite::*;
use serde::{Deserialize, Serialize};
use structopt::*;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::file_data::file_data::FileData;

type HandlerList = Mutex<Vec<Box<dyn Fn() + Send>>>;

/// Options from arguments, and some shared global state.
#[derive(StructOpt)]
#[structopt(about = "Find duplicate files, especially on large filesystems")]
pub(crate) struct Options {
    #[structopt(long, help = "Keep the database file from the previous run")]
    pub no_truncate_db: bool,

    #[structopt(
        short = "m",
        long,
        default_value = "4096",
        help = "Minimum size (bytes) of files to check"
    )]
    pub min_size: Size,

    #[structopt(
        short = "M",
        long,
        default_value = "100000000000",
        help = "Skip files greater than this size, as they are probably not real files. \
            0 bytes means skip nothing"
    )]
    pub max_size: Size, // to prevent accidentally reading something like /proc/kcore.

    #[structopt(default_value = ".", parse(try_from_os_str = get_existing_pathbuf),
        help = "Paths to search for duplicates")]
    pub starting_paths: Vec<PathBuf>,

    #[structopt(
        long,
        default_value = "metadata.sqlite",
        help = "Choose a file, new or existing, for the database. :memory: is a special value"
    )]
    pub db_file: String,

    #[structopt(
        short = "f",
        long,
        help = "Show duplicates that are already fully hardlinked (so no further space savings are possible)"
    )]
    pub show_fully_hardlinked: bool,

    #[structopt(
        short,
        long,
        parse(from_occurrences),
        help = "Reduces level of verbosity"
    )]
    pub quiet: i32,

    #[structopt(
        short,
        long,
        parse(from_occurrences),
        help = "Increases level of verbosity"
    )]
    pub verbose: i32,

    #[structopt(short = "j", long, help = "Print output as JSON")]
    pub print_json: bool,

    #[structopt(short, long, help = "Don't delete the sqlite file. For debugging")]
    pub keep_db_file: bool,

    #[structopt(
        short = "t",
        long,
        default_value = "8",
        parse(try_from_str = get_positive_int),
        help = "How many threads should read small files from disk at a time? \
            Large files use one thread at a time.",
    )]
    pub max_io_threads: u32,

    #[structopt(
        long,
        // The author of rigrep says mmap causes random SIGSEGV or SIGBUS
        // when files are changed during reading. Unlikely
        help = "Use mmap. This increases performance of reading large files on SSDs, \
            but decreases performance on spinning drives. There is also a possibility \
            of crashes when files are modified during reading.",
    )]
    pub mmap: bool,

    // Shared state that's not from program arguments:
    #[structopt(skip)]
    pub interrupt_handlers: Arc<HandlerList>,
}

impl Options {
    /// Set the interrupt handler, and restore the previous one at the end of the scope.
    /// This temporary handler will still exit if there are two interrupts sent in a short time.
    pub fn push_interrupt_handler<F: 'static + Fn() + Send>(&self, handler: F) -> HandlerGuard {
        let mut handlers = self.interrupt_handlers.lock().unwrap();
        handlers.push(Box::new(handler));
        HandlerGuard(Arc::clone(&self.interrupt_handlers))
    }
}

pub(crate) struct HandlerGuard(Arc<HandlerList>);
impl Drop for HandlerGuard {
    /// Restore the previous interrupt handler.
    fn drop(&mut self) {
        let mut lock = self.0.lock().unwrap();
        lock.pop();
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

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy)]
pub(crate) struct Size(pub u64);

impl Display for Size {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u64> for Size {
    fn from(item: u64) -> Self {
        Self(item)
    }
}

impl FromStr for Size {
    type Err = ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>().map(|val| Size(val))
    }
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct RowId(pub u64);

impl Deref for RowId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for RowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy)]
pub(crate) struct Deviceno(pub u64);

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy)]
pub(crate) struct Inode(pub u64);

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy)]
pub(crate) struct DirectoryId(pub u64);

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone)]
pub(crate) struct Directory(pub PathBuf);

impl Deref for Directory {
    type Target = PathBuf;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for Directory {
    fn from(str: String) -> Self {
        Directory(PathBuf::from(str))
    }
}

impl<T: ?Sized + AsRef<OsStr>> From<&T> for Directory {
    fn from(path: &T) -> Self {
        Directory(PathBuf::from(path))
    }
}

impl Directory {
    /// Get the string as a Result instead of Option. Convenient for exception handling.
    pub fn to_str(&self) -> Result<&str> {
        self.0
            .to_str()
            .ok_or_else(|| anyhow!("Could not convert to string: {:#?}", self.0))
    }
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone)]
pub(crate) struct Basename(pub PathBuf);

impl Deref for Basename {
    type Target = PathBuf;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Path> for Basename {
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

impl From<String> for Basename {
    fn from(str: String) -> Self {
        Basename(PathBuf::from(str))
    }
}

impl<T: ?Sized + AsRef<OsStr>> From<&T> for Basename {
    fn from(str: &T) -> Self {
        Basename(PathBuf::from(str))
    }
}

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy)]
pub(crate) struct Checksum(pub [u8; blake3::OUT_LEN]);

impl std::fmt::Display for Checksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0.iter() {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for Checksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"")?;
        let result = std::fmt::Display::fmt(self, f);
        write!(f, "\"")?;
        result
    }
}

impl Deref for Checksum {
    type Target = [u8; blake3::OUT_LEN];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToSql for Checksum {
    fn to_sql(&self) -> rusqlite::Result<types::ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl TryFrom<&[u8]> for Checksum {
    type Error = TryFromSliceError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        value.try_into().map(|bytes| Checksum(bytes))
    }
}

impl TryFrom<Vec<u8>> for Checksum {
    type Error = Vec<u8>;
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        value.try_into().map(|bytes| Checksum(bytes))
    }
}

/// All filenames stored within are duplicates, but they are grouped by device/inode.
/// That way hard linked files are grouped together.
///
/// duplicates[0] is a list of files that are already hard linked together.
/// Likewise, duplicates[1] (if it exists) are hard linked together.
/// But duplicates[0] and duplicates[1] are not hard links with each other.
#[derive(Serialize)]
pub(crate) struct DuplicateGroup {
    pub duplicates: Vec<Vec<String>>,

    pub redundant_bytes: u64,
}

impl DuplicateGroup {
    pub fn new(match_group: Vec<FileData>, options: &Options) -> Result<Option<DuplicateGroup>> {
        type FileId = (Inode, Deviceno);

        assert!(match_group.len() > 1);

        // this may not be actionable, if all files are already linked:
        if !options.show_fully_hardlinked
            && !match_group.iter().skip(1).any(|file| {
                file.inode != match_group[0].inode || file.deviceno != match_group[0].deviceno
            })
        {
            return Ok(None); // no further consolidation is possible
        }

        // Group filenames by hardlinked files, so the user can see what needs to be consolidated
        let mut hardlinked_subgroups = HashMap::<FileId, Vec<String>>::new();
        for file in match_group.iter() {
            let hardlinked_subgroup = hardlinked_subgroups
                .entry((file.inode, file.deviceno))
                .or_default();
            hardlinked_subgroup.push(file.path_str()?);
        }

        let file_size = match_group.first().unwrap().size.0;
        // If the necessary space usage is size, then the redundant space is size * (the number of
        // hard linked groups - 1).
        let redundant_bytes = (hardlinked_subgroups.len() as u64 - 1) * file_size;

        let duplicates = hardlinked_subgroups
            .into_iter()
            .map(|subgroup| subgroup.1)
            .collect();

        Ok(Some(DuplicateGroup {
            duplicates,
            redundant_bytes,
        }))
    }
}
