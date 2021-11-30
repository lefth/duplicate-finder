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

#[cfg(unix)]
use std::os::unix::prelude::{OsStrExt, OsStringExt};
#[cfg(windows)]
use std::os::windows::ffi::{OsStrExt, OsStringExt};

#[cfg(windows)]
use anyhow::bail;

use anyhow::Result;
use rusqlite::{
    types::{FromSql, FromSqlError, ToSqlOutput, Value, ValueRef},
    *,
};
use serde::{Deserialize, Serialize};
use structopt::*;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::file_data::file_data::FileData;

type HandlerList = Mutex<Vec<Box<dyn Fn() + Send>>>;

#[derive(StructOpt)]
#[structopt()]
/// Find duplicate files, especially on large filesystems
pub(crate) struct Options {
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

    #[structopt(short, long, parse(from_occurrences))]
    /// Reduces level of verbosity. `-qqq` will suppress printing duplicate files.
    /// The computation will still happen, and JSON may still be saved.
    // (In debug mode it would be `-qqqq` for total silence)
    pub quiet: i32,

    #[structopt(short, long, parse(from_occurrences))]
    /// Increases level of verbosity
    pub verbose: i32,

    #[structopt(short = "j", long = "write_json")]
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
    /// Don't delete the sqlite file. For debugging
    pub keep_db_file: bool,

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

    pub fn init(&mut self) {
        if self.resume_stage3 || self.resume_stage4 {
            self.no_truncate_db = true;
            self.db_must_exist = true;
        }

        if self.dry_run {
            if self.consolidate {
                info!("--dry-run requested, consolidation will not change files.");
            } else {
                warn!("--dry-run being ignored, since consolidation was not requested.");
            }
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

impl ToSql for Directory {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, rusqlite::Error> {
        to_sql(&self.0)
    }
}

impl FromSql for Directory {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        from_sql_column_result(value).map(Directory)
    }
}

impl<T: ?Sized + AsRef<OsStr>> From<&T> for Directory {
    fn from(path: &T) -> Self {
        Directory(PathBuf::from(path))
    }
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone)]
/// A filename that contains no directory component.
pub(crate) struct Basename(pub PathBuf);

impl ToSql for Basename {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, rusqlite::Error> {
        to_sql(&self.0)
    }
}

impl FromSql for Basename {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        from_sql_column_result(value).map(Basename)
    }
}

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
    pub duplicates: Vec<Vec<PathBuf>>,

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
        let mut hardlinked_subgroups = HashMap::<FileId, Vec<PathBuf>>::new();
        for file in match_group.iter() {
            let hardlinked_subgroup = hardlinked_subgroups
                .entry((file.inode, file.deviceno))
                .or_default();
            hardlinked_subgroup.push(file.path()?);
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

fn to_sql(path: &'_ Path) -> Result<ToSqlOutput<'_>, rusqlite::Error> {
    if let Some(s) = path.to_str() {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes())))
    } else {
        let bytes = str_to_blob(path.as_os_str());
        debug!(
            "Cannot represent path as string; will use bytes: {:?}",
            path
        );
        Ok(ToSqlOutput::Owned(Value::Blob(bytes)))
    }
}

fn from_sql_column_result(
    value: rusqlite::types::ValueRef<'_>,
) -> rusqlite::types::FromSqlResult<PathBuf> {
    match value {
        rusqlite::types::ValueRef::Text(text) => {
            // safe to unwrap; Infallible
            Ok(PathBuf::from(&String::from_utf8(text.to_vec()).expect(
                "Malformed data in DB: Text should be valid UTF-8",
            )))
        }
        rusqlite::types::ValueRef::Blob(blob) => {
            let s = blob_to_str(blob).map_err(|err| FromSqlError::Other(Box::from(err)))?;
            Ok(PathBuf::from(s))
        }
        _ => Err(FromSqlError::InvalidType),
    }
}

/// Interpret a byte string as a possibly invalid UTF-8 path,
/// or as a possibly invalid UTF-16 string on Windows
fn blob_to_str(blob: &[u8]) -> Result<OsString> {
    #[cfg(unix)]
    {
        Ok(OsString::from_vec(blob.to_vec()))
    }
    #[cfg(windows)]
    {
        if blob.len() % 2 == 1 {
            bail!(
                "Blob can't be parsed to filename: uneven byte length: {:?}",
                blob
            );
        }
        let wide: Vec<u16> = blob.chunks(2).map(combine_bytes).collect();
        Ok(OsString::from_wide(&wide))
    }
}

/// Convert a filename str to bytes.
fn str_to_blob(s: &OsStr) -> Vec<u8> {
    #[cfg(unix)]
    {
        let bytes = s.as_bytes();
        bytes.to_vec()
    }
    #[cfg(windows)]
    {
        let mut bytes = Vec::new();
        for item_u16 in s.encode_wide() {
            let (a, b) = split_bytes(item_u16);
            bytes.push(a);
            bytes.push(b);
        }
        bytes
    }
}

#[cfg_attr(unix, allow(dead_code))] // only needed for windows
fn combine_bytes(bytes: &[u8]) -> u16 {
    (bytes[0] as u16) << 8 | bytes[1] as u16
}

#[cfg_attr(unix, allow(dead_code))] // only needed for windows
fn split_bytes(n: u16) -> (u8, u8) {
    let a = (n >> 8) as u8;
    let b = n as u8;
    (a, b)
}

#[test]
fn test_u8_u16_conversion() {
    assert_eq!(combine_bytes(&[0x12, 0xAB]), 0x12AB_u16);
    assert_eq!(split_bytes(0x12AB_u16), (0x12, 0xAB));
}

#[test]
fn test_fname_conversion() -> Result<()> {
    // These are all valid UTF-8 of course:
    for s in ["hello", "", "✔️ ❤️ ☆"] {
        let s = OsString::from_str(s).unwrap(); // Err is Infallible
        assert_eq!(&s, &blob_to_str(&str_to_blob(&s))?);
    }

    // This is an invalid utf-8 string:
    let b = vec![0x66, 0x6f, 0x80, 0x6f];
    assert_eq!(b, str_to_blob(&blob_to_str(&b)?));

    #[cfg(windows)]
    {
        // invalid byte length
        let source = b"x";
        let s = blob_to_str(source);
        assert!(s.is_err());

        // invalid UTF-16 from rust OsString documentation
        let source = [0x0066, 0x006f, 0xD800, 0x006f];
        let s = OsString::from_wide(&source[..]);
        assert_eq!(&s, &blob_to_str(&str_to_blob(&s))?);
    }

    Ok(())
}
