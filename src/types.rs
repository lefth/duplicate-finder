use std::{
    array::TryFromSliceError,
    convert::{TryFrom, TryInto},
    ffi::{OsStr, OsString},
    fmt::Display,
    num::ParseIntError,
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
};

#[cfg(unix)]
use std::os::unix::prelude::{OsStrExt, OsStringExt};
#[cfg(windows)]
use std::os::windows::ffi::{OsStrExt, OsStringExt};

use anyhow::{Context, Result};
use rusqlite::{
    types::{FromSql, FromSqlError, ToSqlOutput, Value, ValueRef},
    *,
};
use serde::{Deserialize, Serialize};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

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

impl FromSql for Checksum {
    fn column_result(value: ValueRef<'_>) -> Result<Self, FromSqlError> {
        match value {
            rusqlite::types::ValueRef::Blob(bytes) => {
                let checksum = bytes
                    .try_into()
                    .context("Blob has wrong byte length for checksum")
                    .map_err(|err| FromSqlError::Other(Box::from(err)));
                checksum
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

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

fn to_sql(path: &'_ Path) -> Result<ToSqlOutput<'_>, rusqlite::Error> {
    if let Some(s) = path.to_str() {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes())))
    } else {
        let bytes = str_to_bytes(path.as_os_str());
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
            let s = bytes_to_str(blob).map_err(|err| FromSqlError::Other(Box::from(err)))?;
            Ok(PathBuf::from(s))
        }
        _ => Err(FromSqlError::InvalidType),
    }
}

/// Interpret a byte string as a possibly invalid UTF-8 path,
/// or as a possibly invalid UTF-16 string on Windows
pub(crate) fn bytes_to_str(blob: &[u8]) -> Result<OsString> {
    #[cfg(unix)]
    {
        Ok(OsString::from_vec(blob.to_vec()))
    }
    #[cfg(windows)]
    {
        if blob.len() % 2 == 1 {
            anyhow::bail!(
                "Blob can't be parsed to filename: uneven byte length: {:?}",
                blob
            );
        }
        let wide: Vec<u16> = blob.chunks(2).map(combine_bytes).collect();
        Ok(OsString::from_wide(&wide))
    }
}

/// Convert a filename str to bytes.
fn str_to_bytes(s: &OsStr) -> Vec<u8> {
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
        assert_eq!(&s, &bytes_to_str(&str_to_bytes(&s))?);
    }

    // This is an invalid utf-8 string:
    let b = vec![0x66, 0x6f, 0x80, 0x6f];
    assert_eq!(b, str_to_bytes(&bytes_to_str(&b)?));

    #[cfg(windows)]
    {
        // invalid byte length
        let source = b"x";
        let s = bytes_to_str(source);
        assert!(s.is_err());

        // invalid UTF-16 from rust OsString documentation
        let source = [0x0066, 0x006f, 0xD800, 0x006f];
        let s = OsString::from_wide(&source[..]);
        assert_eq!(&s, &bytes_to_str(&str_to_bytes(&s))?);
    }

    Ok(())
}
