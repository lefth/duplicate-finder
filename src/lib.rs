#![cfg_attr(windows, feature(windows_by_handle))]

use std::fs::Metadata;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

pub mod consolidation;
pub mod duplicate_group;
pub mod file_data;
pub mod file_db;
pub mod options;
pub mod process_matches;
pub mod types;

pub fn get_deviceno(md: &Metadata) -> u64 {
    #[cfg(windows)]
    return md
        .volume_serial_number()
        .expect("Metadata must not be created with `DirEntry::metadata`") as u64;
    #[cfg(unix)]
    return md.dev();
}
