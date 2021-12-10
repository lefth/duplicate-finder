use std::fs::Metadata;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::{fs::Metadata, os::windows::fs::MetadataExt};

pub(crate) fn get_deviceno(md: &Metadata) -> u64 {
    #[cfg(windows)]
    return md
        .volume_serial_number()
        .expect("Metadata must not be created with `DirEntry::metadata`") as u64;
    #[cfg(unix)]
    return md.dev();
}
