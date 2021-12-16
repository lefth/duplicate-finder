#![cfg_attr(windows, feature(windows_by_handle))]

use std::{fs::Metadata, io::BufRead};

use options::Options;

pub mod consolidation;
pub mod duplicate_group;
pub mod file_data;
pub mod file_db;
pub mod options;
pub mod process_matches;
pub mod types;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

pub fn get_deviceno(md: &Metadata) -> u64 {
    #[cfg(windows)]
    return md
        .volume_serial_number()
        .expect("Metadata must not be created with `DirEntry::metadata`") as u64;
    #[cfg(unix)]
    return md.dev();
}

/// Prompt the user with a Y/n or y/N option (depending on whether the default is true).
pub fn prompt(options: &Options, msg: &str, default: bool) -> bool {
    eprintln!("{} [{}]", msg, if default { "Y/n" } else { "y/N" });
    if options.no_prompt {
        return true;
    }

    // An interrupt should kill the process if it occurs during a prompt and wait for input:
    let _temp_handler = options.push_interrupt_handler(|| std::process::exit(1));

    let input_line = std::io::stdin().lock().lines().next().unwrap().unwrap();
    if default {
        !input_line.to_lowercase().starts_with("n")
    } else {
        input_line.to_lowercase().starts_with("y")
    }
}
