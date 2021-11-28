#![cfg_attr(windows, feature(windows_by_handle))]

use std::fs;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use structopt::lazy_static::lazy_static;
use structopt::StructOpt;

mod file_db;
use crate::{
    file_db::file_db::init_connection,
    process_matches::{GetFiles, GroupByFullChecksum, GroupByShortChecksum, PrintMatches},
};
mod process_matches;
use crate::process_matches::ProcessMatches;
mod file_data;
mod types;
use types::*;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn, LevelFilter};

type JobId = (Inode, Deviceno);

fn init(options: &Options) -> Result<()> {
    let mut log_builder = env_logger::Builder::new();
    log_builder.filter_level(LevelFilter::Trace);
    if !cfg!(debug_assertions) {
        // Output looks better in releases if it's not written like a log file:
        log_builder.format_module_path(false);
        log_builder.format_level(false);
        log_builder.format_timestamp(None);
    }
    log_builder.init();

    // Set verbosity, with a different default for debug/release builds
    let log_levels = [
        LevelFilter::Off,
        LevelFilter::Error,
        LevelFilter::Warn,
        LevelFilter::Info,
        LevelFilter::Debug,
        LevelFilter::Trace,
    ];
    let mut verbosity: i32 = if cfg!(debug_assertions) { 4 } else { 3 };
    verbosity += options.verbose - options.quiet;
    log::set_max_level(log_levels[verbosity.clamp(0, log_levels.len() as i32 - 1) as usize]);

    // Set the interrupt handler to call the top element in a stack of closures.
    // So new status checks can be created, and the old checks will take over
    // when the new element is popped.
    let interrupt_handlers = Arc::clone(&options.interrupt_handlers);
    ctrlc::set_handler(move || {
        lazy_static! {
            static ref LAST_INTERRUPT: Mutex<Option<Instant>> = Mutex::new(None);
        }
        let mut last_interrupt = LAST_INTERRUPT.lock().unwrap();
        let prev_interrupt_time = last_interrupt.replace(Instant::now());
        match prev_interrupt_time.map(|instant| instant.elapsed()) {
            Some(elapsed) if elapsed.as_millis() < 1500 => {
                std::process::exit(1);
            }
            _ => eprintln!("\nInterrupt caught. Quickly press ctrl-c again to exit."),
        }

        let lock = interrupt_handlers.lock().unwrap();
        let last_handler = lock.last();
        if let Some(handler) = last_handler {
            handler();
        }
    })
    .context("Error setting Ctrl-C handler")
}

fn main() -> Result<()> {
    let mut options = Options::from_args();
    options.init();

    init(&options)?;

    let mut conn = init_connection(&mut options)?;

    let _handler_guard = options.push_interrupt_handler(|| eprintln!("\nFinding all files"));

    let options = Arc::new(options);
    let final_matches = if options.resume_stage4 {
        file_db::file_db::get_matching_checksums(&conn, "checksum", &options)?
    } else {
        let candidate_groups = if options.resume_stage3 {
            file_db::file_db::get_matching_checksums(&conn, "shortchecksum", &options)?
        } else {
            let candidate_groups = GetFiles::process_matches(None, &mut conn, &options)?;

            let candidate_groups =
                GroupByShortChecksum::process_matches(Some(candidate_groups), &mut conn, &options)?;
            candidate_groups
        };

        let final_matches =
            GroupByFullChecksum::process_matches(Some(candidate_groups), &mut conn, &options)?;
        final_matches
    };

    PrintMatches::process_matches(Some(final_matches), &mut conn, &options)?;

    conn.close().map_err(|err| err.1)?;
    if !options.keep_db_file && options.db_file != ":memory:" {
        fs::remove_file(&options.db_file)?;
    }
    Ok(())
}
