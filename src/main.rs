#![cfg_attr(windows, feature(windows_by_handle))]

use std::fs;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use crossbeam_utils::thread;
use duplicates::prompt;
use structopt::lazy_static::lazy_static;
use structopt::StructOpt;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn, LevelFilter};

use duplicates::{
    consolidation::*,
    file_db,
    options::Options,
    process_matches::ProcessMatches,
    process_matches::{GetFiles, GroupByFullChecksum, GroupByShortChecksum, PrintMatches},
};

fn init(options: &mut Options) -> Result<()> {
    // The logger must be initialized first, to warn about issues with later init:
    let mut log_builder = env_logger::Builder::new();
    log_builder.filter_level(LevelFilter::Trace);
    if !cfg!(debug_assertions) && !options.log {
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

    options.validate()?;

    // Set the interrupt handler to call the top element in a stack of closures.
    // So new status checks can be created, and the old checks will take over
    // when the new element is popped.
    let interrupt_handlers = Arc::clone(&options.interrupt_handlers);
    ctrlc::set_handler(move || {
        // Get the time since last interrupt:
        lazy_static! {
            static ref LAST_INTERRUPT: Mutex<Option<Instant>> = Mutex::new(None);
        }
        let mut last_interrupt = LAST_INTERRUPT.lock().unwrap();
        let prev_interrupt_time = last_interrupt.replace(Instant::now());

        // Exit if needed
        match prev_interrupt_time {
            Some(prev_interrupt_time) if prev_interrupt_time.elapsed().as_millis() < 1500 => {
                std::process::exit(1);
            }
            _ => {}
        }

        // Call the temporary additional interrupt handler logic:
        let lock = interrupt_handlers.lock().unwrap();
        let last_handler = lock.last();
        if let Some(handler) = last_handler {
            handler();
        }

        eprintln!("\nInterrupt caught. Quickly press ctrl-c again to exit.");
    })
    .context("Error setting Ctrl-C handler")
}

fn main() -> Result<()> {
    let mut options = Options::from_args();
    init(&mut options)?;

    let mut conn = file_db::init_connection(&mut options, false)?;

    if options.migrate_db {
        file_db::migrate_db(&mut conn)?;
        if options.operation_count() == 1 {
            return Ok(());
        }
    }

    if options.no_truncate_db
        // the later stages don't need to read files from disk
        && !options.resume_stage3
        && !options.resume_stage4
        && (!options.migrate_db || options.operation_count() > 1)
    {
        let proceed = prompt(
            &options,
            "Reusing old metadata. Is it okay to guess if a mountpoint device number has changed? \
                \n(Say yes unless you are scanning multiple drives with the same filenames using the same database.)",
            true,
        );
        if proceed {
            file_db::remap_changed_device_numbers(&mut conn)?;
        }
    }

    let options = Arc::new(options);
    let final_matches = if options.resume_stage4 {
        file_db::get_with_checksum(&conn, "checksum", &options)?
    } else {
        let candidate_groups = if options.resume_stage3 {
            file_db::get_with_checksum(&conn, "shortchecksum", &options)?
        } else {
            let _handler_guard =
                options.push_interrupt_handler(|| eprintln!("\nFinding all files"));
            let candidate_groups = GetFiles::process_matches(None, &mut conn, &options)?;

            let candidate_groups =
                GroupByShortChecksum::process_matches(Some(candidate_groups), &mut conn, &options)?;
            candidate_groups
        };

        let final_matches =
            GroupByFullChecksum::process_matches(Some(candidate_groups), &mut conn, &options)?;
        final_matches
    };

    // Get user confirmation before we show a bunch of output, since the output will hide the prompt:
    let user_confirmed_consolidation = options.consolidate
        && prompt(
            &options,
            "\nAutomatically hard linking files is not recommended unless you have made a backup.\n\
            Proceed anyway?",
            false,
        );

    // Because all the filenames might not fit in memory, we have to process them with a channel
    // as they are generated:
    thread::scope(|s| -> Result<()> {
        let (tx, rx) = mpsc::sync_channel(100);
        let handle = s.spawn(|_| -> Result<()> {
            PrintMatches::process_matches(final_matches, &mut conn, &options, tx)?;

            conn.close().map_err(|err| err.1)?;
            if !options.keep_db && options.db_file != ":memory:" {
                fs::remove_file(&options.db_file)?;
            }
            Ok(())
        });

        if options.consolidate && user_confirmed_consolidation {
            consolidate_groups(rx, &options)?;
        } else {
            // drain the receiver so the sender doesn't block
            for _ in rx {}
        }
        handle.join().unwrap()?;
        Ok(())
    })
    .unwrap()?;

    Ok(())
}
