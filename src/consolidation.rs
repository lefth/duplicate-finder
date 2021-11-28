use std::{
    fs::{self, Metadata},
    io::{self, BufRead},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        mpsc, Arc,
    },
    time::Instant,
};

#[cfg(unix)]
use std::os::unix::prelude::MetadataExt;
#[cfg(windows)]
use std::os::windows::prelude::MetadataExt;

use anyhow::{bail, Context, Result};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn, LevelFilter};
use uuid::Uuid;

use crate::types::{DuplicateGroup, Options};

pub(crate) fn consolidate_groups(
    rx_duplicates: mpsc::Receiver<DuplicateGroup>,
    options: &Options,
) -> Result<()> {
    if !options.dry_run {
        eprintln!(
            "\nAutomatically hard linking files is a feature in testing and is not recommended \
            unless you have made a backup. Proceed anyway? [y/N]"
        );

        // ctrl-c should quit at this prompt, not show a status message:
        let _handler_guard = options.push_interrupt_handler(|| std::process::exit(1));

        let input_line = io::stdin().lock().lines().next().unwrap().unwrap();
        if !input_line.to_lowercase().starts_with("y") {
            bail!("Operation cancelled");
        }
    }
    info!("Will overwrite non-linked files with hard links.");

    let start_time = Instant::now();
    let completed_count = Arc::new(AtomicU64::new(0));
    let _handler_guard = {
        let completed_count = Arc::clone(&completed_count);
        options.push_interrupt_handler(move || {
            eprintln!(
                "\nConsolidated {} duplicate groups. {:?} elapsed.",
                completed_count.load(Relaxed),
                start_time.elapsed()
            )
        })
    };

    for mut duplicate_group in rx_duplicates {
        consolidate_group(&mut duplicate_group, options)?;
        completed_count.fetch_add(1, Relaxed);
    }
    Ok(())
}

fn consolidate_group(duplicate_group: &mut DuplicateGroup, options: &Options) -> Result<()> {
    choose_group_to_preserve(duplicate_group);
    if duplicate_group.duplicates.len() <= 1 {
        // There must have been errors processing these files, and now
        // there's nothing left to do with this group.
        return Ok(());
    }

    let link_source = &duplicate_group.duplicates[0][0];
    for group in duplicate_group.duplicates[1..].iter() {
        if let Err(err) = consolidate_sub_group(group, link_source, options) {
            warn!("Problem consolidating sub-group: {}", err);
        }
    }
    Ok(())
}

fn consolidate_sub_group(
    group: &[PathBuf],
    link_source: &PathBuf,
    options: &Options,
) -> Result<()> {
    for file_to_overwrite in group {
        let dir = file_to_overwrite
            .parent()
            .context("Cannot have a file without a parent")?;
        let mut tmp_dest_path = dir.to_owned();

        debug!("Linking {:?} to {:?}", link_source, file_to_overwrite);
        if !options.dry_run {
            tmp_dest_path.push(format!("link-temp-{}", Uuid::new_v4().to_simple()));
            fs::hard_link(link_source, &tmp_dest_path)?;
            // Note: rename is supposed to remove the source afterwards,
            // but on some platforms it won't do that
            // (depending on the underlying system function):
            fs::rename(&tmp_dest_path, file_to_overwrite)?;
            if Path::exists(&tmp_dest_path) {
                fs::remove_file(tmp_dest_path)?;
            }
        }
    }
    Ok(())
}

/// Check the link count to make sure there aren't links we didn't find.
/// That would mean this consolidation actually wouldn't save any space.
/// If that's the case, this group can be preserved or skipped.
fn has_not_found_links(duplicate_group: &[PathBuf]) -> Result<bool> {
    assert!(duplicate_group.len() > 0);

    let first_path = &duplicate_group[0];
    let md = fs::metadata(first_path)?;
    let nlinks = number_of_links(&md).context("Couldn't get hard links from metadata")?;
    if nlinks < duplicate_group.len() as u64 {
        bail!(
            "Group has {} members but there are only {} linked files on this inode.",
            duplicate_group.len(),
            nlinks
        );
    }

    Ok(nlinks != duplicate_group.len() as u64)
}

pub(crate) fn choose_group_to_preserve(duplicate_group: &mut DuplicateGroup) {
    let mut largest_idx = None;
    let mut largest_group = None;
    let mut idx_with_other_links = None;

    // Reverse iteration so we can delete sub-groups that can't be consolidated
    for i in (0..duplicate_group.duplicates.len()).rev() {
        let elem = &duplicate_group.duplicates[i];

        let has_not_found_links = match has_not_found_links(elem) {
            Ok(result) => result,
            Err(err) => {
                warn!(
                    "Error reading links for group to consolidate: {}. Skipping files:",
                    err
                );
                warn_path_list(elem);
                duplicate_group.duplicates.remove(i);
                if let Some(largest_idx) = largest_idx.as_mut() {
                    *largest_idx = *largest_idx - 1;
                }
                if let Some(idx_with_other_links) = idx_with_other_links.as_mut() {
                    *idx_with_other_links = *idx_with_other_links - 1;
                }
                continue;
            }
        };
        if has_not_found_links {
            if let Some(idx_with_other_links) = idx_with_other_links.as_mut() {
                // We can't reclaim all the space. This should be handled manually.
                // (One group with extra links would be okay--we could preserve those
                // files but re-link the other groups.)
                warn!("Duplicate groups have more than one not-found hard link, so all space can't be reclaimed. Skipping:");
                warn_path_list(elem);

                duplicate_group.duplicates.remove(i);
                // we removed an element, so we must decrement the indices we are keeping:
                if let Some(largest_idx) = largest_idx.as_mut() {
                    *largest_idx = *largest_idx - 1;
                }
                *idx_with_other_links = *idx_with_other_links - 1;
                continue;
            } else {
                idx_with_other_links = Some(i);
            }
        } else if largest_group.is_none() || elem.len() > largest_group.unwrap() {
            largest_group = Some(elem.len());
            largest_idx = Some(i);
        }
    }

    if let Some(idx_with_other_links) = idx_with_other_links {
        duplicate_group.duplicates.swap(0, idx_with_other_links);
    } else if let Some(largest_idx) = largest_idx {
        duplicate_group.duplicates.swap(0, largest_idx);
    }
}

fn warn_path_list(paths: &[PathBuf]) {
    for path in paths {
        if let Some(path) = path.to_str() {
            warn!("- {}", path);
        } else {
            warn!("- {:?}", path);
        }
    }
}

fn number_of_links(md: &Metadata) -> Option<u64> {
    #[cfg(unix)]
    return Some(md.nlink());
    #[cfg(windows)]
    return md.number_of_links().map(|n| n as u64);
}
