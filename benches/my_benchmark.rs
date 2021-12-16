use criterion::{criterion_group, criterion_main, Criterion};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use rusqlite::params;
use structopt::StructOpt;

use duplicates::{options::Options, types::FileIdent, *};

pub fn criterion_benchmark(c: &mut Criterion) {
    env_logger::init();

    let mut options =
        Options::from_iter(["duplicates", "--no-truncate-db", "-k", "--consolidate"].iter());
    options.validate().unwrap();
    let conn = file_db::init_connection(&mut options, true).unwrap();

    let file_idents = get_file_idents(&conn, 500).unwrap();

    c.bench_function("get multiple file by ID", |b| {
        b.iter(|| {
            file_db::get_files(&conn, &file_idents, true, false, &options.exclude, |file| {
                debug!("Got file: {:?}", file);
            })
            .unwrap();
        });
    });

    c.bench_function("get single file by ID", |b| {
        b.iter(|| {
            file_db::get_files(&conn, &file_idents, true, true, &options.exclude, |file| {
                debug!("Got file: {:?}", file);
            })
            .unwrap();
        });
    });
}

fn get_file_idents(
    conn: &rusqlite::Connection,
    limit: u64,
) -> rusqlite::Result<Vec<types::FileIdent>> {
    let mut stmt = conn.prepare("SELECT inode, deviceno FROM files LIMIT ?")?;
    let mut rows = stmt.query(params![limit])?;
    let mut ret = Vec::new();
    while let Some(row) = rows.next()? {
        ret.push(FileIdent {
            inode: row.get(0)?,
            deviceno: row.get(1)?,
        });
    }
    assert!(
        ret.len() > 10,
        "Result rows are too small: {}. Generate more data first to get a valid benchmark.",
        ret.len()
    );
    Ok(ret)
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
