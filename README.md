# duplicates

[![Build Status](https://travis-ci.com/lefth/duplicates.svg?branch=master)](https://travis-ci.com/lefth/duplicates)

A duplicate finder that works well on large and slow hard disks.

This program is optimized for slow I/O and to avoid running out of memory.

Warning: this is alpha-level software. It has not had a lot of real world testing.

#### USAGE:
```
    duplicates [FLAGS] [OPTIONS] [starting-paths]...
```

#### FLAGS:
```
    -h, --help                     Prints help information
    -k, --keep-db-file             Don't delete the sqlite file. For debugging
        --mmap                     Use mmap. This increases performance of reading large files on SSDs, but
                                   decreases performance on spinning drives. There is also a possibility of
                                   crashes when files are modified during reading.
        --no-truncate-db           Keep the database file from the previous run
    -j, --print-json               Print output as JSON
    -q, --quiet                    Reduces level of verbosity
    -f, --show-fully-hardlinked    Show duplicates that are already fully hardlinked (so no further space
                                   savings are possible)
    -V, --version                  Prints version information
    -v, --verbose                  Increases level of verbosity
```

#### OPTIONS:
```
        --db-file <db-file>                  Choose a file, new or existing, for the database. :memory:
                                             is a special value [default: metadata.sqlite]
    -t, --max-io-threads <max-io-threads>    How many threads should read small files from disk at a
                                             time? Large files use one thread at a time. [default: 8]
    -M, --max-size <max-size>                Skip files greater than this size, as they are probably not
                                             real files. 0 bytes means skip nothing
                                             [default: 100000000000]
    -m, --min-size <min-size>                Minimum size (bytes) of files to check [default: 4096]
```

#### ARGS:
```
    <starting-paths>...    Paths to search for duplicates [default: .]
```

## Examples
To run with default options:
```
duplicates ~
```

To see all files, more output, and keep a database of info on disk afterwards:
```
duplicates ~ --min-size 1 --db-file metadata.sqlite --keep-db-file -v --show-fully-hardlinked --print-json
```

## Installation

### On Linux, Mac, or WSL:
```
cargo install --git https://github.com/lefth/duplicates
```
### On Windows:
Rust nightly is required:
```
cargo +nightly install --git https://github.com/lefth/duplicates
```

