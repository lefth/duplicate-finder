# duplicates

[![Build Status](https://app.travis-ci.com/lefth/duplicate-finder.svg?branch=master)](https://app.travis-ci.com/lefth/duplicate-finder)

A duplicate finder that works well on large and slow hard disks.

This program is optimized for slow I/O and to avoid running out of memory, in contrast to
some other modern rust software that is optimized for SSDs.

Warning: this is alpha-level software. It has not had a lot of real world testing.

#### USAGE:
```
    duplicates <operation...> [FLAGS] [OPTIONS] [starting-paths]...
```

#### OPERATIONS:
    At least one operation must be chosen:
```
        --consolidate                       Attempt to hard link duplicate files to reclaim space. This is
                                            a testing feature and you should back up your files before
                                            using it
    -j, --write-json <save-json-filename>   Save output to a file as JSON
    -p, --print-duplicates                  Print files that are duplicated on disk, but not already hard
                                            linked
        --migrate-db                        Migrate a saved DB from version 0.0.1 or 0.0.2 to the current format.
                                            Implies --no-truncate-db, as well as --keep-db if used without
                                            another operation
```

#### FLAGS:
```
        --allow-incomplete-consolidation    Continue consolidation even if there are other linked files
                                            that were not detected. This means space will not be saved
                                            in some cases, but that's a necessity if running with a backup
                                            copy (the backup copy normally being created with hard links)
    -n, --dry-run                           Don't consolidate files, but print what would be done
    -h, --help                              Prints help information
    -k, --keep-db                           Don't delete the sqlite file after operations are complete
        --mmap                              Use mmap. This increases performance of reading large files
                                            on SSDs, but decreases performance on spinning drives. There
                                            is also a possibility of crashes when files are modified
                                            during reading
        --no-truncate-db                    Keep the database file from the previous run
    -q, --quiet                             Reduces level of verbosity
        --no-remember-checksums             Don't redo checksums that are already stored in a database.
                                            Useful for resuming an operation without knowing at what stage
                                            it stopped, or adding additional paths to an operation that
                                            was completed. This option only makes sense with
                                            --no-truncate-db
        --resume-stage3                     Resume a previous operation at stage 3: this computes any
                                            necessary full checksums, based on candidates (with matching
                                            short checksums) in an existing database file. Implies
                                            --no-truncate-db
        --resume-stage4                     Resume a previous operation at stage 4: this prints results
                                            based on checksums that have already been computed, and
                                            consolidates the duplicates if requested. Implies
                                            --no-truncate-db
    -f, --show-fully-hardlinked             Show duplicates that are already fully hardlinked (so no
                                            further space savings are possible)
    -V, --version                           Prints version information
    -v, --verbose                           Increases level of verbosity

```

#### OPTIONS:
```
        --buffer-megabytes <buffer-megabytes>
            Tell the program how much memory it can use as buffers for reading files. This is not
            necessary if using --mmap since large files won't be read to buffers anyway. Small buffers are
            allowed but will slow down operation: --buffer-megabytes 0.2
        --db-file <db-file>
            Choose a file, new or existing, for the database. :memory: is a special value [default:
            metadata.sqlite]

    -t, --max-io-threads <max-io-threads>
            How many threads should read small files from disk at a time? Large files use one thread at a
            time [default: 8]
    -M, --max-size <max-size>
            Skip files greater than this size, as they are probably not real files. 0 bytes means skip
            nothing [default: 100000000000]
    -m, --min-size <min-size>                    Minimum size (bytes) of files to check [default: 4096]
```

#### ARGS:
```
    <starting-paths>...    Paths to search for duplicates [default: .]
```

## Examples
To run with default options:
```
duplicates --print-duplicates ~
```

To see all files, more output, and keep a database of info on disk afterwards:
```
duplicates ~ --min-size 1 -v --show-fully-hardlinked --write-json duplicates.json -k
```

To consolidate duplicate files after the database has already been created (and kept
with `-k`):
```
duplicates --consolidate --allow-incomplete-consolidation --resume-stage4 --no-truncate-db -k
```

## Installation

### On Linux, Mac, or WSL:
```
cargo install --git https://github.com/lefth/duplicate-finder
```
### On Windows:
Rust nightly is required:
```
cargo +nightly install --git https://github.com/lefth/duplicate-finder
```

### Cross compilation
I've found this program useful on ARM, so it can remove duplicates on NAS or router-mounted USB drives.
The compilation procedure is slightly different for each target platform, and there is more than one way
to accomplish the same goal. Here is my approach, for a musl-based system with hardware floating point
support ("hf" string in the platform).

Since the target's C library is musl, I needed to use the compiler toolchain from [musl-cross-make](https://github.com/richfelker/musl-cross-make).
The exact target system was "arm-linux-musleabihf". I had to build version 1.1.24 since the most recent
version won't link nicely with the version of musl that's included in the Rust toolchain. See: [problem](https://stackoverflow.com/questions/61934997/undefined-reference-to-stat-time64-when-cross-compiling-rust-project-on-mu), [bug report](https://github.com/rust-lang/rust/issues/72274).
The musl-cross-make target and the cargo build target may not be exactly the same.

If your target system is based on glibc, you will not need to use musl-cross-make, and your target strings
will include "gnueabi" instead of "musleabi".

I had to set `-mcpu` based on the target system's CPU info in `/proc/cpuinfo`, since the target string
isn't specific enough for the compiler to know the chip's capabilities. This environment variable is not
globally applicable (for all compilation), so it shouldn't be specified in the shell's global environment.
If there is a way to simplify this (to make dependencies compile without needing to set CC),
please file an issue or make a pull request.

Also setting the linker, my full build command is:
```
CARGO_TARGET_ARMV7_UNKNOWN_LINUX_MUSLEABIHF_LINKER=arm-linux-musleabihf-gcc CC='arm-linux-musleabihf-gcc -mcpu=generic-armv7-a+vfpv3+neon' cargo build --target armv7-unknown-linux-musleabihf --release
```

If the build fails, it may be necessary to remove the `target` directory and try again.

<!-- vim: textwidth=106 expandtab: -->
