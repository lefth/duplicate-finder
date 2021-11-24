# duplicates

[![Build Status](https://travis-ci.com/lefth/duplicates.svg?branch=master)](https://travis-ci.com/lefth/duplicates)

A duplicate finder that works well on large and slow hard disks.

This program is optimized for slow I/O and to avoid running out of memory, in contrast to
some other modern rust software that is optimized for SSDs.

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
globally applicable (for all compilation), so it shouldn't be specified in the shell's global envirronment.

Also setting the linker, my full build command is:
```
CARGO_TARGET_ARMV7_UNKNOWN_LINUX_MUSLEABIHF_LINKER=arm-linux-musleabihf-gcc CC='arm-linux-musleabihf-gcc -mcpu=generic-armv7-a+vfpv3+neon' cargo build --target armv7-unknown-linux-musleabihf --release
```

If the build fails, it may be necessary to remove the `target` directory and try again.
