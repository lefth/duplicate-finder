pub mod file_data {
    use std::path::PathBuf;

    use anyhow::{anyhow, Result};

    use crate::types::*;
    #[allow(unused_imports)]
    use log::{debug, error, info, trace, warn};

    #[derive(Debug)]
    pub(crate) struct FileData {
        pub row_id: RowId,
        pub dir: Option<Directory>,
        pub basename: Option<Basename>,
        pub deviceno: Deviceno,
        pub inode: Inode,
        pub size: Size,
    }

    impl FileData {
        pub fn new(
            row_id: RowId,
            dir: Option<Directory>,
            basename: Option<Basename>,
            deviceno: Deviceno,
            inode: Inode,
            size: Size,
        ) -> FileData {
            FileData {
                row_id,
                dir,
                basename,
                deviceno,
                inode,
                size,
            }
        }

        /// Get the path: dir/basename.
        /// Error if .unwrap() would panic.
        pub fn path(&self) -> Result<PathBuf> {
            let dir = self.dir.as_ref().ok_or(anyhow!("Dir value not found"))?;
            let basename = self
                .basename
                .as_ref()
                .ok_or(anyhow!("Basename value not found"))?;

            Ok(dir.join(basename))
        }
    }
}
