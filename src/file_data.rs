pub mod file_data {
    use std::path::PathBuf;

    use crate::types::*;
    #[allow(unused_imports)]
    use log::{debug, error, info, trace, warn};

    #[derive(Debug)]
    pub(crate) struct FileData {
        pub row_id: RowId,
        pub dir_id: Option<DirectoryId>,
        pub dir: Option<Directory>,
        pub basename: Option<Basename>,
        pub deviceno: Deviceno,
        pub inode: Inode,
        pub size: Size,
        pub shortchecksum: Option<Checksum>,
        pub checksum: Option<Checksum>,
    }

    impl FileData {
        pub fn new(
            row_id: RowId,
            dir: Option<Directory>,
            basename: Option<Basename>,
            deviceno: Deviceno,
            inode: Inode,
            size: Size,
            shortchecksum: Option<Checksum>,
            checksum: Option<Checksum>,
        ) -> FileData {
            FileData {
                row_id,
                dir_id: None,
                dir,
                basename,
                deviceno,
                inode,
                size,
                shortchecksum,
                checksum,
            }
        }

        /// Panics if .unwrap() would panic
        pub fn path_unwrap(&self) -> PathBuf {
            self.dir
                .as_ref()
                .unwrap()
                .join(self.basename.as_ref().unwrap())
        }

        /// Panics if .unwrap() would panic or if the path isn't a valid UTF-8 string
        pub fn path_str_unwrap(&self) -> String {
            self.path_unwrap()
                .to_str()
                .expect("Paths should be representable as String or skipped")
                .to_string()
        }
    }
}
