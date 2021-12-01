use std::{collections::HashMap, path::PathBuf};

use anyhow::Result;
use serde::{
    ser::{SerializeSeq, SerializeStruct},
    Serialize,
};

use crate::{
    file_data::file_data::FileData,
    types::{Deviceno, Inode, Options},
};

/// All filenames stored within are duplicates, but they are grouped by device/inode.
/// That way hard linked files are grouped together.
///
/// duplicates[0] is a list of files that are already hard linked together.
/// Likewise, duplicates[1] (if it exists) are hard linked together.
/// But duplicates[0] and duplicates[1] are not hard links with each other.
pub(crate) struct DuplicateGroup {
    pub duplicates: Vec<Vec<PathBuf>>,

    pub redundant_bytes: u64,
}

impl DuplicateGroup {
    pub fn new(match_group: Vec<FileData>, options: &Options) -> Result<Option<DuplicateGroup>> {
        type FileId = (Inode, Deviceno);

        assert!(match_group.len() > 1);

        // this may not be actionable, if all files are already linked:
        if !options.show_fully_hardlinked
            && !match_group.iter().skip(1).any(|file| {
                file.inode != match_group[0].inode || file.deviceno != match_group[0].deviceno
            })
        {
            return Ok(None); // no further consolidation is possible
        }

        // Group filenames by hardlinked files, so the user can see what needs to be consolidated
        let mut hardlinked_subgroups = HashMap::<FileId, Vec<PathBuf>>::new();
        for file in match_group.iter() {
            let hardlinked_subgroup = hardlinked_subgroups
                .entry((file.inode, file.deviceno))
                .or_default();
            hardlinked_subgroup.push(file.path()?);
        }

        let file_size = match_group.first().unwrap().size.0;
        // If the necessary space usage is size, then the redundant space is size * (the number of
        // hard linked groups - 1).
        let redundant_bytes = (hardlinked_subgroups.len() as u64 - 1) * file_size;

        let duplicates = hardlinked_subgroups
            .into_iter()
            .map(|subgroup| subgroup.1)
            .collect();

        Ok(Some(DuplicateGroup {
            duplicates,
            redundant_bytes,
        }))
    }
}

/// Helper class to control how PathBuf is serialized. To avoid throwing errors
/// on malformed UTF-8. Works with VecVecPathBufSerializeHelper.
struct VecPathBufSerializeHelper<'a> {
    pub paths: &'a Vec<PathBuf>,
}

impl<'a> Serialize for VecPathBufSerializeHelper<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.paths.len()))?;
        for path in self.paths {
            seq.serialize_element(&path.as_os_str().to_string_lossy())?;
        }
        seq.end()
    }
}

/// Helper class to control how PathBuf is serialized. To avoid throwing errors
/// on malformed UTF-8. Works with VecPathBufSerializeHelper.
struct VecVecPathBufSerializeHelper<'a> {
    pub paths: &'a Vec<Vec<PathBuf>>,
}

impl<'a> Serialize for VecVecPathBufSerializeHelper<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.paths.len()))?;
        for path_vec in self.paths {
            seq.serialize_element(&VecPathBufSerializeHelper { paths: path_vec })?;
        }
        seq.end()
    }
}

impl Serialize for DuplicateGroup {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut ser = serializer.serialize_struct("DuplicateGroup", 2)?;
        ser.serialize_field(
            "duplicates",
            &VecVecPathBufSerializeHelper {
                paths: &self.duplicates,
            },
        )?;
        ser.serialize_field("redundant_bytes", &self.redundant_bytes)?;
        ser.end()
    }
}

#[test]
fn test_group_serialization() -> Result<()> {
    use crate::types::blob_to_str;
    use std::path::Path;

    let bad_str = blob_to_str(b"fran\xe7ais")?; // latin1 on-disk path on a utf8 distro
    let duplicates = DuplicateGroup {
        duplicates: vec![vec![
            Path::new(&bad_str).join("file1"),
            Path::new(&bad_str).join("file2"),
        ]],
        redundant_bytes: 0,
    };

    assert!(serde_json::to_string(&duplicates).is_ok());

    let duplicates = DuplicateGroup {
        duplicates: vec![vec![PathBuf::from("a/x"), PathBuf::from("b/x")]],
        redundant_bytes: 1,
    };
    assert_eq!(
        serde_json::to_string(&duplicates)?,
        "{\"duplicates\":[[\"a/x\",\"b/x\"]],\"redundant_bytes\":1}"
    );

    Ok(())
}
