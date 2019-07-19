use std::cmp::Ordering;

use super::*;

/// A file's type. For most backends this will just be File.
///
/// This crate really only deals with file manipulations and most backends only
/// support regular files and things like directories don't really exist.
/// In some cases though some backends do have real directories and would not
/// support creating a file of the same name. This gives the type of the file.
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum FsFileType {
    /// A regular file.
    File,
    /// A physical directory.
    Directory,
    /// An physical object of unknown type.
    Unknown,
}

impl Eq for FsFileType {}

impl PartialOrd for FsFileType {
    fn partial_cmp(&self, other: &FsFileType) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FsFileType {
    fn cmp(&self, other: &FsFileType) -> Ordering {
        if self == other {
            return Ordering::Equal;
        }

        match self {
            FsFileType::Directory => Ordering::Less,
            FsFileType::File => other.cmp(self),
            FsFileType::Unknown => Ordering::Greater,
        }
    }
}

/// A file in storage.
#[derive(Clone, PartialEq, Debug)]
pub struct FsFile {
    pub(crate) file_type: FsFileType,
    pub(crate) path: FsPath,
    pub(crate) size: u64,
}

impl FsFile {
    /// Gets the file's path.
    pub fn path(&self) -> FsPath {
        self.path.clone()
    }

    /// Gets the file's size.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Gets the file's type.
    pub fn file_type(&self) -> FsFileType {
        self.file_type
    }
}

impl Eq for FsFile {}

impl PartialOrd for FsFile {
    fn partial_cmp(&self, other: &FsFile) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FsFile {
    fn cmp(&self, other: &FsFile) -> Ordering {
        self.path.cmp(&other.path)
    }
}
