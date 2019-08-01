//! Object types.

use std::cmp::Ordering;
use std::fmt;

use super::*;

/// An object's type. For most backends this will just be File.
///
/// This crate really only deals with file manipulations and most backends only
/// support files (in some cases called objects). Things like directories often
/// don't really exist. In some cases though some backends do have real
/// directories and would not support creating a file of the same name without
/// removing them first. This represents the type of thing that exists at a
/// particular path.
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum ObjectType {
    /// A regular file.
    File,
    /// A physical directory.
    Directory,
    /// An physical object of unknown type.
    Unknown,
}

impl Eq for ObjectType {}

impl PartialOrd for ObjectType {
    fn partial_cmp(&self, other: &ObjectType) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ObjectType {
    fn cmp(&self, other: &ObjectType) -> Ordering {
        if self == other {
            return Ordering::Equal;
        }

        match self {
            ObjectType::Directory => Ordering::Less,
            ObjectType::File => other.cmp(self),
            ObjectType::Unknown => Ordering::Greater,
        }
    }
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ObjectType::File => f.pad("file"),
            ObjectType::Directory => f.pad("dir"),
            ObjectType::Unknown => f.pad("unknown"),
        }
    }
}

/// An object of some kind that exists at a poth in the storage system.
///
/// Most backends only support File objects, and this crate only really supports
/// manipulating file objects. This type does however support the idea of a non
/// file type that physically exists at a path.
#[derive(Clone, PartialEq, Debug)]
pub struct Object {
    pub(crate) object_type: ObjectType,
    pub(crate) path: StoragePath,
    pub(crate) size: u64,
}

impl Object {
    /// Gets the object's path.
    pub fn path(&self) -> StoragePath {
        self.path.clone()
    }

    /// Gets the object's size.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Gets the object's type.
    pub fn object_type(&self) -> ObjectType {
        self.object_type
    }
}

impl Eq for Object {}

impl PartialOrd for Object {
    fn partial_cmp(&self, other: &Object) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Object {
    fn cmp(&self, other: &Object) -> Ordering {
        self.path.cmp(&other.path)
    }
}
