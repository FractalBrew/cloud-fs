//! The [`ObjectPath`](struct.ObjectPath.html) type, used for identifying objects in storage.
use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;

use super::error;

/// A path in storage.
///
/// Most storage systems are simple key -> data stores with the key being any
/// string and so an ObjectPath is basically just a thin wrapper around a
/// string. However most uses of storage use the notion of a hierarchical
/// directory structure, even if one does not really exist, by using names
/// separated by the `/` characters. These names are called directory parts
/// throughout these docs and some storage backends add additional meaning to
/// these parts.
///
/// Paths to objects must not start with a `/` character. For all methods other
/// than [`list_objects`](struct.FileStore.html#method.list_objects) the path
/// also must not end with a `/` character.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectPath {
    path: String,
}

impl ObjectPath {
    /// Parses a string into a new `ObjectPath`.
    pub fn new<S: AsRef<str>>(from: S) -> Result<ObjectPath, error::StorageError> {
        let path = from.as_ref();
        if path.starts_with('/') {
            Err(error::parse_error(
                path,
                "ObjectPaths cannot start with the '/' character.",
            ))
        } else {
            Ok(ObjectPath {
                path: path.to_owned(),
            })
        }
    }

    /// Creates an empty `ObjectPath`. Can never fail.
    pub fn empty() -> ObjectPath {
        ObjectPath {
            path: String::new(),
        }
    }

    /// Splits this path into directory parts.
    pub fn parts(&self) -> Vec<&str> {
        if self.path.is_empty() {
            vec![]
        } else {
            self.path.split('/').collect()
        }
    }

    /// Pushes a new directory part to the end of this path.
    pub fn push_part(&mut self, part: &str) {
        if !self.path.is_empty() {
            self.path.push('/');
        }
        self.path.push_str(part);
    }

    /// Shifts a new directory part to the start of this path.
    pub fn shift_part(&mut self, part: &str) {
        if !self.path.is_empty() {
            self.path = format!("{}/{}", part, self.path);
        } else {
            self.path = part.to_owned();
        }
    }

    /// Pops the last directory part from the end of this path.
    pub fn pop_part(&mut self) -> Option<String> {
        if !self.path.is_empty() {
            match self.path.rfind('/') {
                Some(pos) => {
                    let popped = self.path[pos + 1..].to_owned();
                    self.path = self.path[0..pos].to_owned();
                    Some(popped)
                }
                None => {
                    let popped = self.path.to_owned();
                    self.path = String::new();
                    Some(popped)
                }
            }
        } else {
            None
        }
    }

    /// Unshifts the first directory part from the start of this path.
    pub fn unshift_part(&mut self) -> Option<String> {
        if !self.path.is_empty() {
            match self.path.find('/') {
                Some(pos) => {
                    let popped = self.path[0..pos].to_owned();
                    self.path = self.path[pos + 1..].to_owned();
                    Some(popped)
                }
                None => {
                    let popped = self.path.to_owned();
                    self.path = String::new();
                    Some(popped)
                }
            }
        } else {
            None
        }
    }

    /// Joins two paths with the `/` character in between.
    pub fn join(&self, other: &ObjectPath) -> ObjectPath {
        if self.path.is_empty() {
            other.clone()
        } else {
            let mut new = self.clone();

            for part in other.parts() {
                new.push_part(part);
            }

            new
        }
    }

    /// Checks whether this path is prefixed by the given path.
    pub fn starts_with(&self, other: &ObjectPath) -> bool {
        self.path.starts_with(&other.path)
    }

    /// Returns whether the path is empty or ends with a `/` character.
    pub(crate) fn is_dir_prefix(&self) -> bool {
        self.path.is_empty() || self.path.ends_with('/')
    }

    /// Returns whether this path is empty or not.
    pub fn is_empty(&self) -> bool {
        self.path.is_empty()
    }
}

impl fmt::Display for ObjectPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(&self.path)
    }
}

impl FromStr for ObjectPath {
    type Err = error::StorageError;

    fn from_str(s: &str) -> Result<ObjectPath, error::StorageError> {
        ObjectPath::new(s)
    }
}

impl TryFrom<&str> for ObjectPath {
    type Error = error::StorageError;

    fn try_from(s: &str) -> Result<ObjectPath, error::StorageError> {
        ObjectPath::new(s)
    }
}
