//! The [`ObjectPath`](struct.ObjectPath.html) type, used for identifying objects in storage.
use std::fmt;

use super::error;

/// A path in storage.
///
/// Most storage systems are simple key -> data stores with the key being any
/// string and so an ObjectPath is basically just a thin wrapper around a
/// string for now.
///
/// For storage systems that do support hierarchical directory strucutes the
/// character `/` in a path is used to represent the directory separator,
/// regardless of what the underlying storage system actually uses.
///
/// One constraint is currently placed on paths. Paths to objects must not start
/// or end with a `/` character. The path for So paths may be thought of as relative to the root of the
/// storage system.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectPath {
    path: String,
}

impl ObjectPath {
    /// Parses a string into a new `ObjectPath`.
    pub fn new<S: AsRef<str>>(from: S) -> Result<ObjectPath, error::ObjectPathError> {
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

    /// Splits this path into directory parts.
    pub fn parts(&self) -> impl Iterator<Item = &str> {
        self.path.split('/')
    }

    /// Pushes a new directory part to the end of this path.
    pub fn push_part(&mut self, part: &str) {
        if !self.path.is_empty() {
            self.path.push('/');
        }
        self.path.push_str(part);
    }

    /// Pops the last directoryt part from the end of this path.
    pub fn pop_part(&mut self) {
        if !self.path.is_empty() {
            match self.path.rfind('/') {
                Some(pos) => self.path = self.path[0..pos].to_owned(),
                None => self.path = String::new(),
            }
        }
    }

    /// Checks whether this path is prefixed by the given path.
    pub fn starts_with(&self, other: &ObjectPath) -> bool {
        self.path.starts_with(&other.path)
    }
}

impl fmt::Display for ObjectPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(&self.path)
    }
}
