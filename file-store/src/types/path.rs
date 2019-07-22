//! The [`ObjectPath`](struct.ObjectPath.html) types, used for identifying objects in storage.

use std::cmp::min;
use std::cmp::{Ord, Ordering};
use std::fmt;
use std::fs::metadata;
use std::path::{Path, PathBuf};

use super::error;

const PARENT_DIR: &str = "..";
const CURRENT_DIR: &str = ".";

#[derive(PartialEq, Clone, Debug)]
pub(crate) enum Prefix {
    VerbatimUNC(String, String),
    VerbatimDisk(u8),
    UNC(String, String),
    Disk(u8),
}

impl Prefix {
    fn is_drive_path(string: &str, start: usize, allow_forward: bool) -> bool {
        let buff = string.as_bytes();

        if buff.len() < start + 3 {
            false
        } else if buff[start].is_ascii_alphabetic() && buff[start + 1] == b':' {
            buff[start + 2] == b'\\' || (allow_forward && buff[start + 2] == b'/')
        } else {
            false
        }
    }

    fn try_parse(path: &str) -> Result<Option<(Prefix, usize)>, error::ObjectPathError> {
        if path.len() < 3 {
            return Ok(None);
        }

        if path.starts_with("\\\\?\\") {
            if path.starts_with("\\\\?\\UNC\\") {
                let (server, next) = StoragePath::find_separator(path, 8, false);
                if next == path.len() {
                    return Err(error::parse_error(
                        path,
                        "Incorrect format for verbatim UNC path.",
                    ));
                }
                let (share, last) = StoragePath::find_separator(path, next + 1, false);
                return Ok(Some((
                    Prefix::VerbatimUNC(server.to_owned(), share.to_owned()),
                    last,
                )));
            } else if Prefix::is_drive_path(path, 4, false) {
                if let Some(d) = path.bytes().nth(4) {
                    return Ok(Some((Prefix::VerbatimDisk(d), 6)));
                } else {
                    return Err(error::parse_error(path, "Unexpected failure."));
                }
            } else {
                return Err(error::parse_error(
                    path,
                    "Verbatim prefix did not match any supported form.",
                ));
            }
        }

        if Prefix::is_drive_path(path, 0, true) {
            return Ok(Some((Prefix::Disk(path.as_bytes()[0]), 2)));
        }

        if StoragePath::find_separator(path, 0, true) == ("", 0)
            && StoragePath::find_separator(path, 1, true) == ("", 1)
        {
            // Starts with two separators.
            let (server, next) = StoragePath::find_separator(path, 2, true);
            if next < path.len() {
                let (share, last) = StoragePath::find_separator(path, next + 1, true);
                return Ok(Some((
                    Prefix::UNC(server.to_owned(), share.to_owned()),
                    last,
                )));
            }
        }

        Ok(None)
    }

    fn allows_forward_slash(&self) -> bool {
        match self {
            Prefix::VerbatimUNC(_, _) => false,
            Prefix::VerbatimDisk(_) => false,
            _ => true,
        }
    }
}

impl fmt::Display for Prefix {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            // Good comparisons for all the UNC cases.
            Prefix::VerbatimUNC(ref server, ref share) => {
                f.write_fmt(format_args!("\\\\?\\UNC\\{}\\{}", server, share))
            }
            Prefix::VerbatimDisk(c) => f.write_fmt(format_args!("\\\\?\\{}:", char::from(*c))),
            Prefix::UNC(ref server, ref share) => {
                f.write_fmt(format_args!("\\\\{}\\{}", server, share))
            }
            Prefix::Disk(c) => f.write_fmt(format_args!("{}:", char::from(*c))),
        }
    }
}

impl Eq for Prefix {}

impl PartialOrd for Prefix {
    fn partial_cmp(&self, other: &Prefix) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn unc_compare(server_a: &str, share_a: &str, server_b: &str, share_b: &str) -> Ordering {
    let ord = server_a.cmp(server_b);
    if ord != Ordering::Equal {
        return ord;
    }
    share_a.cmp(share_b)
}

impl Ord for Prefix {
    fn cmp(&self, other: &Prefix) -> Ordering {
        if self == other {
            return Ordering::Equal;
        }

        match (self, other) {
            // Good comparisons for the disk cases.
            (Prefix::VerbatimDisk(a), Prefix::VerbatimDisk(b)) => a.cmp(b),
            (Prefix::Disk(a), Prefix::Disk(b)) => a.cmp(b),
            (Prefix::VerbatimDisk(a), Prefix::Disk(b)) => a.cmp(b),

            // Good comparisons for the UNC cases.
            (Prefix::VerbatimUNC(server_a, share_a), Prefix::VerbatimUNC(server_b, share_b)) => {
                unc_compare(server_a, share_a, server_b, share_b)
            }
            (Prefix::UNC(server_a, share_a), Prefix::UNC(server_b, share_b)) => {
                unc_compare(server_a, share_a, server_b, share_b)
            }
            (Prefix::VerbatimUNC(server_a, share_a), Prefix::UNC(server_b, share_b)) => {
                unc_compare(server_a, share_a, server_b, share_b)
            }

            // Now the questionable cases.
            (Prefix::Disk(_), Prefix::VerbatimUNC(_, _)) => Ordering::Less,
            (Prefix::Disk(_), Prefix::UNC(_, _)) => Ordering::Less,
            (Prefix::VerbatimDisk(_), Prefix::VerbatimUNC(_, _)) => Ordering::Less,
            (Prefix::VerbatimDisk(_), Prefix::UNC(_, _)) => Ordering::Less,

            _ => other.cmp(self),
        }
    }
}

/// A path in storage.
///
/// This is similar to PathBuf except that it supports windows and non-windows
/// style paths on all platforms. Generally all of the backends use non-windows
/// style paths for referencing files. This struct contains functions for
/// manipulating and parsing those paths.
///
/// One chief difference is that paths that end with `/` are considered to be
/// directories, those without are files.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StoragePath {
    pub(crate) prefix: Option<Prefix>,
    pub(crate) is_absolute: bool,
    pub(crate) directories: Vec<String>,
    pub(crate) filename: Option<String>,
}

impl StoragePath {
    fn find_separator(string: &str, start: usize, allow_forward: bool) -> (&str, usize) {
        let part = &string[start..];
        let mut pos = part.find('\\').unwrap_or_else(|| part.len());
        if allow_forward {
            pos = min(pos, part.find('/').unwrap_or_else(|| part.len()));
        }

        if pos == part.len() {
            // Separator not found.
            (&part[..], string.len())
        } else {
            // Separator found. Return the position of the separator character.
            (&part[..pos], start + pos)
        }
    }

    /// Creates a `StoragePath` from a `Path` from std.
    pub fn from_std_path(path: &Path) -> Result<StoragePath, error::ObjectPathError> {
        let is_dir = if let Ok(m) = metadata(path) {
            m.is_dir()
        } else {
            false
        };

        if let Some(string) = path.to_str() {
            let mut fspath = StoragePath::new(string)?;
            if is_dir {
                if let Some(f) = fspath.filename.clone() {
                    fspath.push_dir(&f);
                }
            }

            Ok(fspath)
        } else {
            Err(error::parse_error(
                &format!("{}", path.display()),
                "Path was not valid utf8.",
            ))
        }
    }

    /// Converts a `StoragePath` into a std `PathBuf`.
    pub fn as_std_path(&self) -> PathBuf {
        let mut path = self.to_string();
        if self.filename.is_none() && !self.directories.is_empty() {
            path.truncate(path.len() - 1);
        }
        PathBuf::from(path)
    }

    /// Parses a string into a new `StoragePath`.
    pub fn new<S: AsRef<str>>(from: S) -> Result<StoragePath, error::ObjectPathError> {
        let path = from.as_ref();
        let mut pos: usize = 0;
        let mut result: StoragePath = Default::default();
        let mut any_separator = true;

        if let Some((prefix, len)) = Prefix::try_parse(path)? {
            if !prefix.allows_forward_slash() {
                any_separator = false;
            }

            result.prefix = Some(prefix);
            pos = len;
        }

        let path = &path[pos..];
        pos = 0;

        while pos < path.len() {
            let (part, next) = StoragePath::find_separator(path, pos, any_separator);

            if next == 0 {
                result.is_absolute = true;
            } else if next == path.len() {
                if part == PARENT_DIR || part == CURRENT_DIR {
                    result.directories.push(part.to_owned());
                } else if !part.is_empty() {
                    result.filename = Some(part.to_owned());
                }
                break;
            } else {
                result.directories.push(part.to_owned());
            }

            pos = next + 1;
        }

        result.normalize()?;

        Ok(result)
    }

    /// Tests whether this `StoragePath` is an absolute path.
    pub fn is_absolute(&self) -> bool {
        self.is_absolute
    }

    /// Tests whether this `StoragePath` is expected to be a directory.
    pub fn is_directory(&self) -> bool {
        self.filename.is_none()
    }

    /// Tests whether this `StoragePath` is a windows style path.
    pub fn is_windows(&self) -> bool {
        self.prefix.is_some()
    }

    /// Returns true if either this path is absolute or when joining it with an
    /// absolute path will move above the absolute path's directory.
    pub fn is_above_base(&self) -> bool {
        self.is_absolute
            || (!self.directories.is_empty() && self.directories[0].as_str() == PARENT_DIR)
    }

    fn assert_is_normalized(&self) {
        if self.directories.is_empty() {
            return;
        }

        let mut pos = 0;
        if !self.is_absolute {
            while pos < self.directories.len() && self.directories[0].as_str() == PARENT_DIR {
                pos += 1;
            }
        }

        while pos < self.directories.len() {
            let part = self.directories[pos].as_str();
            if part.is_empty() || part == PARENT_DIR || part == CURRENT_DIR {
                panic!("Normalized path must not contain '{}'.", part);
            }
            pos += 1;
        }
    }

    fn normalize(&mut self) -> Result<(), error::ObjectPathError> {
        let mut pos = 0;
        while pos < self.directories.len() {
            match self.directories[pos].as_str() {
                "" => {
                    self.directories.remove(pos);
                }
                PARENT_DIR => {
                    if pos > 0 {
                        if self.directories[pos - 1].as_str() == PARENT_DIR {
                            pos += 1;
                        } else {
                            self.directories.remove(pos - 1);
                            self.directories.remove(pos - 1);
                            pos -= 1;
                        }
                    } else {
                        if self.is_absolute() {
                            return Err(error::parse_error(
                                &format!("{}", self),
                                "Cannot have remaining relative path parts in an absolute path.",
                            ));
                        }
                        pos += 1;
                    }
                }
                CURRENT_DIR => {
                    self.directories.remove(pos);
                }
                _ => pos += 1,
            }
        }

        self.assert_is_normalized();
        Ok(())
    }

    /// Returns a relative path that when joined to this path will return the
    /// target path.
    ///
    /// Both this `StoragePath` and the target `StoragePath` must be absolute.
    pub fn relative(&self, target: &StoragePath) -> Result<StoragePath, error::ObjectPathError> {
        if !self.is_absolute {
            return Err(error::parse_error(
                &format!("{}", self),
                "Start path must be absolute when generating a relative path.",
            ));
        }
        if !target.is_absolute {
            return Err(error::parse_error(
                &format!("{}", target),
                "Final path must be absolute when generating a relative path.",
            ));
        }
        if self.prefix != target.prefix {
            if let Some(ref prefix) = target.prefix {
                return Err(error::parse_error(&format!("{}", prefix), "Can only generate a relative path between two absolute paths with the same Windows prefix."));
            } else {
                return Err(error::parse_error("<none>", "Can only generate a relative path between two absolute paths with the same Windows prefix."));
            }
        }

        self.assert_is_normalized();
        target.assert_is_normalized();

        let mut relative: StoragePath = Default::default();
        relative.filename = target.filename.clone();

        let mut same_count = 0;
        let min_length = min(self.directories.len(), target.directories.len());
        while same_count < min_length
            && self.directories[same_count] == target.directories[same_count]
        {
            same_count += 1;
        }

        for _ in same_count..self.directories.len() {
            relative.directories.push(String::from(PARENT_DIR));
        }

        for i in same_count..target.directories.len() {
            relative.directories.push(target.directories[i].clone());
        }

        relative.assert_is_normalized();

        Ok(relative)
    }

    /// Joins a relative path to the current path and returns the result.
    pub fn join(&self, path: &StoragePath) -> Result<StoragePath, error::ObjectPathError> {
        self.assert_is_normalized();
        path.assert_is_normalized();

        if path.is_absolute {
            return Ok(path.clone());
        }

        let mut joined = self.clone();
        joined.filename = path.filename.clone();

        for dir in path.directories.iter() {
            joined.directories.push(dir.clone());
        }

        joined.normalize()?;

        Ok(joined)
    }

    /// Gets the filename for this `StoragePath` if there is one.
    pub fn filename(&self) -> Option<String> {
        self.filename.clone()
    }

    /// Moves this `StoragePath` to the named subdirectory.
    ///
    /// This will throw away the filename from the path.
    pub fn push_dir(&mut self, dir: &str) {
        self.directories.push(dir.to_owned());
        self.filename = None;
    }

    /// Overwrites the filename for this `StoragePath`.
    pub fn set_filename(&mut self, filename: &str) {
        self.filename = Some(filename.to_owned());
    }

    /// Converts this into a directory by moving the current filename into the
    /// list of directories.
    pub fn make_dir(&mut self) {
        if let Some(name) = self.filename.take() {
            self.push_dir(&name);
        }
    }

    /// Converts this into a file by moving the last path item into the
    /// filename.
    pub fn make_file(&mut self) {
        if self.filename.is_none() {
            self.filename = self.directories.pop();
        }
    }
}

impl fmt::Display for StoragePath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let separator = if self.prefix.is_some() { "\\" } else { "/" };

        if let Some(p) = &self.prefix {
            p.fmt(f)?
        }

        if self.is_absolute {
            f.write_str(separator)?;
        }

        f.write_str(&self.directories.join(separator))?;
        if !self.directories.is_empty() {
            f.write_str(separator)?;
        }

        if let Some(ref filename) = self.filename {
            f.write_str(filename.as_str())?;
        }

        Ok(())
    }
}

impl Eq for StoragePath {}

impl PartialOrd for StoragePath {
    fn partial_cmp(&self, other: &StoragePath) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StoragePath {
    fn cmp(&self, other: &StoragePath) -> Ordering {
        let selfp = format!("{}", self);
        let otherp = format!("{}", other);

        selfp.cmp(&otherp)
    }
}

impl AsRef<StoragePath> for StoragePath {
    fn as_ref(&self) -> &StoragePath {
        self
    }
}

#[cfg(test)]
#[allow(clippy::cognitive_complexity)]
mod tests {
    use super::*;

    fn directories(parts: Vec<&str>) -> Vec<String> {
        parts.iter().cloned().map(|s| s.to_owned()).collect()
    }

    #[test]
    fn test_path_parse_basic() -> Result<(), error::ObjectPathError> {
        let path = StoragePath::new("/foo/bar")?;
        assert_eq!(
            path,
            StoragePath {
                prefix: None,
                is_absolute: true,
                directories: directories(vec!["foo"]),
                filename: Some(String::from("bar")),
            }
        );
        assert_eq!(path.to_string(), "/foo/bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(!path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("foo/bar")?;
        assert_eq!(
            path,
            StoragePath {
                prefix: None,
                is_absolute: false,
                directories: directories(vec!["foo"]),
                filename: Some(String::from("bar")),
            }
        );
        assert_eq!(path.to_string(), "foo/bar");
        assert!(!path.is_absolute());
        assert!(!path.is_directory());
        assert!(!path.is_windows());
        assert!(!path.is_above_base());

        let path = StoragePath::new("foo/bar/")?;
        assert_eq!(
            path,
            StoragePath {
                prefix: None,
                is_absolute: false,
                directories: directories(vec!["foo", "bar"]),
                filename: None,
            }
        );
        assert_eq!(path.to_string(), "foo/bar/");
        assert!(!path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());
        assert!(!path.is_above_base());

        let path = StoragePath::new("/")?;
        assert_eq!(
            path,
            StoragePath {
                prefix: None,
                is_absolute: true,
                directories: directories(vec![]),
                filename: None,
            }
        );
        assert_eq!(path.to_string(), "/");
        assert!(path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("")?;
        assert_eq!(
            path,
            StoragePath {
                prefix: None,
                is_absolute: false,
                directories: directories(vec![]),
                filename: None,
            }
        );
        assert_eq!(path.to_string(), "");
        assert!(!path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());
        assert!(!path.is_above_base());

        let path = StoragePath::new("foo\\bar/")?;
        assert_eq!(
            path,
            StoragePath {
                prefix: None,
                is_absolute: false,
                directories: directories(vec!["foo", "bar"]),
                filename: None,
            }
        );
        assert_eq!(path.to_string(), "foo/bar/");
        assert!(!path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());
        assert!(!path.is_above_base());

        let path = StoragePath::new("\\foo\\bar")?;
        assert_eq!(
            path,
            StoragePath {
                prefix: None,
                is_absolute: true,
                directories: directories(vec!["foo"]),
                filename: Some(String::from("bar")),
            }
        );
        assert_eq!(path.to_string(), "/foo/bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(!path.is_windows());
        assert!(path.is_above_base());

        Ok(())
    }

    #[test]
    fn test_path_parse_windows() -> Result<(), error::ObjectPathError> {
        let path = StoragePath::new("C:\\foo\\bar")?;
        assert_eq!(
            path,
            StoragePath {
                prefix: Some(Prefix::Disk(b'C')),
                is_absolute: true,
                directories: directories(vec!["foo"]),
                filename: Some(String::from("bar")),
            }
        );
        assert_eq!(path.to_string(), "C:\\foo\\bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("C:/foo\\bar")?;
        assert_eq!(
            path,
            StoragePath {
                prefix: Some(Prefix::Disk(b'C')),
                is_absolute: true,
                directories: directories(vec!["foo"]),
                filename: Some(String::from("bar")),
            }
        );
        assert_eq!(path.to_string(), "C:\\foo\\bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("\\\\bar\\foo/test")?;
        assert_eq!(
            path,
            StoragePath {
                prefix: Some(Prefix::UNC(String::from("bar"), String::from("foo"))),
                is_absolute: true,
                directories: directories(vec![]),
                filename: Some(String::from("test")),
            }
        );
        assert_eq!(path.to_string(), "\\\\bar\\foo\\test");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("\\\\?\\C:\\foo\\bar")?;
        assert_eq!(
            path,
            StoragePath {
                prefix: Some(Prefix::VerbatimDisk(b'C')),
                is_absolute: true,
                directories: directories(vec!["foo"]),
                filename: Some(String::from("bar")),
            }
        );
        assert_eq!(path.to_string(), "\\\\?\\C:\\foo\\bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("\\\\?\\C:\\foo/bar")?;
        assert_eq!(
            path,
            StoragePath {
                prefix: Some(Prefix::VerbatimDisk(b'C')),
                is_absolute: true,
                directories: directories(vec![]),
                filename: Some(String::from("foo/bar")),
            }
        );
        assert_eq!(path.to_string(), "\\\\?\\C:\\foo/bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("\\\\?\\UNC\\bar\\foo\\test")?;
        assert_eq!(
            path,
            StoragePath {
                prefix: Some(Prefix::VerbatimUNC(
                    String::from("bar"),
                    String::from("foo")
                )),
                is_absolute: true,
                directories: directories(vec![]),
                filename: Some(String::from("test")),
            }
        );
        assert_eq!(path.to_string(), "\\\\?\\UNC\\bar\\foo\\test");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(path.is_windows());
        assert!(path.is_above_base());

        Ok(())
    }

    #[test]
    fn test_path_normalize() -> Result<(), error::ObjectPathError> {
        let path = StoragePath::new("/foo/../bar")?;
        assert_eq!(path.to_string(), "/bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(!path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("/foo/../bar/")?;
        assert_eq!(path.to_string(), "/bar/");
        assert!(path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("/foo/baz//diz/.././bar/")?;
        assert_eq!(path.to_string(), "/foo/baz/bar/");
        assert!(path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("../baz/../../diz")?;
        assert_eq!(path.to_string(), "../../diz");
        assert!(!path.is_absolute());
        assert!(!path.is_directory());
        assert!(!path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("../foo/./../bar/")?;
        assert_eq!(path.to_string(), "../bar/");
        assert!(!path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("/foo/bar/..")?;
        assert_eq!(path.to_string(), "/foo/");
        assert!(path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("/foo/bar/.")?;
        assert_eq!(path.to_string(), "/foo/bar/");
        assert!(path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("./")?;
        assert_eq!(path.to_string(), "");
        assert!(!path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());
        assert!(!path.is_above_base());

        let path = StoragePath::new(".")?;
        assert_eq!(path.to_string(), "");
        assert!(!path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());
        assert!(!path.is_above_base());

        let path = StoragePath::new("../")?;
        assert_eq!(path.to_string(), "../");
        assert!(!path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());
        assert!(path.is_above_base());

        let path = StoragePath::new("..")?;
        assert_eq!(path.to_string(), "../");
        assert!(!path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());
        assert!(path.is_above_base());

        Ok(())
    }

    #[test]
    fn test_path_join() -> Result<(), error::ObjectPathError> {
        let base = StoragePath::new("/foo/bar")?;
        let sub = StoragePath::new("test/baz")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/foo/test/baz");
        assert!(joined.is_absolute());
        assert!(!joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar/")?;
        let sub = StoragePath::new("test/baz")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/foo/bar/test/baz");
        assert!(joined.is_absolute());
        assert!(!joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar/")?;
        let sub = StoragePath::new("test/baz/")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/foo/bar/test/baz/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("C:\\")?;
        let sub = StoragePath::new("test/baz/")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "C:\\test\\baz\\");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/")?;
        let sub = StoragePath::new("test/baz/")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/test/baz/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar")?;
        let sub = StoragePath::new("../")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar")?;
        let sub = StoragePath::new("..")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar/")?;
        let sub = StoragePath::new("../baz")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/foo/baz");
        assert!(joined.is_absolute());
        assert!(!joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar/")?;
        let sub = StoragePath::new("./")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/foo/bar/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar/")?;
        let sub = StoragePath::new(".")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/foo/bar/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar/")?;
        let sub = StoragePath::new("./..")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/foo/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar")?;
        let sub = StoragePath::new("./")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/foo/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar")?;
        let sub = StoragePath::new("..")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar")?;
        let sub = StoragePath::new("")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/foo/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar")?;
        let sub = StoragePath::new("baz")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/foo/baz");
        assert!(joined.is_absolute());
        assert!(!joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar/")?;
        let sub = StoragePath::new("")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/foo/bar/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/foo/bar/")?;
        let sub = StoragePath::new("baz")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/foo/bar/baz");
        assert!(joined.is_absolute());
        assert!(!joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        let base = StoragePath::new("/")?;
        let sub = StoragePath::new("foo/bar/baz")?;
        let joined = base.join(&sub)?;
        assert_eq!(joined.to_string(), "/foo/bar/baz");
        assert!(joined.is_absolute());
        assert!(!joined.is_directory());
        assert!(!joined.is_windows());
        assert!(joined.is_above_base());

        Ok(())
    }

    #[test]
    fn test_path_relative() -> Result<(), error::ObjectPathError> {
        let base = StoragePath::new("/foo/bar")?;
        let next = StoragePath::new("/foo/baz")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "baz");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(!relative.is_above_base());

        let base = StoragePath::new("/foo/bar/")?;
        let next = StoragePath::new("/foo/baz")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../baz");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/")?;
        let next = StoragePath::new("/foo/baz/")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../baz/");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar")?;
        let next = StoragePath::new("/foo/baz/")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "baz/");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());
        assert!(!relative.is_above_base());

        let base = StoragePath::new("/foo/bar")?;
        let next = StoragePath::new("/foo/bar")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "bar");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(!relative.is_above_base());

        let base = StoragePath::new("/foo/bar/")?;
        let next = StoragePath::new("/foo/bar/")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());
        assert!(!relative.is_above_base());

        let base = StoragePath::new("/foo/bar/")?;
        let next = StoragePath::new("/foo/bar")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../bar");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar")?;
        let next = StoragePath::new("/foo/bar/")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "bar/");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());
        assert!(!relative.is_above_base());

        let base = StoragePath::new("/foo/bar/")?;
        let next = StoragePath::new("/foo/")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz")?;
        let next = StoragePath::new("/foo/bar/bad/gah")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "bad/gah");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(!relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz")?;
        let next = StoragePath::new("/foo/bad/gah")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../bad/gah");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz")?;
        let next = StoragePath::new("/foo/bar/baz")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "baz");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(!relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz")?;
        let next = StoragePath::new("/foo/bar")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../bar");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz")?;
        let next = StoragePath::new("/foo/bar/")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());
        assert!(!relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz")?;
        let next = StoragePath::new("/foo/bar/baz/")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "baz/");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());
        assert!(!relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz")?;
        let next = StoragePath::new("/foo/bar/baz/gad")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "baz/gad");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(!relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/")?;
        let next = StoragePath::new("/foo/bar/bad/gah")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../bad/gah");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/")?;
        let next = StoragePath::new("/foo/bad/gah")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../../bad/gah");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/")?;
        let next = StoragePath::new("/foo/bar/baz")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../baz");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/")?;
        let next = StoragePath::new("/foo/bar")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../../bar");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/")?;
        let next = StoragePath::new("/foo/bar/")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/")?;
        let next = StoragePath::new("/foo/bar/baz/")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());
        assert!(!relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/")?;
        let next = StoragePath::new("/foo/bar/baz/gad")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "gad");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(!relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/gah/ooh")?;
        let next = StoragePath::new("/foo/bar/bad/gah")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../../bad/gah");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/gah/ooh")?;
        let next = StoragePath::new("/foo/bad/gah")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../../../bad/gah");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/gah/ooh")?;
        let next = StoragePath::new("/foo/bar/baz")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../../baz");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/gah/ooh")?;
        let next = StoragePath::new("/foo/bar")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../../../bar");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/gah/ooh")?;
        let next = StoragePath::new("/foo/bar/")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../../");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/gah/ooh")?;
        let next = StoragePath::new("/foo/bar/baz/")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/foo/bar/baz/gah/ooh")?;
        let next = StoragePath::new("/foo/bar/baz/gad")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "../gad");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(relative.is_above_base());

        let base = StoragePath::new("/")?;
        let next = StoragePath::new("/foo/bar/baz/gad")?;
        let relative = base.relative(&next)?;
        assert_eq!(relative.to_string(), "foo/bar/baz/gad");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());
        assert!(!relative.is_above_base());

        Ok(())
    }

    #[test]
    fn test_make_dir() -> Result<(), error::ObjectPathError> {
        fn test_into_dir(path: &str, expected: &str) -> Result<(), error::ObjectPathError> {
            let mut file = StoragePath::new(path)?;
            file.make_dir();
            assert_eq!(file.to_string(), expected);
            Ok(())
        }

        test_into_dir("/", "/")?;
        test_into_dir("", "")?;
        test_into_dir("foo", "foo/")?;
        test_into_dir("/foo", "/foo/")?;
        test_into_dir("foo/", "foo/")?;
        test_into_dir("/foo/", "/foo/")?;
        test_into_dir("foo/bar", "foo/bar/")?;
        test_into_dir("/foo/bar", "/foo/bar/")?;
        test_into_dir("foo/bar/", "foo/bar/")?;
        test_into_dir("/foo/bar/", "/foo/bar/")?;

        Ok(())
    }

    #[test]
    fn test_make_file() -> Result<(), error::ObjectPathError> {
        fn test_into_file(path: &str, expected: &str) -> Result<(), error::ObjectPathError> {
            let mut file = StoragePath::new(path)?;
            file.make_file();
            assert_eq!(file.to_string(), expected);
            Ok(())
        }

        test_into_file("/", "/")?;
        test_into_file("", "")?;
        test_into_file("foo", "foo")?;
        test_into_file("/foo", "/foo")?;
        test_into_file("foo/", "foo")?;
        test_into_file("/foo/", "/foo")?;
        test_into_file("foo/bar", "foo/bar")?;
        test_into_file("/foo/bar", "/foo/bar")?;
        test_into_file("foo/bar/", "foo/bar")?;
        test_into_file("/foo/bar/", "/foo/bar")?;

        Ok(())
    }
}
