use std::cmp::min;
use std::cmp::{Ord, Ordering};
use std::error::Error;
use std::fmt;
use std::io;
use std::path::Path;

use bytes::Bytes;

use crate::backends::Backend;

pub type Data = Bytes;

#[derive(Clone, Debug)]
pub enum FsErrorType {
    ParseError,
    HostNotSupported,
    InvalidPath,
    Other,
}

#[derive(Clone, Debug)]
pub struct FsError {
    error_type: FsErrorType,
    description: String,
}

impl FsError {
    pub(crate) fn new<S: AsRef<str>>(error_type: FsErrorType, description: S) -> FsError {
        FsError {
            error_type,
            description: description.as_ref().to_owned(),
        }
    }

    pub(crate) fn from_error<E>(error: E) -> FsError
    where
        E: Error + fmt::Display,
    {
        Self::new(FsErrorType::Other, format!("{}", error))
    }
}

impl fmt::Display for FsError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(&self.description)
    }
}

impl Error for FsError {}

impl From<io::Error> for FsError {
    fn from(e: io::Error) -> FsError {
        FsError::from_error(e)
    }
}

pub type FsResult<R> = Result<R, FsError>;

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

    fn try_parse(path: &str) -> FsResult<Option<(Prefix, usize)>> {
        if path.len() < 3 {
            return Ok(None);
        }

        if path.starts_with("\\\\?\\") {
            if path.starts_with("\\\\?\\UNC\\") {
                let (server, next) = FsPath::find_separator(path, 8, false);
                if next == path.len() {
                    return Err(FsError::new(
                        FsErrorType::ParseError,
                        "Incorrect format for verbatim UNC path.",
                    ));
                }
                let (share, last) = FsPath::find_separator(path, next + 1, false);
                return Ok(Some((
                    Prefix::VerbatimUNC(server.to_owned(), share.to_owned()),
                    last,
                )));
            } else if Prefix::is_drive_path(path, 4, false) {
                if let Some(d) = path.bytes().nth(4) {
                    return Ok(Some((Prefix::VerbatimDisk(d), 6)));
                } else {
                    return Err(FsError::new(FsErrorType::ParseError, "Unexpected failure."));
                }
            } else {
                return Err(FsError::new(
                    FsErrorType::ParseError,
                    "Verbatim prefix did not match any supported form.",
                ));
            }
        }

        if Prefix::is_drive_path(path, 0, true) {
            return Ok(Some((Prefix::Disk(path.as_bytes()[0]), 2)));
        }

        if FsPath::find_separator(path, 0, true) == ("", 0)
            && FsPath::find_separator(path, 1, true) == ("", 1)
        {
            // Starts with two separators.
            let (server, next) = FsPath::find_separator(path, 2, true);
            if next < path.len() {
                let (share, last) = FsPath::find_separator(path, next + 1, true);
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

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FsPath {
    pub(crate) prefix: Option<Prefix>,
    pub(crate) components: Vec<String>,
}

impl FsPath {
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

    pub fn from_std_path(path: &Path) -> FsResult<FsPath> {
        if let Some(string) = path.to_str() {
            FsPath::new(string)
        } else {
            Err(FsError::new(
                FsErrorType::ParseError,
                "Path was not valid utf8.",
            ))
        }
    }

    pub fn new<S: AsRef<str>>(from: S) -> FsResult<FsPath> {
        let path = from.as_ref();
        let mut pos: usize = 0;
        let mut result: FsPath = Default::default();
        let mut any_separator = true;

        if let Some((prefix, len)) = Prefix::try_parse(path)? {
            if !prefix.allows_forward_slash() {
                any_separator = false;
            }

            result.prefix = Some(prefix);
            pos = len;
        }

        if pos == path.len() {
            return Ok(result);
        }

        loop {
            let (part, next) = FsPath::find_separator(path, pos, any_separator);
            result.components.push(part.to_owned());
            if next == path.len() {
                break;
            }
            pos = next + 1;
        }

        Ok(result)
    }

    pub fn is_absolute(&self) -> bool {
        self.components.first().map(|s| s.as_str()) == Some("")
    }

    pub fn is_directory(&self) -> bool {
        if let Some(part) = self.components.last() {
            match part.as_ref() {
                "" => true,
                CURRENT_DIR => true,
                PARENT_DIR => true,
                _ => false,
            }
        } else {
            true
        }
    }

    pub fn is_windows(&self) -> bool {
        self.prefix.is_some()
    }

    pub fn normalize(&mut self) -> FsResult<()> {
        let mut pos = 0;
        while pos < self.components.len() {
            match self.components[pos].as_str() {
                "" => {
                    if pos == 0 || pos == self.components.len() - 1 {
                        pos += 1;
                    } else {
                        self.components.remove(pos);
                    }
                }
                CURRENT_DIR => {
                    if pos == self.components.len() - 1 {
                        self.components[pos] = String::new();
                        pos += 1;
                    } else {
                        self.components.remove(pos);
                    }
                }
                PARENT_DIR => {
                    if pos == 0 || self.components[pos - 1].as_str() == PARENT_DIR {
                        pos += 1;
                    } else if pos == 1 && self.components[0].is_empty() {
                        return Err(FsError::new(
                            FsErrorType::ParseError,
                            "Cannot move above the root",
                        ));
                    } else {
                        self.components.remove(pos - 1);

                        if pos == self.components.len() {
                            self.components[pos - 1] = String::new();
                        } else {
                            self.components.remove(pos - 1);
                            pos -= 1;
                        }
                    }
                }
                _ => pos += 1,
            }
        }

        Ok(())
    }

    pub fn relative(&self, path: &FsPath) -> FsResult<FsPath> {
        if !self.is_absolute() || !path.is_absolute() {
            Err(FsError::new(
                FsErrorType::ParseError,
                "Both paths must be absolute to create a relative path.",
            ))
        } else if self.prefix != path.prefix {
            Err(FsError::new(
                FsErrorType::ParseError,
                "Both paths must use the same prefix to create a relative path.",
            ))
        } else {
            let mut base = self.clone();
            base.normalize()?;
            let mut target = path.clone();
            target.normalize()?;

            println!("{} -> {}", base, target);
            let mut result: FsPath = Default::default();

            let count = min(base.components.len(), target.components.len());
            let mut i = 0;
            while i < count && base.components[i] == target.components[i] {
                i += 1;
            }

            let (move_up, mut add_from) =
                if i == count && base.components.len() == target.components.len() {
                    // Both paths are the same, but if the path is not a directory
                    // we must use the last part as the relative path.
                    if base.is_directory() {
                        (0, target.components.len())
                    } else {
                        (0, i - 1)
                    }
                } else if i == base.components.len() {
                    // base cannot be a directory in this case so we must reuse the
                    // last part unless this is the root.
                    if i == 1 {
                        (0, i)
                    } else {
                        (0, i - 1)
                    }
                } else if i == target.components.len() {
                    // target cannot be a directory in this case so we must walk up
                    // the remaining parts of base and reuse the last part.
                    (base.components.len() - i, i - 1)
                } else if base.components[i].is_empty() {
                    // target must be a sub-path of base here, we just need to add
                    // the rest of it.
                    (0, i)
                } else if target.components[i].is_empty() {
                    // base is a sub-path of target, we just need to move up enough.
                    (base.components.len() - i - 1, target.components.len())
                } else {
                    // The more generic case. Shared prefix. Something extra in both.
                    (base.components.len() - i - 1, i)
                };

            println!("{} matched, move up {}, add from {}", i, move_up, add_from);

            for _ in 0..move_up {
                result.components.push(String::from(PARENT_DIR));
            }

            while add_from < target.components.len() {
                result.components.push(target.components[add_from].clone());
                add_from += 1;
            }

            Ok(result)
        }
    }

    pub fn join(&self, path: &FsPath) -> FsResult<FsPath> {
        if !self.is_absolute() {
            Err(FsError::new(
                FsErrorType::ParseError,
                "Cannot join to a relative path.",
            ))
        } else if path.is_absolute() {
            Ok(path.clone())
        } else {
            let mut result = self.clone();
            result.components.pop();

            if path.components.is_empty() {
                result.components.push(String::new());
            } else {
                for component in &path.components {
                    result.components.push(component.clone());
                }
            }

            Ok(result)
        }
    }
}

impl fmt::Display for FsPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let separator = if self.prefix.is_some() { "\\" } else { "/" };

        if let Some(p) = &self.prefix {
            p.fmt(f)?
        }

        f.write_str(&self.components.join(separator))
    }
}

impl Eq for FsPath {}

impl PartialOrd for FsPath {
    fn partial_cmp(&self, other: &FsPath) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FsPath {
    fn cmp(&self, other: &FsPath) -> Ordering {
        if self.is_absolute() != other.is_absolute() {
            // Weird case.
            if self.is_absolute() {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        } else {
            for i in 0..min(self.components.len(), other.components.len()) {
                let ord = self.components[i].cmp(&other.components[i]);
                if ord != Ordering::Equal {
                    return ord;
                }
            }

            if self.components.len() < other.components.len() {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
    }
}

impl AsRef<FsPath> for FsPath {
    fn as_ref(&self) -> &FsPath {
        self
    }
}

#[derive(Clone, Debug)]
pub struct FsSettings {
    pub(crate) backend: Backend,
    pub(crate) path: FsPath,
}

impl FsSettings {
    pub fn new(backend: Backend, path: FsPath) -> FsSettings {
        FsSettings { backend, path }
    }

    pub fn backend(&self) -> &Backend {
        &self.backend
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct FsFile {
    pub(crate) path: FsPath,
    pub(crate) size: Option<u64>,
}

impl FsFile {
    pub fn path(&self) -> &FsPath {
        &self.path
    }

    pub fn size(&self) -> Option<u64> {
        self.size
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

#[cfg(test)]
#[allow(clippy::cognitive_complexity)]
mod tests {
    use super::*;

    fn components(parts: Vec<&str>) -> Vec<String> {
        parts.iter().cloned().map(|s| s.to_owned()).collect()
    }

    #[test]
    fn test_path_parse_basic() -> FsResult<()> {
        let path = FsPath::new("/foo/bar")?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "bar",]),
            }
        );
        assert_eq!(path.to_string(), "/foo/bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(!path.is_windows());

        let path = FsPath::new("foo/bar")?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: components(vec!["foo", "bar",]),
            }
        );
        assert_eq!(path.to_string(), "foo/bar");
        assert!(!path.is_absolute());
        assert!(!path.is_directory());
        assert!(!path.is_windows());

        let path = FsPath::new("foo/bar/")?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: components(vec!["foo", "bar", "",]),
            }
        );
        assert_eq!(path.to_string(), "foo/bar/");
        assert!(!path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());

        let path = FsPath::new("/")?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: components(vec!["", "",]),
            }
        );
        assert_eq!(path.to_string(), "/");
        assert!(path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());

        let path = FsPath::new("")?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: vec![],
            }
        );
        assert_eq!(path.to_string(), "");
        assert!(!path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());

        let path = FsPath::new("foo\\bar/")?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: components(vec!["foo", "bar", "",]),
            }
        );
        assert_eq!(path.to_string(), "foo/bar/");
        assert!(!path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());

        let path = FsPath::new("\\foo\\bar")?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "bar",]),
            }
        );
        assert_eq!(path.to_string(), "/foo/bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(!path.is_windows());

        Ok(())
    }

    #[test]
    fn test_path_parse_windows() -> FsResult<()> {
        let path = FsPath::new("C:\\foo\\bar")?;
        assert_eq!(
            path,
            FsPath {
                prefix: Some(Prefix::Disk(b'C')),
                components: components(vec!["", "foo", "bar",]),
            }
        );
        assert_eq!(path.to_string(), "C:\\foo\\bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(path.is_windows());

        let path = FsPath::new("C:/foo\\bar")?;
        assert_eq!(
            path,
            FsPath {
                prefix: Some(Prefix::Disk(b'C')),
                components: components(vec!["", "foo", "bar",]),
            }
        );
        assert_eq!(path.to_string(), "C:\\foo\\bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(path.is_windows());

        let path = FsPath::new("\\\\bar\\foo/test")?;
        assert_eq!(
            path,
            FsPath {
                prefix: Some(Prefix::UNC(String::from("bar"), String::from("foo"))),
                components: components(vec!["", "test",]),
            }
        );
        assert_eq!(path.to_string(), "\\\\bar\\foo\\test");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(path.is_windows());

        let path = FsPath::new("\\\\?\\C:\\foo\\bar")?;
        assert_eq!(
            path,
            FsPath {
                prefix: Some(Prefix::VerbatimDisk(b'C')),
                components: components(vec!["", "foo", "bar",]),
            }
        );
        assert_eq!(path.to_string(), "\\\\?\\C:\\foo\\bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(path.is_windows());

        let path = FsPath::new("\\\\?\\C:\\foo/bar")?;
        assert_eq!(
            path,
            FsPath {
                prefix: Some(Prefix::VerbatimDisk(b'C')),
                components: components(vec!["", "foo/bar",]),
            }
        );
        assert_eq!(path.to_string(), "\\\\?\\C:\\foo/bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(path.is_windows());

        let path = FsPath::new("\\\\?\\UNC\\bar\\foo\\test")?;
        assert_eq!(
            path,
            FsPath {
                prefix: Some(Prefix::VerbatimUNC(
                    String::from("bar"),
                    String::from("foo")
                )),
                components: components(vec!["", "test",]),
            }
        );
        assert_eq!(path.to_string(), "\\\\?\\UNC\\bar\\foo\\test");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(path.is_windows());

        Ok(())
    }

    #[test]
    fn test_path_normalize() -> FsResult<()> {
        let mut path = FsPath::new("/foo/../bar")?;
        path.normalize()?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: components(vec!["", "bar",]),
            }
        );
        assert_eq!(path.to_string(), "/bar");
        assert!(path.is_absolute());
        assert!(!path.is_directory());
        assert!(!path.is_windows());

        let mut path = FsPath::new("/foo/../bar/")?;
        path.normalize()?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: components(vec!["", "bar", "",]),
            }
        );
        assert_eq!(path.to_string(), "/bar/");
        assert!(path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());

        let mut path = FsPath::new("/foo/baz//diz/.././bar/")?;
        path.normalize()?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "baz", "bar", "",]),
            }
        );
        assert_eq!(path.to_string(), "/foo/baz/bar/");
        assert!(path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());

        let mut path = FsPath::new("../baz/../../diz")?;
        path.normalize()?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: components(vec!["..", "..", "diz",]),
            }
        );
        assert_eq!(path.to_string(), "../../diz");
        assert!(!path.is_absolute());
        assert!(!path.is_directory());
        assert!(!path.is_windows());

        let mut path = FsPath::new("../foo/./../bar/")?;
        path.normalize()?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: components(vec!["..", "bar", "",]),
            }
        );
        assert_eq!(path.to_string(), "../bar/");
        assert!(!path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());

        let mut path = FsPath::new("/foo/bar/..")?;
        path.normalize()?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "",]),
            }
        );
        assert_eq!(path.to_string(), "/foo/");
        assert!(path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());

        let mut path = FsPath::new("/foo/bar/.")?;
        path.normalize()?;
        assert_eq!(
            path,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "bar", "",]),
            }
        );
        assert_eq!(path.to_string(), "/foo/bar/");
        assert!(path.is_absolute());
        assert!(path.is_directory());
        assert!(!path.is_windows());

        Ok(())
    }

    #[test]
    fn test_path_join() -> FsResult<()> {
        let base = FsPath::new("/foo/bar")?;
        let sub = FsPath::new("test/baz")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "test", "baz",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/test/baz");
        assert!(joined.is_absolute());
        assert!(!joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar/")?;
        let sub = FsPath::new("test/baz")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "bar", "test", "baz",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/bar/test/baz");
        assert!(joined.is_absolute());
        assert!(!joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar/")?;
        let sub = FsPath::new("test/baz/")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "bar", "test", "baz", "",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/bar/test/baz/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("C:\\")?;
        let sub = FsPath::new("test/baz/")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: Some(Prefix::Disk(b'C')),
                components: components(vec!["", "test", "baz", "",]),
            }
        );
        assert_eq!(joined.to_string(), "C:\\test\\baz\\");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(joined.is_windows());

        let base = FsPath::new("/")?;
        let sub = FsPath::new("test/baz/")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "test", "baz", "",]),
            }
        );
        assert_eq!(joined.to_string(), "/test/baz/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar")?;
        let sub = FsPath::new("../")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "..", "",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/../");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar")?;
        let sub = FsPath::new("..")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "..",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/..");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar/")?;
        let sub = FsPath::new("../baz")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "bar", "..", "baz",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/bar/../baz");
        assert!(joined.is_absolute());
        assert!(!joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar/")?;
        let sub = FsPath::new("./")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "bar", ".", "",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/bar/./");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar/")?;
        let sub = FsPath::new(".")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "bar", ".",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/bar/.");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar/")?;
        let sub = FsPath::new("./..")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "bar", ".", "..",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/bar/./..");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar")?;
        let sub = FsPath::new("./")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", ".", "",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/./");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar")?;
        let sub = FsPath::new("..")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "..",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/..");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar")?;
        let sub = FsPath::new("")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar")?;
        let sub = FsPath::new("baz")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "baz",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/baz");
        assert!(joined.is_absolute());
        assert!(!joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar/")?;
        let sub = FsPath::new("")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "bar", "",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/bar/");
        assert!(joined.is_absolute());
        assert!(joined.is_directory());
        assert!(!joined.is_windows());

        let base = FsPath::new("/foo/bar/")?;
        let sub = FsPath::new("baz")?;
        let joined = base.join(&sub)?;
        assert_eq!(
            joined,
            FsPath {
                prefix: None,
                components: components(vec!["", "foo", "bar", "baz",]),
            }
        );
        assert_eq!(joined.to_string(), "/foo/bar/baz");
        assert!(joined.is_absolute());
        assert!(!joined.is_directory());
        assert!(!joined.is_windows());

        Ok(())
    }

    #[test]
    fn test_path_relative() -> FsResult<()> {
        let base = FsPath::new("/foo/bar")?;
        let next = FsPath::new("/foo/baz")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["baz",]),
            }
        );
        assert_eq!(relative.to_string(), "baz");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/")?;
        let next = FsPath::new("/foo/baz")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "baz",]),
            }
        );
        assert_eq!(relative.to_string(), "../baz");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/")?;
        let next = FsPath::new("/foo/baz/")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "baz", "",]),
            }
        );
        assert_eq!(relative.to_string(), "../baz/");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar")?;
        let next = FsPath::new("/foo/baz/")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["baz", "",]),
            }
        );
        assert_eq!(relative.to_string(), "baz/");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar")?;
        let next = FsPath::new("/foo/bar")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["bar",]),
            }
        );
        assert_eq!(relative.to_string(), "bar");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/")?;
        let next = FsPath::new("/foo/bar/")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec![]),
            }
        );
        assert_eq!(relative.to_string(), "");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/")?;
        let next = FsPath::new("/foo/bar")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "bar",]),
            }
        );
        assert_eq!(relative.to_string(), "../bar");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar")?;
        let next = FsPath::new("/foo/bar/")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["bar", "",]),
            }
        );
        assert_eq!(relative.to_string(), "bar/");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/")?;
        let next = FsPath::new("/foo/")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..",]),
            }
        );
        assert_eq!(relative.to_string(), "..");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz")?;
        let next = FsPath::new("/foo/bar/bad/gah")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["bad", "gah",]),
            }
        );
        assert_eq!(relative.to_string(), "bad/gah");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz")?;
        let next = FsPath::new("/foo/bad/gah")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "bad", "gah",]),
            }
        );
        assert_eq!(relative.to_string(), "../bad/gah");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz")?;
        let next = FsPath::new("/foo/bar/baz")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["baz",]),
            }
        );
        assert_eq!(relative.to_string(), "baz");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz")?;
        let next = FsPath::new("/foo/bar")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "bar",]),
            }
        );
        assert_eq!(relative.to_string(), "../bar");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz")?;
        let next = FsPath::new("/foo/bar/")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec![]),
            }
        );
        assert_eq!(relative.to_string(), "");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz")?;
        let next = FsPath::new("/foo/bar/baz/")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["baz", "",]),
            }
        );
        assert_eq!(relative.to_string(), "baz/");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz")?;
        let next = FsPath::new("/foo/bar/baz/gad")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["baz", "gad",]),
            }
        );
        assert_eq!(relative.to_string(), "baz/gad");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/")?;
        let next = FsPath::new("/foo/bar/bad/gah")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "bad", "gah",]),
            }
        );
        assert_eq!(relative.to_string(), "../bad/gah");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/")?;
        let next = FsPath::new("/foo/bad/gah")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "..", "bad", "gah",]),
            }
        );
        assert_eq!(relative.to_string(), "../../bad/gah");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/")?;
        let next = FsPath::new("/foo/bar/baz")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "baz",]),
            }
        );
        assert_eq!(relative.to_string(), "../baz");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/")?;
        let next = FsPath::new("/foo/bar")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "..", "bar",]),
            }
        );
        assert_eq!(relative.to_string(), "../../bar");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/")?;
        let next = FsPath::new("/foo/bar/")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..",]),
            }
        );
        assert_eq!(relative.to_string(), "..");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/")?;
        let next = FsPath::new("/foo/bar/baz/")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec![]),
            }
        );
        assert_eq!(relative.to_string(), "");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/")?;
        let next = FsPath::new("/foo/bar/baz/gad")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["gad",]),
            }
        );
        assert_eq!(relative.to_string(), "gad");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/gah/ooh")?;
        let next = FsPath::new("/foo/bar/bad/gah")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "..", "bad", "gah",]),
            }
        );
        assert_eq!(relative.to_string(), "../../bad/gah");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/gah/ooh")?;
        let next = FsPath::new("/foo/bad/gah")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "..", "..", "bad", "gah",]),
            }
        );
        assert_eq!(relative.to_string(), "../../../bad/gah");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/gah/ooh")?;
        let next = FsPath::new("/foo/bar/baz")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "..", "baz",]),
            }
        );
        assert_eq!(relative.to_string(), "../../baz");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/gah/ooh")?;
        let next = FsPath::new("/foo/bar")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "..", "..", "bar",]),
            }
        );
        assert_eq!(relative.to_string(), "../../../bar");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/gah/ooh")?;
        let next = FsPath::new("/foo/bar/")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "..",]),
            }
        );
        assert_eq!(relative.to_string(), "../..");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/gah/ooh")?;
        let next = FsPath::new("/foo/bar/baz/")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..",]),
            }
        );
        assert_eq!(relative.to_string(), "..");
        assert!(!relative.is_absolute());
        assert!(relative.is_directory());
        assert!(!relative.is_windows());

        let base = FsPath::new("/foo/bar/baz/gah/ooh")?;
        let next = FsPath::new("/foo/bar/baz/gad")?;
        let relative = base.relative(&next)?;
        assert_eq!(
            relative,
            FsPath {
                prefix: None,
                components: components(vec!["..", "gad",]),
            }
        );
        assert_eq!(relative.to_string(), "../gad");
        assert!(!relative.is_absolute());
        assert!(!relative.is_directory());
        assert!(!relative.is_windows());

        Ok(())
    }
}
