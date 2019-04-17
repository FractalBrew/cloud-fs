use std::io;
use std::fs;
use std::path::{PathBuf};
use url::Url;

use crate::{ Fs, Error, FsImplementation, Metadata, ReadDir, FileKind };

fn metadata_from_fs(metadata: fs::Metadata) -> Metadata {
    let mut kind = FileKind::Symlink;
    if metadata.file_type().is_dir() {
        kind = FileKind::Directory;
    } else if metadata.file_type().is_file() {
        kind = FileKind::File;
    }

    Metadata {
        kind,
        length: metadata.len(),
    }
}

pub struct FileFs {
    root: Url,
}

impl FileFs {
    pub fn connect(url: Url) -> Result<Fs, io::Error> {
        let fs = FileFs {
            root: url,
        };

        Ok(Fs {
            scheme: Box::new(fs),
        })
    }

    fn get_path(&self, url: &Url) -> io::Result<PathBuf> {
        match url.to_file_path() {
            Ok(path) => Ok(path),
            _ => Err(io::Error::new(io::ErrorKind::InvalidInput, Error::new(&format!("Unable to convert '{}' to a file path.", url.to_string())))),
        }
    }
}

impl FsImplementation for FileFs {
    fn get_root(&self) -> &Url {
        &self.root
    }

    fn metadata(&self, url: Url) -> io::Result<Metadata> {
        Ok(metadata_from_fs(fs::metadata(self.get_path(&url)?)?))
    }

    fn symlink_metadata(&self, url: Url) -> io::Result<Metadata> {
        Ok(metadata_from_fs(fs::symlink_metadata(self.get_path(&url)?)?))
    }

    fn read_dir(&self, url: Url) -> io::Result<ReadDir> {
        Ok(ReadDir {})
    }

    fn remove_dir_all(&self, url: Url) -> io::Result<()> {
        fs::remove_dir_all(self.get_path(&url)?)
    }

    fn remove_file(&self, url: Url) -> io::Result<()> {
        fs::remove_file(self.get_path(&url)?)
    }
}
