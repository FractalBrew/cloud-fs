use std::io;
use std::path::Path;
use http::uri::Uri;

use crate::Fs;
use crate::Error;
use crate::FsImplementation;
use crate::Metadata;
use crate::ReadDir;

pub struct FileFs {
}

impl FileFs {
  pub fn new(uri: Uri) -> Result<Fs, Error> {
    Ok(Fs {
      scheme: Box::new(FileFs {}),
    })
  }
}

impl FsImplementation for FileFs {
    fn metadata(&self, path: &Path) -> io::Result<Metadata> {
      Ok(Metadata {})
    }

    fn read_dir(&self, path: &Path) -> io::Result<ReadDir> {
      Ok(ReadDir {})
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
      Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
      Ok(())
    }
}
