extern crate tokio_fs;

use std::path::PathBuf;
use std::fs::Metadata;

use tokio::fs::*;
use tokio_fs::{DirEntry};

use super::BackendImplementation;
use crate::types::{FsPath, FsFile};
use crate::utils::{MergedStreams, stream_from_future};
use crate::*;

struct FileLister {
    base: FsPath,
    entries: Vec<DirEntry>,
    stream: MergedStreams<DirEntry, FsError>,
}

impl FileLister {
    fn list(base: FsPath, path: PathBuf) -> FileLister {
        let mut lister = FileLister {
            base,
            entries: Vec::new(),
            stream: MergedStreams::new(),
        };

        lister.add_directory(path);
        lister
    }

    fn add_directory(&mut self, path: PathBuf) {
        self.stream.push(stream_from_future(read_dir(path))
            .map_err(FsError::from_error));
    }

    fn add_dir_entry(&mut self, entry: DirEntry) {
        self.entries.push(entry);
    }

    fn into_file(&self, entry: &DirEntry, metadata: &Metadata) -> FsResult<FsFile> {
        Ok(FsFile {
            path: FsPath::from_std_path(&entry.path())?,
            size: Some(metadata.len()),
        })
    }

    fn poll_entries(&mut self) -> Result<Option<FsFile>, FsError> {
        let mut i = 0;
        while i < self.entries.len() {
            match self.entries[i].poll_metadata() {
                Ok(Async::Ready(metadata)) => {
                    let entry = self.entries.remove(i);

                    if metadata.is_dir() {
                        self.add_directory(entry.path().to_owned());
                    } else if metadata.is_file() {
                        return Ok(Some(self.into_file(&entry, &metadata)?));
                    }
                },
                Ok(Async::NotReady) => i += 1,
                Err(error) => {
                    self.entries.remove(i);
                    return Err(FsError::from_error(error));
                },
            }
        }

        Ok(None)
    }

    fn poll_stream(&mut self) -> Result<bool, FsError> {
        match self.stream.poll()? {
            Async::Ready(Some(entry)) => {
                self.add_dir_entry(entry);
                Ok(true)
            },
            Async::Ready(None) => Ok(false),
            Async::NotReady => Ok(true),
        }
    }
}

impl Stream for FileLister {
    type Item = FsFile;
    type Error = FsError;

    fn poll(&mut self) -> FsStreamPoll<Self::Item> {
        if !self.poll_stream()? && self.entries.is_empty() {
            Ok(Async::Ready(None))
        } else if let Some(file) = self.poll_entries()? {
            Ok(Async::Ready(Some(file)))
        } else {
            Ok(Async::NotReady)
        }
    }
}

/// Accesses files on the local filesystem. Included with the feature "file".
#[derive(Debug)]
pub struct FileBackend {
    settings: FsSettings,
}

impl FileBackend {
    pub fn connect(settings: FsSettings) -> ConnectFuture {
        ConnectFuture::from_item(Fs {
            backend: BackendImplementation::File(FileBackend {
                settings: settings.to_owned(),
            }),
        })
    }

    fn get_target(&self, path: &FsPath) -> FsResult<PathBuf> {
        // We know that the path is absolute and not windows-like here.
        let mut relative = path.clone();
        relative.components.remove(0);

        //let target = self.settings.path.join(&relative)?;
        Ok(PathBuf::from(path.to_string()))
    }
}

impl FsImpl for FileBackend {
    fn list_files(&self, path: &FsPath) -> FileListFuture {
        match self.get_target(path) {
            Ok(target) => FileListFuture::from_item(
                FileListStream::from_stream(
                    FileLister::list(self.settings.path.clone(), target)
                )
            ),
            Err(error) => FileListFuture::from_error(error),
        }
    }

    fn get_file(&self, path: &FsPath) -> FileFuture {
        let target = self.get_target(path);
        unimplemented!();
    }

    fn delete_file(&self, path: &FsPath) -> OperationCompleteFuture {
        let target = self.get_target(path);
        unimplemented!();
    }

    fn get_file_stream(&self, path: &FsPath) -> DataStreamFuture {
        let target = self.get_target(path);
        unimplemented!();
    }

    fn write_from_stream(&self, path: &FsPath, stream: DataStream) -> OperationCompleteFuture {
        let target = self.get_target(path);
        unimplemented!();
    }
}
