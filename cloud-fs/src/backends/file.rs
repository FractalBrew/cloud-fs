//! Accesses files on the local filesystem. Included with the feature "file".
extern crate tokio_fs;

use std::fs::Metadata;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use bytes::BytesMut;
use tokio::fs::*;
use tokio_fs::DirEntry;

use super::BackendImplementation;
use crate::types::{FsFile, FsPath};
use crate::utils::{stream_from_future, MergedStreams};
use crate::*;

// How many bytes to attempt to read from a file at a time.
const BUFFER_SIZE: usize = 20 * 1024 * 1024;

struct FileLister {
    entries: Vec<DirEntry>,
    stream: MergedStreams<DirEntry, FsError>,
    stream_is_done: bool,
}

impl FileLister {
    fn list(path: PathBuf) -> FileLister {
        let mut lister = FileLister {
            entries: Vec::new(),
            stream: MergedStreams::new(),
            stream_is_done: false,
        };

        lister.add_directory(path);
        lister
    }

    fn add_directory(&mut self, path: PathBuf) {
        self.stream
            .push(stream_from_future(read_dir(path)).map_err(FsError::from_error));
        self.stream_is_done = false;
    }

    fn add_dir_entry(&mut self, entry: DirEntry) {
        self.entries.push(entry);
    }

    fn poll_entries(&mut self) -> FsResult<Option<(DirEntry, Metadata)>> {
        if self.entries.is_empty() {
            return Ok(None);
        }

        let mut i = 0;
        while i < self.entries.len() {
            match self.entries[i].poll_metadata() {
                Ok(Async::Ready(metadata)) => {
                    let entry = self.entries.remove(i);

                    if metadata.is_dir() {
                        self.add_directory(entry.path().to_owned());
                    } else if metadata.is_file() {
                        return Ok(Some((entry, metadata)));
                    }
                }
                Ok(Async::NotReady) => i += 1,
                Err(error) => {
                    self.entries.remove(i);
                    return Err(FsError::from_error(error));
                }
            }
        }

        Ok(None)
    }

    fn poll_stream(&mut self) -> FsResult<()> {
        if self.stream_is_done {
            return Ok(());
        }

        loop {
            match self.stream.poll() {
                Ok(Async::Ready(Some(entry))) => {
                    self.add_dir_entry(entry);
                }
                Ok(Async::Ready(None)) => {
                    self.stream_is_done = true;
                    break;
                }
                Ok(Async::NotReady) => {
                    break;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}

impl Stream for FileLister {
    type Item = (DirEntry, Metadata);
    type Error = FsError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Load up as many entries as we can.
        self.poll_stream()?;

        // Find one that is ready
        let poll_result = self.poll_entries()?;
        if poll_result.is_some() {
            return Ok(Async::Ready(poll_result));
        }

        // Are we done?
        if self.stream_is_done && self.entries.is_empty() {
            Ok(Async::Ready(None))
        } else {
            Ok(Async::NotReady)
        }
    }
}

struct FileStream {
    file: File,
}

impl FileStream {
    fn build(file: File) -> DataStream {
        DataStream::from_stream(FileStream { file })
    }
}

impl Stream for FileStream {
    type Item = Bytes;
    type Error = FsError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);
        match self.file.read_buf(&mut buffer) {
            Ok(Async::Ready(0)) => Ok(Async::Ready(None)),
            Ok(Async::Ready(size)) => {
                buffer.split_off(size);
                Ok(Async::Ready(Some(buffer.freeze())))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(FsError::from_error(e)),
        }
    }
}

/// The backend implementation for local file storage.
#[derive(Debug)]
pub struct FileBackend {
    settings: FsSettings,
}

impl FileBackend {
    /// Creates a new instance of the file backend.
    pub fn connect(settings: FsSettings) -> ConnectFuture {
        ConnectFuture::from_item(Fs {
            backend: BackendImplementation::File(FileBackend {
                settings: settings.to_owned(),
            }),
        })
    }

    fn fs_path_into_real_path(base: &FsPath, path: &FsPath) -> FsResult<PathBuf> {
        // We know that the path is absolute and not windows-like here.
        let mut relative = path.clone();
        relative.is_absolute = false;
        let target = base.join(&relative)?;

        Ok(target.as_std_path())
    }

    fn real_path_into_fs_path(base: &FsPath, path: &Path) -> FsResult<FsPath> {
        let fspath = FsPath::from_std_path(path)?;

        let mut relative = base.relative(&fspath)?;
        if relative.is_above_base() {
            return Err(FsError::new(
                FsErrorKind::InvalidPath,
                "Received an invalid path from the filesystem.",
            ));
        }

        relative.is_absolute = true;
        Ok(relative)
    }

    fn get_fsfile(base: &FsPath, path: PathBuf, metadata: Metadata) -> FsResult<FsFile> {
        Ok(FsFile {
            path: FileBackend::real_path_into_fs_path(base, &path)?,
            size: metadata.len(),
        })
    }

    fn get_api_target(&self, path: &FsPath) -> FsResult<PathBuf> {
        FileBackend::fs_path_into_real_path(&self.settings.path, path)
    }
}

impl FsImpl for FileBackend {
    fn list_files(&self, path: FsPath) -> FileListFuture {
        match self.get_api_target(&path) {
            Ok(target) => {
                let base = self.settings.path.clone();

                let file_list = FileLister::list(target).then(move |result| {
                    result.and_then(|(entry, metadata)| {
                        FileBackend::get_fsfile(&base, entry.path(), metadata)
                    })
                });
                FileListFuture::from_item(FileListStream::from_stream(file_list))
            }
            Err(error) => FileListFuture::from_error(error),
        }
    }

    fn get_file(&self, path: FsPath) -> FileFuture {
        match self.get_api_target(&path) {
            Ok(target) => {
                let base = self.settings.path.clone();

                FileFuture::from_future(metadata(target.clone()).then(move |r| match r {
                    Ok(m) => {
                        if m.is_file() {
                            FileBackend::get_fsfile(&base, target, m)
                        } else {
                            Err(FsError::new(
                                FsErrorKind::NotFound,
                                format!("{} was not found.", path),
                            ))
                        }
                    }
                    Err(e) => {
                        if e.kind() == ErrorKind::NotFound {
                            Err(FsError::new(
                                FsErrorKind::NotFound,
                                format!("{} was not found.", path),
                            ))
                        } else {
                            Err(FsError::from_error(e))
                        }
                    }
                }))
            }
            Err(error) => FileFuture::from_error(error),
        }
    }

    fn get_file_stream(&self, path: FsPath) -> DataStreamFuture {
        match self.get_api_target(&path) {
            Ok(target) => DataStreamFuture::from_future(
                File::open(target)
                    .map_err(FsError::from_error)
                    .map(FileStream::build),
            ),
            Err(error) => DataStreamFuture::from_error(error),
        }
    }

    fn delete_file(&self, path: FsPath) -> OperationCompleteFuture {
        let _target = self.get_api_target(&path);
        OperationCompleteFuture::from_error(FsError::new(
            FsErrorKind::NotImplemented,
            "FileBackend::delete_file is not yet implemented.",
        ))
    }

    fn write_from_stream(&self, path: FsPath, _stream: DataStream) -> OperationCompleteFuture {
        let _target = self.get_api_target(&path);
        OperationCompleteFuture::from_error(FsError::new(
            FsErrorKind::NotImplemented,
            "FileBackend::write_from_stream is not yet implemented.",
        ))
    }
}
