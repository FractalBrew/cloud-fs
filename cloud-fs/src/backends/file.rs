//! Accesses files on the local filesystem. Included with the feature "file".
extern crate tokio_fs;

use std::fs::Metadata;
use std::io;
use std::path::{Path, PathBuf};

use bytes::BytesMut;
use tokio::fs::*;
use tokio::prelude::future::{err, Either};
use tokio_fs::DirEntry;

use super::BackendImplementation;
use crate::types::{FsFile, FsPath};
use crate::utils::{stream_from_future, MergedStreams};
use crate::*;

// How many bytes to attempt to read from a file at a time.
const BUFFER_SIZE: usize = 20 * 1024 * 1024;

#[derive(Clone, Debug)]
struct FileSpace {
    base: FsPath,
}

impl FileSpace {
    fn get_std_path(&self, path: &FsPath) -> FsResult<PathBuf> {
        if !path.is_absolute() {
            return Err(FsError::new(
                FsErrorKind::ParseError,
                "Target path is expected to be absolute.",
            ));
        }

        let relative = FsPath::new("/")?.relative(path)?;
        let target = self.base.join(&relative)?;

        Ok(target.as_std_path())
    }

    fn get_fs_path(&self, path: &Path) -> FsResult<FsPath> {
        if !path.is_absolute() {
            return Err(FsError::new(
                FsErrorKind::ParseError,
                "Target path is expected to be absolute.",
            ));
        }

        let target = FsPath::from_std_path(path)?;
        let relative = self.base.relative(&target)?;

        FsPath::new("/")?.join(&relative)
    }

    fn get_fserror(&self, error: io::Error, path: &Path) -> FsError {
        match self.get_fs_path(path) {
            Ok(target) => match error.kind() {
                io::ErrorKind::NotFound => {
                    FsError::new(FsErrorKind::NotFound, format!("{} was not found.", target))
                }
                _ => FsError::new(FsErrorKind::Other, format!("Failed to access {}.", target)),
            },
            Err(e) => e,
        }
    }

    fn get_fsfile(&self, path: &Path, metadata: Metadata) -> FsResult<FsFile> {
        Ok(FsFile {
            path: self.get_fs_path(path)?,
            size: metadata.len(),
        })
    }
}

struct FileLister {
    entries: Vec<DirEntry>,
    stream: MergedStreams<DirEntry, FsError>,
    stream_is_done: bool,
    space: FileSpace,
}

impl FileLister {
    fn list(space: FileSpace, path: PathBuf) -> FileLister {
        let mut lister = FileLister {
            entries: Vec::new(),
            stream: MergedStreams::new(),
            stream_is_done: false,
            space,
        };

        lister.add_directory(path);
        lister
    }

    fn add_directory(&mut self, path: PathBuf) {
        let space = self.space.clone();
        let error_path = path.clone();
        self.stream.push(
            stream_from_future(read_dir(path)).map_err(move |e| space.get_fserror(e, &error_path)),
        );
        self.stream_is_done = false;
    }

    fn add_dir_entry(&mut self, entry: DirEntry) {
        self.entries.push(entry);
    }

    fn poll_entries(&mut self) -> FsResult<Option<FsFile>> {
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
                        let file = self.space.get_fsfile(&entry.path(), metadata)?;
                        return Ok(Some(file));
                    }
                }
                Ok(Async::NotReady) => i += 1,
                Err(error) => {
                    let entry = self.entries.remove(i);
                    return Err(self.space.get_fserror(error, &entry.path()));
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
    type Item = FsFile;
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
    path: PathBuf,
    space: FileSpace,
    file: File,
}

impl FileStream {
    fn build(path: &Path, space: FileSpace, file: File) -> DataStream {
        DataStream::from_stream(FileStream {
            path: path.to_owned(),
            space,
            file,
        })
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
            Err(e) => Err(self.space.get_fserror(e, &self.path)),
        }
    }
}

/// The backend implementation for local file storage.
#[derive(Debug)]
pub struct FileBackend {
    space: FileSpace,
    settings: FsSettings,
}

impl FileBackend {
    /// Creates a new instance of the file backend.
    pub fn connect(settings: FsSettings) -> ConnectFuture {
        ConnectFuture::from_item(Fs {
            backend: BackendImplementation::File(FileBackend {
                space: FileSpace {
                    base: settings.path.clone(),
                },
                settings: settings.to_owned(),
            }),
        })
    }
}

impl FsImpl for FileBackend {
    fn list_files(&self, path: FsPath) -> FileListFuture {
        match self.space.get_std_path(&path) {
            Ok(target) => FileListFuture::from_item(FileListStream::from_stream(FileLister::list(
                self.space.clone(),
                target,
            ))),
            Err(error) => FileListFuture::from_error(error),
        }
    }

    fn get_file(&self, path: FsPath) -> FileFuture {
        match self.space.get_std_path(&path) {
            Ok(target) => {
                let space = self.space.clone();

                FileFuture::from_future(symlink_metadata(target.clone()).then(move |r| match r {
                    Ok(m) => {
                        if m.is_file() {
                            space.get_fsfile(&target, m)
                        } else {
                            Err(FsError::new(
                                FsErrorKind::NotFound,
                                format!("{} was not found.", path),
                            ))
                        }
                    }
                    Err(e) => Err(space.get_fserror(e, &target)),
                }))
            }
            Err(error) => FileFuture::from_error(error),
        }
    }

    fn get_file_stream(&self, path: FsPath) -> DataStreamFuture {
        match self.space.get_std_path(&path) {
            Ok(target) => {
                let space = self.space.clone();
                let build_space = self.space.clone();
                let build_target = target.clone();
                let meta_target = target.clone();
                DataStreamFuture::from_future(
                    symlink_metadata(target)
                        .then(move |r| match r {
                            Ok(m) => {
                                if m.is_file() {
                                    let error_target = meta_target.clone();
                                    Either::A(
                                        File::open(meta_target)
                                            .map_err(move |e| space.get_fserror(e, &error_target)),
                                    )
                                } else {
                                    Either::B(err(FsError::new(
                                        FsErrorKind::NotFound,
                                        format!("{} was not found.", path),
                                    )))
                                }
                            }
                            Err(e) => Either::B(err(space.get_fserror(e, &meta_target))),
                        })
                        .map(move |f| FileStream::build(&build_target, build_space, f)),
                )
            }
            Err(error) => DataStreamFuture::from_error(error),
        }
    }

    fn delete_file(&self, _path: FsPath) -> OperationCompleteFuture {
        OperationCompleteFuture::from_error(FsError::new(
            FsErrorKind::NotImplemented,
            "FileBackend::delete_file is not yet implemented.",
        ))
    }

    fn write_from_stream(&self, _path: FsPath, _stream: DataStream) -> OperationCompleteFuture {
        OperationCompleteFuture::from_error(FsError::new(
            FsErrorKind::NotImplemented,
            "FileBackend::write_from_stream is not yet implemented.",
        ))
    }
}
