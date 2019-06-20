//! Accesses files on the local filesystem. Included with the feature "file".
extern crate tokio_fs;

use std::fs::Metadata;
use std::io;
use std::path::{Path, PathBuf};

use bytes::BytesMut;
use tokio::fs::*;
use tokio::io::{flush, write_all, WriteAll};
use tokio::prelude::future::{err, ok, Either};
use tokio_fs::{DirEntry, SymlinkMetadataFuture};

use super::BackendImplementation;
use crate::types::{Data, FsFile, FsPath};
use crate::utils::{stream_from_future, MergedStreams};
use crate::*;

// How many bytes to attempt to read from a file at a time.
const BUFFER_SIZE: usize = 20 * 1024 * 1024;

struct FsPathInfo {
    space: FileSpace,
    path: PathBuf,
    future: SymlinkMetadataFuture<PathBuf>,
}

impl FsPathInfo {
    fn fetch(space: &FileSpace, path: &Path) -> FsPathInfo {
        FsPathInfo {
            space: space.clone(),
            path: path.to_owned(),
            future: symlink_metadata(path.to_owned()),
        }
    }
}

impl Future for FsPathInfo {
    type Item = (PathBuf, Option<Metadata>);
    type Error = FsError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.future.poll() {
            Ok(Async::Ready(m)) => Ok(Async::Ready((self.path.clone(), Some(m)))),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    Ok(Async::Ready((self.path.clone(), None)))
                } else {
                    Err(self.space.get_fserror(e, &self.path))
                }
            }
        }
    }
}

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
                io::ErrorKind::NotFound => FsError::not_found(&target),
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

    fn not_found(&self, target: &Path) -> FsError {
        match self.get_fs_path(target) {
            Ok(path) => FsError::not_found(&path),
            Err(e) => e,
        }
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
        self.stream.push(
            stream_from_future(read_dir(path.clone()))
                .map_err(move |e| space.get_fserror(e, &path)),
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

struct FileWriter {
    file: Option<File>,
    stream: DataStream,
    write_future: Option<WriteAll<File, Data>>,
}

impl FileWriter {
    fn new(file: File, stream: DataStream) -> FileWriter {
        FileWriter {
            file: Some(file),
            stream,
            write_future: None,
        }
    }
}

impl Future for FileWriter {
    type Item = File;
    type Error = FsError;

    fn poll(&mut self) -> Poll<File, FsError> {
        if let Some(ref mut future) = self.write_future {
            match future.poll() {
                Ok(Async::Ready((file, _))) => {
                    self.file = Some(file);
                    self.write_future = None;
                }
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(e) => return Err(FsError::from_error(e)),
            }
        }

        match self.stream.poll() {
            Ok(Async::Ready(Some(data))) => {
                self.write_future = Some(write_all(self.file.take().unwrap(), data));
                self.poll()
            }
            Ok(Async::Ready(None)) => Ok(Async::Ready(self.file.take().unwrap())),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(FsError::from_error(e)),
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

                FileFuture::from_future(FsPathInfo::fetch(&self.space, &target).and_then(
                    move |(target, meta)| match meta {
                        Some(m) => {
                            if m.is_file() {
                                space.get_fsfile(&target, m)
                            } else {
                                Err(space.not_found(&target))
                            }
                        }
                        None => Err(space.not_found(&target)),
                    },
                ))
            }
            Err(error) => FileFuture::from_error(error),
        }
    }

    fn get_file_stream(&self, path: FsPath) -> DataStreamFuture {
        match self.space.get_std_path(&path) {
            Ok(target) => {
                let space = self.space.clone();
                let build_space = space.clone();

                DataStreamFuture::from_future(
                    FsPathInfo::fetch(&self.space, &target)
                        .and_then(move |(target, meta)| match meta {
                            Some(m) => {
                                if m.is_file() {
                                    Either::A(
                                        File::open(target.clone())
                                            .map_err(move |e| space.get_fserror(e, &target)),
                                    )
                                } else {
                                    Either::B(err(space.not_found(&target)))
                                }
                            }
                            None => Either::B(err(space.not_found(&target))),
                        })
                        .map(move |f| FileStream::build(&target, build_space, f)),
                )
            }
            Err(error) => DataStreamFuture::from_error(error),
        }
    }

    fn delete_file(&self, path: FsPath) -> OperationCompleteFuture {
        match self.space.get_std_path(&path) {
            Ok(target) => {
                println!("Deleting {}", target.display());
                let space = self.space.clone();

                OperationCompleteFuture::from_future(
                    FsPathInfo::fetch(&self.space, &target).and_then(move |(target, meta)| {
                        match meta {
                            Some(m) => {
                                if m.is_file() {
                                    Either::A(
                                        remove_file(target.clone())
                                            .map_err(move |e| space.get_fserror(e, &target)),
                                    )
                                } else {
                                    Either::B(err(space.not_found(&target)))
                                }
                            }
                            None => Either::B(err(space.not_found(&target))),
                        }
                    }),
                )
            }
            Err(error) => OperationCompleteFuture::from_error(error),
        }
    }

    fn write_from_stream(&self, path: FsPath, stream: DataStream) -> OperationCompleteFuture {
        match self.space.get_std_path(&path) {
            Ok(target) => {
                let space = self.space.clone();

                OperationCompleteFuture::from_future(
                    FsPathInfo::fetch(&self.space, &target)
                        .and_then(move |(target, meta)| match meta {
                            Some(m) => {
                                if m.is_file() {
                                    Either::A(ok(target))
                                } else if m.is_dir() {
                                    Either::B(remove_dir(target.clone()).then(move |r| match r {
                                        Ok(_) => Ok(target),
                                        Err(_) => Err(space.not_found(&target)),
                                    }))
                                } else {
                                    Either::A(err(space.not_found(&target)))
                                }
                            }
                            None => Either::A(ok(target)),
                        })
                        .and_then(|target| File::create(target).map_err(FsError::from_error))
                        .and_then(move |file| FileWriter::new(file, stream))
                        .and_then(|file| flush(file).map_err(FsError::from_error))
                        .map(|_| ()),
                )
            }
            Err(error) => OperationCompleteFuture::from_error(error),
        }
    }
}
