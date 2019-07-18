//! Accesses files on the local filesystem. Included with the feature "file".
extern crate tokio_fs;

use std::fs::Metadata;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use ::futures::compat::*;
use ::futures::future::{ready, TryFutureExt, BoxFuture, FutureExt};
use ::futures::ready;
use ::futures::stream::{BoxStream, once, TryStreamExt, Stream};
use bytes::BytesMut;
use tokio_fs::{read_dir, remove_file, symlink_metadata, DirEntry, File};
use tokio::prelude::stream::Stream as TokioStream;
use tokio::prelude::Async as TokioAsync;
use tokio::io::AsyncRead as TokioAsyncRead;

use super::BackendImplementation;
use crate::futures::{stream_from_future, MergedStreams};
use crate::types::{Data, FsFile, FsPath};
use crate::*;

// How many bytes to attempt to read from a file at a time.
const BUFFER_SIZE: usize = 20 * 1024 * 1024;

fn get_fserror(error: io::Error, path: &FsPath) -> FsError {
    match error.kind() {
        io::ErrorKind::NotFound => FsError::not_found(path),
        _ => FsError::unknown(error),
    }
}

fn wrap_future<F>(future: F, path: &FsPath) -> impl Future<Output = Result<F::Ok, FsError>>
where
    F: TryFutureExt<Error = io::Error>,
{
    let wrapped = path.clone();
    future.map_err(move |e| get_fserror(e, &wrapped))
}

fn wrap_stream<S>(stream: S, path: &FsPath) -> impl Stream<Item = Result<S::Ok, FsError>>
where
    S: TryStreamExt<Error = io::Error>,
{
    let wrapped = path.clone();
    stream.map_err(move |e| get_fserror(e, &wrapped))
}

/*
struct FsPathInfo {
    space: FileSpace,
    target: PathBuf,
    future: SymlinkMetadataFuture<PathBuf>,
}

impl FsPathInfo {
    fn fetch(space: &FileSpace, target: &Path) -> FsPathInfo {
        FsPathInfo {
            space: space.clone(),
            target: target.to_owned(),
            future: symlink_metadata(target.to_owned()),
        }
    }
}

impl Future for FsPathInfo {
    type Item = (PathBuf, Option<Metadata>);
    type Error = FsError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.future.poll() {
            Ok(Async::Ready(m)) => Ok(Async::Ready((self.target.clone(), Some(m)))),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    Ok(Async::Ready((self.target.clone(), None)))
                } else {
                    Err(self.space.get_fserror(e, &self.target))
                }
            }
        }
    }
}*/

#[derive(Clone, Debug)]
struct FileSpace {
    base: FsPath,
}

impl FileSpace {
    fn get_std_path(&self, path: &FsPath) -> FsResult<PathBuf> {
        if !path.is_absolute() {
            return Err(FsError::parse_error(
                &format!("{}", path),
                "Target path is expected to be absolute.",
            ));
        }

        let relative = FsPath::new("/")?.relative(path)?;
        let target = self.base.join(&relative)?;

        Ok(target.as_std_path())
    }
}

fn directory_stream(path: &FsPath, space: FileSpace) -> impl Stream<Item = FsResult<(FsPath, Metadata)>> {
    async fn build_base(path: FsPath, space: FileSpace) -> FsResult<impl Stream<Item = FsResult<DirEntry>>> {
        let target = space.get_std_path(&path)?;
        Ok(wrap_stream(wrap_future(read_dir(target.clone()).compat(), &path).await?.compat(), &path))
    }

    async fn start_stream(path: FsPath, space: FileSpace) -> impl Stream<Item = FsResult<(FsPath, Metadata)>> {
        let stream = match build_base(path.clone(), space.clone()).await {
            Ok(s) => s,
            Err(e) => return once(ready::<FsResult<(FsPath, Metadata)>>(Err(e))).left_stream(),
        };

        stream.and_then(move |direntry| {
            let fname = direntry.file_name();
            let path = path.clone();
            wrap_future(symlink_metadata(direntry.path()).compat(), &path.clone())
                .map(move |result| {
                    match result {
                        Ok(metadata) => {
                            let filename = match fname.into_string() {
                                Ok(f) => f,
                                Err(_) => return Err(FsError::parse_error("", "Unable to convert OSString.")),
                            };

                            let mut found = path.clone();
                            if metadata.is_dir() {
                                found.push_dir(&filename);
                            } else {
                                found.set_filename(&filename);
                            }
                            Ok((found, metadata))
                        },
                        Err(e) => Err(e),
                    }
                })
        }).right_stream()
    }

    start_stream(path.clone(), space).flatten_stream()
}

struct FileLister {
    stream: Pin<Box<MergedStreams<FsResult<(FsPath, Metadata)>>>>,
    space: FileSpace,
}

impl FileLister {
    fn list(space: FileSpace, path: FsPath) -> FileLister {
        let mut lister = FileLister {
            stream: Box::pin(MergedStreams::new()),
            space,
        };

        lister.add_directory(path);
        lister
    }

    fn add_directory(&mut self, path: FsPath) {
        self.stream.push(directory_stream(&path, self.space.clone()));
    }
}

impl Stream for FileLister {
    type Item = FsResult<FsFile>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> FsStreamPoll<FsFile> {
        loop {
            match self.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok((path, metadata)))) => {
                    if metadata.is_file() {
                        return Poll::Ready(Some(Ok(FsFile {
                            path,
                            size: metadata.len(),
                        })));
                    } else if metadata.is_dir() {
                        self.add_directory(path);
                    }
                },
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

struct FileStream {
    path: FsPath,
    target: PathBuf,
    file: File,
}

impl FileStream {
    fn build(path: &FsPath, target: &Path, file: File) -> DataStream {
        let stream = FileStream {
            path: path.to_owned(),
            target: target.to_owned(),
            file,
        };

        DataStream::from_stream(stream.compat())
    }
}

impl TokioStream for FileStream {
    type Item = Bytes;
    type Error = FsError;

    fn poll(&mut self) -> Result<TokioAsync<Option<Bytes>>, FsError> {
        let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);
        match self.file.read_buf(&mut buffer) {
            Ok(TokioAsync::Ready(0)) => Ok(TokioAsync::Ready(None)),
            Ok(TokioAsync::Ready(size)) => {
                buffer.split_off(size);
                Ok(TokioAsync::Ready(Some(buffer.freeze())))
            }
            Ok(TokioAsync::NotReady) => Ok(TokioAsync::NotReady),
            Err(e) => Err(get_fserror(e, &self.path)),
        }
    }
}

/*
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
        loop {
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
                }
                Ok(Async::Ready(None)) => return Ok(Async::Ready(self.file.take().unwrap())),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(e) => return Err(FsError::from_error(e)),
            }
        }
    }
}
*/
/// The backend implementation for local file storage.
#[derive(Debug)]
pub struct FileBackend {
    space: FileSpace,
    settings: FsSettings,
}

impl FileBackend {
    /// Creates a new instance of the file backend.
    ///
    /// The authentication and address parts of the settings are ignored. The
    /// path is interpreted as a local and then used as the root of the
    /// filesystem accessed by the created [`Fs`](../struct.Fs.html).
    pub fn connect(settings: FsSettings) -> ConnectFuture {
        ConnectFuture::from_future(async {
            let space = FileSpace {
                base: settings.path.clone(),
            };
            let target = space.get_std_path(&FsPath::new("/")?)?;
            symlink_metadata(target).compat().map(|r| {
                match r {
                    Ok(meta) => {
                        if !meta.is_dir() {
                            Err(FsError::invalid_path(&settings.path, "Path setting was not a directory."))
                        } else {
                            Ok(Fs {
                                backend: BackendImplementation::File(FileBackend { space, settings }),
                            })
                        }
                    },
                    Err(e) => {
                        Err(get_fserror(e, &settings.path))
                    }
                }
            })
            .await
        })
    }
}

impl FsImpl for FileBackend {
    fn list_files(&self, path: FsPath) -> FileListFuture {
        async fn list(path: FsPath, space: FileSpace) -> FsResult<FileListStream> {
            Ok(FileListStream::from_stream(FileLister::list(
                space,
                path,
            )))
        }

        FileListFuture::from_future(list(path, self.space.clone()))
    }

    fn get_file(&self, path: FsPath) -> FileFuture {
        async fn get(path: FsPath, space: FileSpace) -> FsResult<FsFile> {
            let target = space.get_std_path(&path)?;
            let metadata = match symlink_metadata(target.clone()).compat().await {
                Ok(m) => m,
                Err(e) => return Err(get_fserror(e, &path)),
            };

            if metadata.is_file() {
                Ok(FsFile {
                    path,
                    size: metadata.len(),
                })
            } else {
                Err(FsError::not_found(&path))
            }
        }

        FileFuture::from_future(get(path, self.space.clone()))
    }

    fn get_file_stream(&self, path: FsPath) -> DataStreamFuture {
        async fn read(path: FsPath, space: FileSpace) -> FsResult<DataStream> {
            let target = space.get_std_path(&path)?;

            let metadata = wrap_future(symlink_metadata(target.clone()).compat(), &path).await?;
            if !metadata.is_file() {
                return Err(FsError::not_found(&path))
            }

            let file = wrap_future(File::open(target.clone()).compat(), &path).await?;
            Ok(FileStream::build(&path, &target, file))
        }

        DataStreamFuture::from_future(read(path, self.space.clone()))
    }

    fn delete_file(&self, path: FsPath) -> OperationCompleteFuture {
        async fn delete(path: FsPath, space: FileSpace) -> FsResult<()> {
            let target = space.get_std_path(&path)?;
            let metadata = match symlink_metadata(target.clone()).compat().await {
                Ok(m) => m,
                Err(e) => return Err(get_fserror(e, &path)),
            };

            if metadata.is_file() {
                match remove_file(target.clone()).compat().await {
                    Ok(()) => Ok(()),
                    Err(e) => Err(get_fserror(e, &path)),
                }
            } else {
                Err(FsError::not_found(&path))
            }
        }

        OperationCompleteFuture::from_future(delete(path, self.space.clone()))
    }

    fn write_from_stream(&self, path: FsPath, stream: DataStream) -> OperationCompleteFuture {
        unimplemented!();
        /*
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
        */
    }
}
