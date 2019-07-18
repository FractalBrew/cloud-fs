//! Accesses files on the local filesystem. Included with the feature "file".
extern crate tokio_fs;

use std::fs::Metadata;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use ::futures::compat::*;
use ::futures::future::{ready, FutureExt, TryFutureExt};
use ::futures::stream::{once, Stream, TryStreamExt};
use bytes::BytesMut;
use tokio::io::{write_all, AsyncRead as TokioAsyncRead};
use tokio::prelude::stream::Stream as TokioStream;
use tokio::prelude::Async as TokioAsync;
use tokio_fs::{read_dir, remove_dir, remove_file, symlink_metadata, DirEntry, File};

use super::BackendImplementation;
use crate::futures::MergedStreams;
use crate::types::{Data, FsFile, FsFileType, FsPath};
use crate::*;

// How many bytes to attempt to read from a file at a time.
const BUFFER_SIZE: usize = 20 * 1024 * 1024;

fn get_fserror(error: io::Error, path: FsPath) -> FsError {
    match error.kind() {
        io::ErrorKind::NotFound => FsError::not_found(path),
        _ => FsError::unknown(error),
    }
}

fn wrap_future<F>(future: F, path: FsPath) -> impl Future<Output = Result<F::Ok, FsError>>
where
    F: TryFutureExt<Error = io::Error>,
{
    future.map_err(move |e| get_fserror(e, path))
}

fn wrap_stream<S>(stream: S, path: FsPath) -> impl Stream<Item = Result<S::Ok, FsError>>
where
    S: TryStreamExt<Error = io::Error>,
{
    stream.map_err(move |e| get_fserror(e, path.clone()))
}

fn get_fsfile(mut path: FsPath, metadata: &Metadata) -> FsFile {
    let (file_type, size) = if metadata.is_file() {
        (FsFileType::File, metadata.len())
    } else if metadata.is_dir() {
        (FsFileType::Directory, 0)
    } else {
        (FsFileType::Unknown, 0)
    };

    if metadata.is_dir() {
        path.make_dir();
    }

    FsFile {
        file_type,
        path,
        size,
    }
}

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

fn directory_stream(
    space: &FileSpace,
    path: FsPath,
) -> impl Stream<Item = FsResult<(FsPath, Metadata)>> {
    #[allow(clippy::needless_lifetimes)]
    async fn build_base(
        space: &FileSpace,
        path: FsPath,
    ) -> FsResult<impl Stream<Item = FsResult<DirEntry>>> {
        let target = space.get_std_path(&path)?;
        Ok(wrap_stream(
            wrap_future(read_dir(target.clone()).compat(), path.clone())
                .await?
                .compat(),
            path,
        ))
    }

    async fn start_stream(
        space: FileSpace,
        path: FsPath,
    ) -> impl Stream<Item = FsResult<(FsPath, Metadata)>> {
        let stream = match build_base(&space, path.clone()).await {
            Ok(s) => s,
            Err(e) => return once(ready::<FsResult<(FsPath, Metadata)>>(Err(e))).left_stream(),
        };

        stream
            .and_then(move |direntry| {
                let fname = direntry.file_name();
                let mut path = path.clone();
                wrap_future(symlink_metadata(direntry.path()).compat(), path.clone()).map(
                    move |result| match result {
                        Ok(metadata) => {
                            let filename = match fname.into_string() {
                                Ok(f) => f,
                                Err(_) => {
                                    return Err(FsError::parse_error(
                                        "",
                                        "Unable to convert OSString.",
                                    ))
                                }
                            };

                            if metadata.is_dir() {
                                path.push_dir(&filename);
                            } else {
                                path.set_filename(&filename);
                            }
                            Ok((path, metadata))
                        }
                        Err(e) => Err(e),
                    },
                )
            })
            .right_stream()
    }

    start_stream(space.clone(), path).flatten_stream()
}

type FileList = FsResult<(FsPath, Metadata)>;
struct FileLister {
    stream: Pin<Box<MergedStreams<FileList>>>,
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
        self.stream.push(directory_stream(&self.space, path));
    }
}

impl Stream for FileLister {
    type Item = FsResult<FsFile>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> FsStreamPoll<FsFile> {
        match self.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok((path, metadata)))) => {
                if metadata.is_dir() {
                    self.add_directory(path.clone());
                }

                Poll::Ready(Some(Ok(get_fsfile(path, &metadata))))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct FileReadStream {
    path: FsPath,
    file: File,
}

impl FileReadStream {
    fn build(path: FsPath, file: File) -> DataStream {
        let stream = FileReadStream { path, file };

        DataStream::from_stream(stream.compat())
    }
}

impl TokioStream for FileReadStream {
    type Item = Data;
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
            Err(e) => Err(get_fserror(e, self.path.clone())),
        }
    }
}

#[allow(clippy::needless_lifetimes)]
async fn delete_directory(space: FileSpace, path: FsPath) -> FsResult<()> {
    let allfiles = FileLister::list(space.clone(), path.clone())
        .try_collect::<Vec<FsFile>>()
        .await?;
    let files = allfiles
        .iter()
        .filter(|file| file.file_type() != FsFileType::Directory);
    let directories = allfiles
        .iter()
        .filter(|file| file.file_type() == FsFileType::Directory);

    for file in files {
        let target = space.get_std_path(&file.path())?;
        wrap_future(remove_file(target).compat(), file.path()).await?;
    }

    for dir in directories {
        let target = space.get_std_path(&dir.path())?;
        wrap_future(remove_dir(target).compat(), dir.path()).await?;
    }

    let target = space.get_std_path(&path)?;
    wrap_future(remove_dir(target).compat(), path).await
}

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
            symlink_metadata(target)
                .compat()
                .map(|r| match r {
                    Ok(meta) => {
                        if !meta.is_dir() {
                            Err(FsError::invalid_path(
                                settings.path,
                                "Path setting was not a directory.",
                            ))
                        } else {
                            Ok(Fs {
                                backend: BackendImplementation::File(FileBackend {
                                    space,
                                    settings,
                                }),
                            })
                        }
                    }
                    Err(e) => Err(get_fserror(e, settings.path)),
                })
                .await
        })
    }
}

impl FsImpl for FileBackend {
    fn list_files(&self, path: FsPath) -> FileListFuture {
        async fn list(space: FileSpace, path: FsPath) -> FsResult<FileListStream> {
            Ok(FileListStream::from_stream(FileLister::list(space, path)))
        }

        FileListFuture::from_future(list(self.space.clone(), path))
    }

    fn get_file(&self, path: FsPath) -> FileFuture {
        async fn get(space: FileSpace, path: FsPath) -> FsResult<FsFile> {
            let target = space.get_std_path(&path)?;
            let metadata = match symlink_metadata(target.clone()).compat().await {
                Ok(m) => m,
                Err(e) => return Err(get_fserror(e, path)),
            };

            Ok(get_fsfile(path, &metadata))
        }

        FileFuture::from_future(get(self.space.clone(), path))
    }

    fn get_file_stream(&self, path: FsPath) -> DataStreamFuture {
        async fn read(space: FileSpace, path: FsPath) -> FsResult<DataStream> {
            let target = space.get_std_path(&path)?;

            let metadata =
                wrap_future(symlink_metadata(target.clone()).compat(), path.clone()).await?;
            if !metadata.is_file() {
                return Err(FsError::not_found(path));
            }

            let file = wrap_future(File::open(target).compat(), path.clone()).await?;
            Ok(FileReadStream::build(path, file))
        }

        DataStreamFuture::from_future(read(self.space.clone(), path))
    }

    fn delete_file(&self, path: FsPath) -> OperationCompleteFuture {
        async fn delete(space: FileSpace, mut path: FsPath) -> FsResult<()> {
            let target = space.get_std_path(&path)?;
            let metadata =
                wrap_future(symlink_metadata(target.clone()).compat(), path.clone()).await?;

            if !metadata.is_dir() {
                wrap_future(remove_file(target.clone()).compat(), path.clone()).await
            } else {
                path.make_dir();
                delete_directory(space, path).await
            }
        }

        OperationCompleteFuture::from_future(delete(self.space.clone(), path))
    }

    fn write_from_stream(
        &self,
        path: FsPath,
        stream: StreamHolder<Result<Data, io::Error>>,
    ) -> OperationCompleteFuture {
        async fn write(
            space: FileSpace,
            mut path: FsPath,
            stream: StreamHolder<Result<Data, io::Error>>,
        ) -> FsResult<()> {
            let target = space.get_std_path(&path)?;
            match symlink_metadata(target.clone()).compat().await {
                Ok(m) => {
                    if m.is_dir() {
                        path.make_dir();
                        delete_directory(space, path.clone()).await?;
                    } else {
                        wrap_future(remove_file(target.clone()).compat(), path.clone()).await?;
                    }
                }
                Err(e) => {
                    if e.kind() != io::ErrorKind::NotFound {
                        return Err(get_fserror(e, path));
                    }
                }
            };

            let file = wrap_future(File::create(target).compat(), path.clone()).await?;
            wrap_future(
                stream.try_fold(file, |file, data| {
                    write_all(file, data).compat().map_ok(|(file, _data)| file)
                }),
                path,
            )
            .await?;

            Ok(())
        }

        OperationCompleteFuture::from_future(write(self.space.clone(), path, stream))
    }
}
