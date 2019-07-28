//! Accesses files on the local filesystem. Included with the feature "file".
use std::convert::TryFrom;
use std::fs::Metadata;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures::compat::*;
use futures::future::{ready, Future, FutureExt, TryFutureExt};
use futures::stream::{once, Stream, StreamExt, TryStreamExt};
use old_futures::prelude::{Async, Stream as OldStream};
use tokio_fs::{read_dir, remove_dir, remove_file, symlink_metadata, DirEntry, File};
use tokio_io::io::write_all;
use tokio_io::AsyncRead as TokioAsyncRead;

use super::{Backend, BackendImplementation, StorageImpl};
use crate::filestore::FileStore;
use crate::types::error;
use crate::types::stream::{MergedStreams, ResultStreamPoll};
use crate::types::*;

// How many bytes to attempt to read from a file at a time.
const BUFFER_SIZE: usize = 20 * 1024 * 1024;

fn get_fserror(error: io::Error, path: StoragePath) -> io::Error {
    match error.kind() {
        io::ErrorKind::NotFound => error::not_found(path),
        _ => error,
    }
}

fn wrap_future<F>(future: F, path: StoragePath) -> impl Future<Output = io::Result<F::Ok>>
where
    F: TryFutureExt<Error = io::Error>,
{
    future.map_err(move |e| get_fserror(e, path))
}

fn wrap_stream<S>(stream: S, path: StoragePath) -> impl Stream<Item = io::Result<S::Ok>>
where
    S: TryStreamExt<Error = io::Error>,
{
    stream.map_err(move |e| get_fserror(e, path.clone()))
}

fn get_fsfile(mut path: StoragePath, metadata: &Metadata) -> Object {
    let (file_type, size) = if metadata.is_file() {
        (ObjectType::File, metadata.len())
    } else if metadata.is_dir() {
        (ObjectType::Directory, 0)
    } else {
        (ObjectType::Unknown, 0)
    };

    if metadata.is_dir() {
        path.make_dir();
    }

    Object {
        file_type,
        path,
        size,
    }
}

#[derive(Clone, Debug)]
struct FileSpace {
    base: StoragePath,
}

impl FileSpace {
    fn get_std_path(&self, path: &StoragePath) -> io::Result<PathBuf> {
        if !path.is_absolute() {
            return Err(error::invalid_path(
                path.clone(),
                "Target path is expected to be absolute.",
            ));
        }

        let relative = StoragePath::new("/")?.relative(path)?;
        let target = self.base.join(&relative)?;

        Ok(target.as_std_path())
    }
}

fn directory_stream(
    space: &FileSpace,
    path: StoragePath,
) -> impl Stream<Item = io::Result<(StoragePath, Metadata)>> {
    #[allow(clippy::needless_lifetimes)]
    async fn build_base(
        space: &FileSpace,
        path: StoragePath,
    ) -> io::Result<impl Stream<Item = io::Result<DirEntry>>> {
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
        path: StoragePath,
    ) -> impl Stream<Item = io::Result<(StoragePath, Metadata)>> {
        let stream = match build_base(&space, path.clone()).await {
            Ok(s) => s,
            Err(e) => {
                return once(ready::<io::Result<(StoragePath, Metadata)>>(Err(e))).left_stream()
            }
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
                                    return Err(error::invalid_data("Unable to convert OSString."))
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

type FileList = io::Result<(StoragePath, Metadata)>;
struct FileLister {
    stream: Pin<Box<MergedStreams<FileList>>>,
    space: FileSpace,
}

impl FileLister {
    fn list(space: FileSpace, path: StoragePath) -> FileLister {
        let mut lister = FileLister {
            stream: Box::pin(MergedStreams::new()),
            space,
        };

        lister.add_directory(path);
        lister
    }

    fn add_directory(&mut self, path: StoragePath) {
        self.stream.push(directory_stream(&self.space, path));
    }
}

impl Stream for FileLister {
    type Item = io::Result<Object>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> ResultStreamPoll<Object> {
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
    path: StoragePath,
    file: File,
}

impl FileReadStream {
    fn build(path: StoragePath, file: File) -> DataStream {
        let stream = FileReadStream { path, file };

        DataStream::from_stream(stream.compat())
    }
}

impl OldStream for FileReadStream {
    type Item = Data;
    type Error = io::Error;

    fn poll(&mut self) -> io::Result<Async<Option<Data>>> {
        let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);
        match self.file.read_buf(&mut buffer) {
            Ok(Async::Ready(0)) => Ok(Async::Ready(None)),
            Ok(Async::Ready(size)) => {
                buffer.split_off(size);
                Ok(Async::Ready(Some(buffer.freeze())))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(get_fserror(e, self.path.clone())),
        }
    }
}

#[allow(clippy::needless_lifetimes)]
async fn delete_directory(space: FileSpace, path: StoragePath) -> io::Result<()> {
    let allfiles = FileLister::list(space.clone(), path.clone())
        .try_collect::<Vec<Object>>()
        .await?;
    let files = allfiles
        .iter()
        .filter(|file| file.file_type() != ObjectType::Directory);
    let directories = allfiles
        .iter()
        .filter(|file| file.file_type() == ObjectType::Directory);

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

/// The backend implementation for local file storage. Only included when the
/// `file` feature is enabled.
#[derive(Clone, Debug)]
pub struct FileBackend {
    space: FileSpace,
}

impl FileBackend {
    /// Creates a new [`FileStore`](../struct.FileStore.html) instance using the
    /// file backend.
    ///
    /// The root path provided must be a directory and is used as the base of
    /// the visible storage.
    ///
    /// Directories and symlinks cannot be created but will be visible through
    /// `list_objects` and `get_object`. `delete_object` and `write_file_from_stream`
    /// will remove these (in the directory case recursively).
    pub fn connect(root: &Path) -> ConnectFuture {
        let target = root.to_owned();
        ConnectFuture::from_future(async move {
            let path = StoragePath::from_std_path(&target)?;

            let metadata =
                wrap_future(symlink_metadata(target.clone()).compat(), path.clone()).await?;
            if !metadata.is_dir() {
                Err(error::invalid_settings("Root path is not a directory."))
            } else {
                Ok(FileStore {
                    backend: BackendImplementation::File(FileBackend {
                        space: FileSpace { base: path },
                    }),
                })
            }
        })
    }
}

impl TryFrom<FileStore> for FileBackend {
    type Error = io::Error;

    fn try_from(file_store: FileStore) -> io::Result<FileBackend> {
        if let BackendImplementation::File(b) = file_store.backend {
            Ok(b)
        } else {
            Err(error::invalid_settings(
                "FileStore does not hold a FileBackend",
            ))
        }
    }
}

impl StorageImpl for FileBackend {
    fn backend_type(&self) -> Backend {
        Backend::File
    }

    fn list_objects(&self, path: StoragePath) -> ObjectStreamFuture {
        async fn list(space: FileSpace, path: StoragePath) -> io::Result<ObjectStream> {
            Ok(ObjectStream::from_stream(FileLister::list(space, path)))
        }

        ObjectStreamFuture::from_future(list(self.space.clone(), path))
    }

    fn get_object(&self, path: StoragePath) -> ObjectFuture {
        async fn get(space: FileSpace, path: StoragePath) -> io::Result<Object> {
            let target = space.get_std_path(&path)?;
            let metadata = match symlink_metadata(target.clone()).compat().await {
                Ok(m) => m,
                Err(e) => return Err(get_fserror(e, path)),
            };

            Ok(get_fsfile(path, &metadata))
        }

        ObjectFuture::from_future(get(self.space.clone(), path))
    }

    fn get_file_stream(&self, path: StoragePath) -> DataStreamFuture {
        async fn read(space: FileSpace, path: StoragePath) -> io::Result<DataStream> {
            let target = space.get_std_path(&path)?;

            let metadata =
                wrap_future(symlink_metadata(target.clone()).compat(), path.clone()).await?;
            if !metadata.is_file() {
                return Err(error::not_found(path));
            }

            let file = wrap_future(File::open(target).compat(), path.clone()).await?;
            Ok(FileReadStream::build(path, file))
        }

        DataStreamFuture::from_future(read(self.space.clone(), path))
    }

    fn delete_object(&self, path: StoragePath) -> OperationCompleteFuture {
        async fn delete(space: FileSpace, mut path: StoragePath) -> io::Result<()> {
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

    fn write_file_from_stream(&self, path: StoragePath, stream: DataStream) -> WriteCompleteFuture {
        async fn write(
            space: FileSpace,
            mut path: StoragePath,
            mut stream: DataStream,
        ) -> Result<(), TransferError> {
            let target = space
                .get_std_path(&path)
                .map_err(TransferError::TargetError)?;
            match symlink_metadata(target.clone()).compat().await {
                Ok(m) => {
                    if m.is_dir() {
                        path.make_dir();
                        delete_directory(space, path.clone())
                            .await
                            .map_err(TransferError::TargetError)?;
                    } else {
                        wrap_future(remove_file(target.clone()).compat(), path.clone())
                            .await
                            .map_err(TransferError::TargetError)?;
                    }
                }
                Err(e) => {
                    if e.kind() != io::ErrorKind::NotFound {
                        return Err(TransferError::TargetError(get_fserror(e, path)));
                    }
                }
            };

            let mut file = wrap_future(File::create(target).compat(), path.clone())
                .await
                .map_err(TransferError::TargetError)?;
            loop {
                let option = stream.next().await;
                if let Some(result) = option {
                    let data = result.map_err(TransferError::SourceError)?;
                    file = match write_all(file, data).compat().await {
                        Ok((f, _)) => Ok(f),
                        Err(e) => Err(TransferError::TargetError(e)),
                    }?;
                } else {
                    break;
                }
            }

            Ok(())
        }

        WriteCompleteFuture::from_future(write(self.space.clone(), path, stream))
    }
}
