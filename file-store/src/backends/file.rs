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

fn get_storage_error(error: io::Error, path: ObjectPath) -> StorageError {
    match error.kind() {
        io::ErrorKind::NotFound => error::not_found(path, Some(error)),
        _ => error::other_error(&format!("{}", path), Some(error)),
    }
}

fn wrap_future<F>(future: F, path: ObjectPath) -> impl Future<Output = StorageResult<F::Ok>>
where
    F: TryFutureExt<Error = io::Error>,
{
    future.map_err(move |e| get_storage_error(e, path))
}

fn wrap_stream<S>(stream: S, path: ObjectPath) -> impl Stream<Item = StorageResult<S::Ok>>
where
    S: TryStreamExt<Error = io::Error>,
{
    stream.map_err(move |e| get_storage_error(e, path.clone()))
}

fn get_object(path: ObjectPath, metadata: &Metadata) -> Object {
    let (object_type, size) = if metadata.is_file() {
        (ObjectType::File, metadata.len())
    } else if metadata.is_dir() {
        (ObjectType::Directory, 0)
    } else {
        (ObjectType::Unknown, 0)
    };

    Object {
        object_type,
        path,
        size,
    }
}

#[derive(Clone, Debug)]
struct FileSpace {
    base: PathBuf,
}

impl FileSpace {
    fn get_std_path(&self, path: &ObjectPath) -> StorageResult<PathBuf> {
        let mut result = self.base.clone();
        for part in path.parts() {
            result.push(part);
        }

        Ok(result)
    }
}

fn directory_stream(
    space: &FileSpace,
    path: ObjectPath,
) -> impl Stream<Item = StorageResult<(ObjectPath, Metadata)>> {
    #[allow(clippy::needless_lifetimes)]
    async fn build_base(
        space: &FileSpace,
        path: ObjectPath,
    ) -> StorageResult<impl Stream<Item = StorageResult<DirEntry>>> {
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
        path: ObjectPath,
    ) -> impl Stream<Item = StorageResult<(ObjectPath, Metadata)>> {
        let stream = match build_base(&space, path.clone()).await {
            Ok(s) => s,
            Err(e) => {
                return once(ready::<StorageResult<(ObjectPath, Metadata)>>(Err(e))).left_stream()
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
                                    return Err(error::invalid_data::<StorageError>(
                                        "Unable to convert OSString.",
                                        None,
                                    ))
                                }
                            };

                            path.push_part(&filename);
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

type FileList = StorageResult<(ObjectPath, Metadata)>;
struct FileLister {
    stream: Pin<Box<MergedStreams<FileList>>>,
    space: FileSpace,
}

impl FileLister {
    fn list(space: FileSpace, path: ObjectPath) -> FileLister {
        let mut lister = FileLister {
            stream: Box::pin(MergedStreams::new()),
            space,
        };

        lister.add_directory(path);
        lister
    }

    fn add_directory(&mut self, path: ObjectPath) {
        self.stream.push(directory_stream(&self.space, path));
    }
}

impl Stream for FileLister {
    type Item = StorageResult<Object>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> ResultStreamPoll<Object> {
        match self.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok((path, metadata)))) => {
                if metadata.is_dir() {
                    self.add_directory(path.clone());
                }

                Poll::Ready(Some(Ok(get_object(path, &metadata))))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct FileReadStream {
    path: ObjectPath,
    file: File,
}

impl FileReadStream {
    fn build(path: ObjectPath, file: File) -> DataStream {
        let stream = FileReadStream { path, file };

        DataStream::from_stream(stream.compat())
    }
}

impl OldStream for FileReadStream {
    type Item = Data;
    type Error = StorageError;

    fn poll(&mut self) -> StorageResult<Async<Option<Data>>> {
        let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);
        match self.file.read_buf(&mut buffer) {
            Ok(Async::Ready(0)) => Ok(Async::Ready(None)),
            Ok(Async::Ready(size)) => {
                buffer.split_off(size);
                Ok(Async::Ready(Some(buffer.freeze())))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(get_storage_error(e, self.path.clone())),
        }
    }
}

#[allow(clippy::needless_lifetimes)]
async fn delete_directory(space: FileSpace, path: ObjectPath) -> StorageResult<()> {
    let allfiles = FileLister::list(space.clone(), path.clone())
        .try_collect::<Vec<Object>>()
        .await?;
    let files = allfiles
        .iter()
        .filter(|file| file.object_type() != ObjectType::Directory);
    let directories = allfiles
        .iter()
        .filter(|file| file.object_type() == ObjectType::Directory);

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
            let metadata = wrap_future(
                symlink_metadata(target.clone()).compat(),
                ObjectPath::new("")?,
            )
            .await?;
            if !metadata.is_dir() {
                Err(error::invalid_settings::<StorageError>(
                    "Root path is not a directory.",
                    None,
                ))
            } else {
                Ok(FileStore {
                    backend: BackendImplementation::File(FileBackend {
                        space: FileSpace { base: target },
                    }),
                })
            }
        })
    }
}

impl TryFrom<FileStore> for FileBackend {
    type Error = StorageError;

    fn try_from(file_store: FileStore) -> StorageResult<FileBackend> {
        if let BackendImplementation::File(b) = file_store.backend {
            Ok(b)
        } else {
            Err(error::invalid_settings::<StorageError>(
                "FileStore does not hold a FileBackend",
                None,
            ))
        }
    }
}

impl StorageImpl for FileBackend {
    fn backend_type(&self) -> Backend {
        Backend::File
    }

    fn list_objects(&self, path: ObjectPath) -> ObjectStreamFuture {
        async fn list(space: FileSpace, path: ObjectPath) -> StorageResult<ObjectStream> {
            Ok(ObjectStream::from_stream(FileLister::list(space, path)))
        }

        ObjectStreamFuture::from_future(list(self.space.clone(), path))
    }

    fn get_object(&self, path: ObjectPath) -> ObjectFuture {
        async fn get(space: FileSpace, path: ObjectPath) -> StorageResult<Object> {
            let target = space.get_std_path(&path)?;
            let metadata = match symlink_metadata(target.clone()).compat().await {
                Ok(m) => m,
                Err(e) => return Err(get_storage_error(e, path)),
            };

            Ok(get_object(path, &metadata))
        }

        ObjectFuture::from_future(get(self.space.clone(), path))
    }

    fn get_file_stream(&self, path: ObjectPath) -> DataStreamFuture {
        async fn read(space: FileSpace, path: ObjectPath) -> StorageResult<DataStream> {
            let target = space.get_std_path(&path)?;

            let metadata =
                wrap_future(symlink_metadata(target.clone()).compat(), path.clone()).await?;
            if !metadata.is_file() {
                return Err(error::not_found::<StorageError>(path, None));
            }

            let file = wrap_future(File::open(target).compat(), path.clone()).await?;
            Ok(FileReadStream::build(path, file))
        }

        DataStreamFuture::from_future(read(self.space.clone(), path))
    }

    fn delete_object(&self, path: ObjectPath) -> OperationCompleteFuture {
        async fn delete(space: FileSpace, path: ObjectPath) -> StorageResult<()> {
            let target = space.get_std_path(&path)?;
            let metadata =
                wrap_future(symlink_metadata(target.clone()).compat(), path.clone()).await?;

            if !metadata.is_dir() {
                wrap_future(remove_file(target.clone()).compat(), path.clone()).await
            } else {
                delete_directory(space, path).await
            }
        }

        OperationCompleteFuture::from_future(delete(self.space.clone(), path))
    }

    fn write_file_from_stream(&self, path: ObjectPath, stream: DataStream) -> WriteCompleteFuture {
        async fn write(
            space: FileSpace,
            path: ObjectPath,
            mut stream: DataStream,
        ) -> Result<(), TransferError> {
            let target = space
                .get_std_path(&path)
                .map_err(TransferError::TargetError)?;
            match symlink_metadata(target.clone()).compat().await {
                Ok(m) => {
                    if m.is_dir() {
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
                        return Err(TransferError::TargetError(get_storage_error(e, path)));
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
                        Err(e) => Err(TransferError::TargetError(get_storage_error(
                            e,
                            path.clone(),
                        ))),
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
