//! The generic [`FileStore`](../struct.FileStore.html).
use std::error::Error;

use bytes::buf::FromBuf;
use bytes::IntoBuf;
use futures::stream::{Stream, StreamExt};

use super::backends::{Backend, BackendImplementation, StorageImpl};
use super::types::stream::WrappedStream;
use super::types::*;

/// Provides access to a storage backend.
///
/// `FileStore`s provide the APIs to access files on one of the storage
/// backends. They are clonable to allow for capturing into closures etc.
///
/// Normally you would create a `FileStore` from one of the [backend implementations](backends/index.html).
#[derive(Clone, Debug)]
pub struct FileStore {
    pub(crate) backend: BackendImplementation,
}

impl FileStore {
    /// Retrieves the type of backend that this FileStore is using.
    pub fn backend_type(&self) -> Backend {
        call_backend!(&self.backend, backend_type)
    }

    /// Lists the objects that are prefixed by the given path.
    ///
    /// Because the majority of cloud storage systems do not really have a
    /// notion of directories and files, just file identifiers, this function
    /// will return any objects that have an identifier prefixed by `path`.
    pub fn list_objects(&self, path: ObjectPath) -> ObjectStreamFuture {
        call_backend!(&self.backend, list_objects, path)
    }

    /// Gets info about the object at the given path.
    ///
    /// This will return a [`NotFound`](enum.StorageErrorKind.html#variant.NotFound)
    /// error if no object exists at the fiven path.
    pub fn get_object(&self, path: ObjectPath) -> ObjectFuture {
        call_backend!(&self.backend, get_object, path)
    }

    /// Gets a stream of data for the file at the given path.
    ///
    /// The data returned is not necessarily in any particular chunk size.
    /// Dropping the stream at any point before completion should be considered
    /// to be safe.
    ///
    /// This will return a [`NotFound`](enum.StorageErrorKind.html#variant.NotFound)
    /// error if the object at the path does not exist or is not a file.
    pub fn get_file_stream(&self, path: ObjectPath) -> DataStreamFuture {
        call_backend!(&self.backend, get_file_stream, path)
    }

    /// Copies a file from one path to another within this `FileStore`.
    ///
    /// Normally this will be an efficient operation but in some cases it will
    /// require retrieving the entire file and then sending it to the new
    /// location.
    pub fn copy_file(&self, path: ObjectPath, target: ObjectPath) -> CopyCompleteFuture {
        call_backend!(&self.backend, copy_file, path, target)
    }

    /// Moves a file from one path to another within this `FileStore`.
    ///
    /// Normally this will be an efficient operation but in some cases it will
    /// require retrieving the entire file and then sending it to the new
    /// location.
    pub fn move_file(&self, path: ObjectPath, target: ObjectPath) -> MoveCompleteFuture {
        call_backend!(&self.backend, move_file, path, target)
    }

    /// Deletes the object at the given path.
    ///
    /// For backends that support physical directories if the object at tbe path
    /// is a directory then this will delete the directory and its contents.
    ///
    /// This will return a [`NotFound`](enum.StorageErrorKind.html#variant.NotFound)
    /// error if the object does not exist.
    pub fn delete_object(&self, path: ObjectPath) -> OperationCompleteFuture {
        call_backend!(&self.backend, delete_object, path)
    }

    /// Writes a stream of data to the file at the given path.
    ///
    /// Calling this will overwrite anything at the given path (notably on
    /// backends that support symlinks or directories those will be deleted
    /// along with their contents and replaced with a file). The rationale for
    /// this is that for network based backends not overwriting generally
    /// involves more API calls to check if something is there first. If you
    /// care about overwriting, call [`get_object`](struct.FileStore.html#method.get_file)
    /// first and check the result.
    ///
    /// If this operation fails there are no guarantees about the state of the
    /// file. If that is an issue then you should consider always calling
    /// [`delete_object`](struct.FileStore.html#method.delete_object) after a
    /// failure.
    ///
    /// The future returned will only resolve once all the data from the stream
    /// is succesfully written to storage. If the provided stream resolves to
    /// None at any point this will be considered the end of the data to be
    /// written.
    ///
    /// Any error emitted by the stream will cause this operation to fail.
    pub fn write_file_from_stream<S, I, E>(
        &self,
        path: ObjectPath,
        stream: S,
    ) -> WriteCompleteFuture
    where
        S: Stream<Item = Result<I, E>> + Send + 'static,
        I: IntoBuf + 'static,
        E: 'static + Error + Send + Sync,
    {
        let mapped = stream.map(|r| match r {
            Ok(b) => Ok(Data::from_buf(b)),
            Err(e) => Err(error::other_error(&format!("{}", e), Some(e))),
        });

        call_backend!(
            &self.backend,
            write_file_from_stream,
            path,
            WrappedStream::<Data>::from_stream(mapped)
        )
    }
}
