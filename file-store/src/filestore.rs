//! The generic [`FileStore`](../struct.FileStore.html).
use std::convert::TryInto;
use std::error::Error;

use bytes::IntoBuf;
use futures::stream::Stream;

use super::backends::{Backend, BackendImplementation, StorageBackend};
use super::types::*;

/// Provides access to a storage backend.
///
/// `FileStore` exposes all of the functionality guaranteed to be implemented by
/// every backend in a backend-agnostic manner (i.e. no generics). This is the
/// type you should use if you want your code to be able to use any backend.
///
/// You create a `FileStore` from one of the [backend implementations](backends/index.html).
#[derive(Clone, Debug)]
pub struct FileStore {
    pub(crate) backend: BackendImplementation,
}

impl FileStore {
    /// Retrieves the type of this backend.
    pub fn backend_type(&self) -> Backend {
        call_backend!(&self.backend, backend_type)
    }

    /// Lists the objects that are prefixed by the given prefix.
    ///
    /// This will return the entire directory structure under the given prefix.
    /// Be sure to include a trailing `/` if you only want to include objects
    /// inside that (possibly virtual) directory. This will only include
    /// directory objects if those actually exists in the underlying storage.
    pub fn list_objects<P>(&self, prefix: P) -> ObjectStreamFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        call_backend!(&self.backend, list_objects, prefix)
    }

    /// Lists the objects that exist in the given (possibly virtual) directory.
    ///
    /// Given a path (ending with a `/` character is optional), all objects
    /// that have a name beginning with the directory and not including any
    /// additional `/` character are returned. This will include directory
    /// objects even if the underlying storage doesn't actually support
    /// directories to indicate that there are deeper objects not included.
    pub fn list_directory<P>(&self, dir: P) -> ObjectStreamFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        call_backend!(&self.backend, list_directory, dir)
    }

    /// Gets info about the object at the given path.
    ///
    /// This will return a [`NotFound`](enum.StorageErrorKind.html#variant.NotFound)
    /// error if no object exists at the fiven path.
    pub fn get_object<P>(&self, path: P) -> ObjectFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
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
    pub fn get_file_stream<O>(&self, reference: O) -> DataStreamFuture
    where
        O: ObjectReference,
    {
        call_backend!(&self.backend, get_file_stream, reference)
    }

    /// Copies a file from one path to another within this `Backend`.
    ///
    /// Normally this will be an efficient operation but in some cases it will
    /// require retrieving the entire file and then sending it to the new
    /// location.
    pub fn copy_file<O, P>(&self, reference: O, target: P) -> CopyCompleteFuture
    where
        O: ObjectReference,
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        call_backend!(&self.backend, copy_file, reference, target)
    }

    /// Moves a file from one path to another within this `Backend`.
    ///
    /// Normally this will be an efficient operation but in some cases it will
    /// require retrieving the entire file and then sending it to the new
    /// location.
    pub fn move_file<O, P>(&self, reference: O, target: P) -> MoveCompleteFuture
    where
        O: ObjectReference,
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        call_backend!(&self.backend, move_file, reference, target)
    }

    /// Deletes the object at the given path.
    ///
    /// For backends that support physical directories if the object at tbe path
    /// is a directory then this will delete the directory and its contents.
    ///
    /// This will return a [`NotFound`](enum.StorageErrorKind.html#variant.NotFound)
    /// error if the object does not exist.
    pub fn delete_object<O>(&self, reference: O) -> OperationCompleteFuture
    where
        O: ObjectReference,
    {
        call_backend!(&self.backend, delete_object, reference)
    }

    /// Writes a stream of data to the file at the given path.
    ///
    /// Calling this will overwrite anything at the given path (notably on
    /// backends that support symlinks or directories those will be deleted
    /// along with their contents and replaced with a file). The rationale for
    /// this is that for network based backends not overwriting generally
    /// involves more API calls to check if something is there first. If you
    /// care about overwriting, call [`get_object`](struct.StorageBackend.html#method.get_file)
    /// first and check the result.
    ///
    /// If this operation fails there are no guarantees about the state of the
    /// file. If that is an issue then you should consider always calling
    /// [`delete_object`](struct.StorageBackend.html#method.delete_object) after a
    /// failure.
    ///
    /// The future returned will only resolve once all the data from the stream
    /// is succesfully written to storage. If the provided stream resolves to
    /// None at any point this will be considered the end of the data to be
    /// written.
    ///
    /// Any error emitted by the stream will cause this operation to fail.
    pub fn write_file_from_stream<S, I, E, P>(&self, path: P, stream: S) -> WriteCompleteFuture
    where
        S: Stream<Item = Result<I, E>> + Send + 'static,
        I: IntoBuf + 'static,
        E: 'static + Error + Send + Sync,
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        call_backend!(&self.backend, write_file_from_stream, path, stream)
    }
}
