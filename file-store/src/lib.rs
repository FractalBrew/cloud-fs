//! An asynchronous API for accessing files that could be on any of a number of
//! different storage backends.
//!
//! The API offers functions for listing, reading, writing and deleting files
//! from a storage backend. Each backend offers the same API plus in some cases
//! additional backend specific functionality.
//!
//! Obviously offering the same API across all backends means the API is fairly
//! basic, but if all you want to do is write, read or list files it should be
//! plenty. The API aims to keep you using functionality available in all
//! backends by returning types that only expose the standard functionality. You
//! can however easily get to the underlying backend implementations in order to
//! access backend-specific functionality if needed.
//!
//! Which backend is available depends on the features that file-store is
//! compiled with. See the [`backends`](backends/index.html) module.
//!
//! The [`FileStore`](enum.FileStore.html) is the main way to access storage. A
//! [`FileStore`](enum.FileStore.html) is created from one of the backends.
#![warn(missing_docs)]

#[macro_use]
pub mod backends;
mod types;
pub mod utils;

pub use types::*;

use std::convert::TryInto;

use bytes::IntoBuf;
use enum_dispatch::enum_dispatch;
use futures::future::TryFutureExt;
use futures::stream::Stream;

use backends::b2::B2Backend;
use backends::file::FileBackend;

/// The trait that every storage backend must implement at a minimum.
#[enum_dispatch]
pub trait StorageBackend: Clone + Send + 'static {
    /// Retrieves the type of this backend.
    fn backend_type(&self) -> backends::Backend;

    /// Lists the objects that are prefixed by the given prefix.
    ///
    /// This will return the entire directory structure under the given prefix.
    /// Be sure to include a trailing `/` if you only want to include objects
    /// inside that (possibly virtual) directory. This will only include
    /// directory objects if those actually exists in the underlying storage.
    fn list_objects<P>(&self, prefix: P) -> ObjectStreamFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>;

    /// Lists the objects that exist in the given (possibly virtual) directory.
    ///
    /// Given a path (ending with a `/` character is optional), all objects
    /// that have a name beginning with the directory and not including any
    /// additional `/` character are returned. This will include directory
    /// objects even if the underlying storage doesn't actually support
    /// directories to indicate that there may be deeper objects not included.
    fn list_directory<P>(&self, dir: P) -> ObjectStreamFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>;

    /// Gets info about the object at the given path.
    ///
    /// This will return a [`NotFound`](enum.StorageErrorKind.html#variant.NotFound)
    /// error if no object exists at the fiven path.
    fn get_object<P>(&self, path: P) -> ObjectFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>;

    /// Gets a stream of data for the file at the given path.
    ///
    /// The data returned is not necessarily in any particular chunk size.
    /// Dropping the stream at any point before completion should be considered
    /// to be safe.
    ///
    /// This will return a [`NotFound`](enum.StorageErrorKind.html#variant.NotFound)
    /// error if the object at the path does not exist or is not a file.
    fn get_file_stream<P>(&self, path: P) -> DataStreamFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>;

    /// Copies a file from one path to another within this `Backend`.
    ///
    /// Normally this will be an efficient operation but in some cases it will
    /// require retrieving the entire file and then sending it to the new
    /// location.
    fn copy_file<P, I>(&self, source: P, target: I) -> CopyCompleteFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
        I: TryInto<UploadInfo>,
        I::Error: Into<StorageError>,
    {
        let source = DataStream::from_stream(self.get_file_stream(source).try_flatten_stream());
        self.write_file_from_stream(target, source)
    }

    /// Moves a file from one path to another within this `Backend`.
    ///
    /// Normally this will be an efficient operation but in some cases it will
    /// require retrieving the entire file and then sending it to the new
    /// location.
    fn move_file<P, I>(&self, source: P, target: I) -> MoveCompleteFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
        I: TryInto<UploadInfo>,
        I::Error: Into<StorageError>,
    {
        let path = match source.try_into() {
            Ok(p) => p,
            Err(e) => {
                return MoveCompleteFuture::from_value(Err(TransferError::SourceError(e.into())));
            }
        };

        let deleter = self.clone();
        MoveCompleteFuture::from_future(self.copy_file(path.clone(), target).and_then(move |()| {
            deleter
                .delete_object(path)
                .map_err(TransferError::SourceError)
        }))
    }

    /// Deletes the object at the given path.
    ///
    /// For backends that support physical directories if the object at tbe path
    /// is a directory then this will delete the directory and its contents.
    ///
    /// This will return a [`NotFound`](enum.StorageErrorKind.html#variant.NotFound)
    /// error if the object does not exist.
    fn delete_object<P>(&self, path: P) -> OperationCompleteFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>;

    /// Writes a stream of data to the file at the given path.
    ///
    /// Calling this will overwrite anything at the given path (notably on
    /// backends that support symlinks or directories those will be deleted
    /// along with their contents and replaced with a file). The rationale for
    /// this is that for network based backends not overwriting generally
    /// involves more API calls to check if something is there first. If you
    /// care about overwriting, call [`get_object`](backends/trait.StorageBackend.html#method.get_file)
    /// first and check the result.
    ///
    /// If this operation fails there are no guarantees about the state of the
    /// file. If that is an issue then you should consider always calling
    /// [`delete_object`](backends/trait.StorageBackend.html#method.delete_object) after a
    /// failure.
    ///
    /// The future returned will only resolve once all the data from the stream
    /// is succesfully written to storage. If the provided stream resolves to
    /// None at any point this will be considered the end of the data to be
    /// written.
    ///
    /// Any error emitted by the stream will cause this operation to fail.
    fn write_file_from_stream<S, I, E, P>(&self, info: P, stream: S) -> WriteCompleteFuture
    where
        S: Stream<Item = Result<I, E>> + Send + 'static,
        I: IntoBuf + 'static,
        E: Into<StorageError> + 'static,
        P: TryInto<UploadInfo>,
        P::Error: Into<StorageError>;
}

#[enum_dispatch(StorageBackend)]
/// Provides access to a storage backend.
///
/// `FileStore` exposes all of the functionality guaranteed to be implemented by
/// every backend. This is the type you should use if you want your code to be
/// able to use any backend.
///
/// You create a `FileStore` from one of the [backend implementations](backends/index.html).
#[allow(clippy::large_enum_variant, missing_docs)]
#[derive(Clone, Debug)]
pub enum FileStore {
    #[cfg(feature = "file")]
    File(FileBackend),
    #[cfg(feature = "b2")]
    B2(B2Backend),
}
