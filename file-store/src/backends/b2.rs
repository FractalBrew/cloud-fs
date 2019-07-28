//! Accesses files in a Backblaze B2 bucket. Included with the feature "b2".
use std::convert::TryFrom;
use std::io;

use super::{Backend, BackendImplementation, StorageImpl};
use crate::filestore::FileStore;
use crate::types::*;

const _DEFAULT_API_HOST: &str = "api.backblazeb2.com";
const _API_VERSION: &str = "v2";

/// The backend implementation for B2 storage.
#[derive(Debug, Clone)]
pub struct B2Backend {}

impl B2Backend {
    /// Creates a new [`FileStore`](../struct.FileStore.html) instance using the
    /// b2 backend.
    pub fn connect() -> ConnectFuture {
        ConnectFuture::from_result(Ok(FileStore {
            backend: BackendImplementation::B2(B2Backend {}),
        }))
    }
}

impl TryFrom<FileStore> for B2Backend {
    type Error = io::Error;

    fn try_from(file_store: FileStore) -> io::Result<B2Backend> {
        if let BackendImplementation::B2(b) = file_store.backend {
            Ok(b)
        } else {
            Err(error::invalid_settings(
                "FileStore does not hold a FileBackend",
            ))
        }
    }
}

impl StorageImpl for B2Backend {
    fn backend_type(&self) -> Backend {
        Backend::B2
    }

    fn list_objects(&self, _path: StoragePath) -> ObjectStreamFuture {
        unimplemented!();
    }

    fn get_object(&self, _path: StoragePath) -> ObjectFuture {
        unimplemented!();
    }

    fn get_file_stream(&self, _path: StoragePath) -> DataStreamFuture {
        unimplemented!();
    }

    fn delete_object(&self, _path: StoragePath) -> OperationCompleteFuture {
        unimplemented!();
    }

    fn write_file_from_stream(
        &self,
        _path: StoragePath,
        _stream: DataStream,
    ) -> WriteCompleteFuture {
        unimplemented!();
    }
}
