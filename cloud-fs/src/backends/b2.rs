//! Accesses files in a Backblaze B2 bucket. Included with the feature "b2".
use crate::*;

/// The backend implementation for B2 storage.
#[derive(Debug)]
pub struct B2Backend {
    settings: FsSettings,
}

impl B2Backend {
    /// Creates a new instance of the B2 backend.
    pub fn connect(_settings: FsSettings) -> ConnectFuture {
        ConnectFuture::from_error(FsError::new(FsErrorKind::NotImplemented, "B2Backend::connect is not yet implemented."))
    }
}

impl FsImpl for B2Backend {
    fn list_files(&self, _path: FsPath) -> FileListFuture {
        FileListFuture::from_error(FsError::new(FsErrorKind::NotImplemented, "B2Backend::list_files is not yet implemented."))
    }

    fn get_file(&self, _path: FsPath) -> FileFuture {
        FileFuture::from_error(FsError::new(FsErrorKind::NotImplemented, "B2Backend::get_file is not yet implemented."))
    }

    fn get_file_stream(&self, _path: FsPath) -> DataStreamFuture {
        DataStreamFuture::from_error(FsError::new(FsErrorKind::NotImplemented, "B2Backend::get_file_stream is not yet implemented."))
    }

    fn delete_file(&self, _path: FsPath) -> OperationCompleteFuture {
        OperationCompleteFuture::from_error(FsError::new(FsErrorKind::NotImplemented, "B2Backend::delete_file is not yet implemented."))
    }

    fn write_from_stream(&self, _path: FsPath, _stream: DataStream) -> OperationCompleteFuture {
        OperationCompleteFuture::from_error(FsError::new(FsErrorKind::NotImplemented, "B2Backend::write_from_stream is not yet implemented."))
    }
}
