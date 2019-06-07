use super::BackendImplementation;

use crate::types::{FsErrorType, FsPath};
use crate::*;

/// Accesses files on the local filesystem. Included with the feature "file".
#[derive(Debug)]
pub struct FileBackend {
    settings: FsSettings,
}

impl FileBackend {
    pub fn connect(settings: FsSettings) -> ConnectFuture {
        if settings.hostname().is_some() {
            ConnectFuture::from_error(FsError::new(
                FsErrorType::HostNotSupported,
                "The File fs does not support accessing other hosts.",
            ))
        } else {
            ConnectFuture::from_item(Fs {
                backend: BackendImplementation::File(FileBackend {
                    settings: settings.to_owned(),
                }),
            })
        }
    }
}

impl FsImpl for FileBackend {
    fn list_files(&self, path: &FsPath) -> FileListStream {
        unimplemented!();
    }

    fn get_file(&self, path: &FsPath) -> FileFuture {
        unimplemented!();
    }

    fn delete_file(&self, path: &FsPath) -> OperationCompleteFuture {
        unimplemented!();
    }

    fn get_file_stream(&self, path: &FsPath) -> DataStreamFuture {
        unimplemented!();
    }

    fn write_from_stream(&self, path: &FsPath, stream: DataStream) -> OperationCompleteFuture {
        unimplemented!();
    }
}
