use tokio::prelude::*;

use cloud_fs::*;

pub fn test_delete_file(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    future::finished(fs)
}

pub fn test_write_from_stream(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    future::finished(fs)
}
