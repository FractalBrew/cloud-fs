use tokio::prelude::*;

use cloud_fs::*;

fn test_delete_file(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    future::finished(fs)
}

fn test_write_from_stream(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    future::finished(fs)
}

pub fn run_tests(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    test_delete_file(fs).and_then(test_write_from_stream)
}
