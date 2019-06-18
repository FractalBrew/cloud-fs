use std::fs::metadata;
use std::io::ErrorKind;
use std::path::PathBuf;

use tokio::prelude::*;

use cloud_fs::*;

pub fn test_delete_file(fs: Fs, local_path: PathBuf) -> impl Future<Item = Fs, Error = FsError> {
    fn test_delete(fs: Fs, local_path: PathBuf, path: &str) -> impl Future<Item = (Fs, PathBuf), Error = FsError> {
        let remote = FsPath::new(path).unwrap();
        let mut target = local_path.clone();
        target.push(FsPath::new("/").unwrap().relative(&remote).unwrap().as_std_path());
        fs.delete_file(remote)
            .and_then(move |_| {
                match metadata(target.clone()) {
                    Ok(_) => test_fail!("Failed to delete {}", target.display()),
                    Err(e) => test_assert_eq!(e.kind(), ErrorKind::NotFound, "Should have failed to find {}", target.display()),
                }
                Ok((fs, local_path))
            })
    }

    test_delete(fs, local_path, "/largefile")
        .map(|(fs, _)| fs)
}

pub fn test_write_from_stream(fs: Fs, _local_path: PathBuf) -> impl Future<Item = Fs, Error = FsError> {
    future::finished(fs)
}
