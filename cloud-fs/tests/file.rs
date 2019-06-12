extern crate cloud_fs;

use tokio;
use tokio::prelude::*;

mod shared;
use shared::*;

use cloud_fs::{Backend, Fs, FsPath, FsResult, FsSettings};

#[test]
fn test_file_backend() -> FsResult<()> {
    let temp = prepare_test()?;

    let mut base = FsPath::new(format!("{}/", temp.path().display()))?;
    base.push_dir("test1");
    base.push_dir("dir1");
    let settings = FsSettings::new(Backend::File, base);
    tokio::run(
        Fs::new(settings)
            .and_then(run_test)
            .map_err(|e| panic!("{}", e)),
    );
    cleanup(temp)
}
