extern crate cloud_fs;

mod shared;
use shared::*;

use cloud_fs::backends::Backend;
use cloud_fs::{FsPath, FsResult, FsSettings};

#[test]
fn test_file_backend() -> FsResult<()> {
    let temp = prepare_test()?;

    let mut base = FsPath::new(format!("{}/", temp.path().display()))?;
    base.push_dir("test1");
    base.push_dir("dir1");
    run_from_settings(FsSettings::new(Backend::File, base));

    cleanup(temp)
}
