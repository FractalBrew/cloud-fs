extern crate cloud_fs;

mod shared;
use shared::*;

use cloud_fs::{Backend, FsSettings, FsResult, FsPath};

#[test]
fn test_file_backend() -> FsResult<()> {
    let temp = begin_test()?;

    let settings = FsSettings::new(Backend::File, FsPath::from_std_path(temp.path())?);
    run_test(settings)?;
    end_test(temp)
}
