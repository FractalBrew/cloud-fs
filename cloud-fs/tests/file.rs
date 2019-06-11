extern crate cloud_fs;

use tokio;
use tokio::prelude::*;

mod shared;
use shared::*;

use cloud_fs::{Backend, Fs, FsPath, FsResult, FsSettings};

#[test]
fn test_file_backend() -> FsResult<()> {
    /*let temp = prepare_test()?;

    let settings = FsSettings::new(Backend::File, FsPath::from_std_path(temp.path())?);
    tokio::run(Fs::new(settings).and_then(run_test).map_err(|e| panic!(e)));
    cleanup(temp)*/
    Ok(())
}
