extern crate cloud_fs;
extern crate tempfile;
extern crate tokio;

mod read;
mod utils;
mod write;

use std::fs::create_dir_all;
use std::iter::empty;
use std::path::PathBuf;

use tempfile::{tempdir, TempDir};
use tokio::prelude::*;

use utils::*;

use cloud_fs::utils::run_future;
use cloud_fs::*;

pub fn prepare_test() -> FsResult<TempDir> {
    let temp = tempdir()?;

    let mut dir = PathBuf::from(temp.path());
    dir.push("test1");
    dir.push("dir1");
    create_dir_all(dir.clone())?;

    write_file(
        &dir,
        "smallfile.txt",
        b"This is quite a short file.".iter().cloned(),
    )?;
    write_file(&dir, "largefile", ContentIterator::new(0, 100 * MB))?;
    write_file(&dir, "mediumfile", ContentIterator::new(58, 5 * MB))?;

    dir.push("dir2");
    create_dir_all(dir.clone())?;
    write_file(&dir, "foo", empty())?;
    write_file(&dir, "bar", empty())?;
    write_file(&dir, "0foo", empty())?;
    write_file(&dir, "5diz", empty())?;
    write_file(&dir, "1bar", empty())?;
    write_file(&dir, "daz", empty())?;
    write_file(&dir, "hop", empty())?;
    write_file(&dir, "yu", empty())?;

    Ok(temp)
}

pub fn run<F>(future: F)
where
    F: Future<Item = Fs, Error = FsError> + Sized + Send + Sync + 'static,
{
    if let Err(e) = run_future(future) {
        panic!("{}", e);
    }
}

pub fn run_from_settings(settings: FsSettings) {
    run(Fs::new(settings)
        .and_then(read::run_tests)
        .and_then(write::run_tests));
}

pub fn cleanup(temp: TempDir) -> FsResult<()> {
    temp.close()?;

    Ok(())
}
