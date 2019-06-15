extern crate cloud_fs;

mod runner;

use std::path::Path;

use tokio::prelude::Future;

use cloud_fs::backends::Backend;
use cloud_fs::{Fs, FsError, FsPath, FsResult, FsSettings};

fn build_fs(path: &Path) -> FsResult<(impl Future<Item = Fs, Error = FsError>, ())> {
    let mut base = FsPath::new(format!("{}/", path.display()))?;
    base.push_dir("test1");
    base.push_dir("dir1");

    Ok((Fs::new(FsSettings::new(Backend::File, base)), ()))
}

fn cleanup(_: ()) {}

build_tests!("file", true, build_fs, cleanup);
