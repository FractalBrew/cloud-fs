#![cfg(feature = "file")]

extern crate cloud_fs;

#[macro_use]
mod runner;

use tokio::prelude::Future;

use cloud_fs::backends::Backend;
use cloud_fs::{Fs, FsError, FsPath, FsResult, FsSettings};
use runner::TestContext;

fn build_fs(context: &TestContext) -> FsResult<(impl Future<Item = Fs, Error = FsError>, ())> {
    let root = FsPath::new(format!("{}/", context.get_root().display()))?;
    Ok((Fs::connect(FsSettings::new(Backend::File, root)), ()))
}

fn cleanup(_: ()) {}

build_tests!("file", false, build_fs, cleanup);
