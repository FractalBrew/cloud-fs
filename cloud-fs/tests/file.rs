#![cfg(feature = "file")]
#![feature(async_await)]
#![allow(clippy::needless_lifetimes)]

extern crate cloud_fs;

#[macro_use]
mod runner;

use cloud_fs::backends::Backend;
use cloud_fs::{Fs, FsPath, FsResult, FsSettings};
use runner::TestContext;

async fn build_fs(context: &TestContext) -> FsResult<(Fs, ())> {
    let root = FsPath::new(format!("{}/", context.get_root().display()))?;
    Ok((Fs::connect(FsSettings::new(Backend::File, root)).await?, ()))
}

async fn cleanup(_: ()) {}

build_tests!(Backend::File, false, build_fs, cleanup);
