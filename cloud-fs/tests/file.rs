#![cfg(feature = "file")]
#![feature(async_await)]
#![allow(clippy::needless_lifetimes)]

extern crate cloud_fs;

#[macro_use]
mod runner;

use cloud_fs::backends::{Backend, FileBackend};
use cloud_fs::{Fs, FsResult};
use runner::TestContext;

async fn build_fs(context: &TestContext) -> FsResult<(Fs, ())> {
    Ok((FileBackend::connect(&context.get_root()).await?, ()))
}

async fn cleanup(_: ()) {}

build_tests!(Backend::File, false, build_fs, cleanup);
