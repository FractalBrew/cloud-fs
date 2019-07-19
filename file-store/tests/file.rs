#![cfg(feature = "file")]
#![feature(async_await)]
#![allow(clippy::needless_lifetimes)]

extern crate file_store;

#[macro_use]
mod runner;

use file_store::backends::{Backend, FileBackend};
use file_store::{FileStore, FsResult};
use runner::TestContext;

async fn build_fs(context: &TestContext) -> FsResult<(FileStore, ())> {
    Ok((FileBackend::connect(&context.get_root()).await?, ()))
}

async fn cleanup(_: ()) {}

build_tests!(Backend::File, false, build_fs, cleanup);
