#![cfg(feature = "file")]
#![feature(async_await)]
#![allow(clippy::needless_lifetimes)]

extern crate file_store;

#[macro_use]
mod runner;

use file_store::backends::{Backend, FileBackend};
use file_store::FileStore;
use runner::{TestContext, TestResult};

async fn build_fs(context: &TestContext) -> TestResult<(FileStore, ())> {
    Ok((FileBackend::connect(&context.get_root()).await?, ()))
}

async fn cleanup(_: ()) -> TestResult<()> {
    Ok(())
}

build_tests!(Backend::File, build_fs, cleanup);
