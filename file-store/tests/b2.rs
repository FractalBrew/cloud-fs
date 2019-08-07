#![cfg(feature = "b2")]
#![feature(async_await)]
#![allow(clippy::needless_lifetimes)]

extern crate file_store;

#[macro_use]
mod runner;
mod mocks;

use futures::channel::oneshot::Sender;

use file_store::backends::B2Backend;
use file_store::backends::Backend;
use file_store::executor::spawn;
use file_store::FileStore;

use mocks::b2_server::build_server;
use runner::{TestContext, TestError, TestResult};

async fn build_fs(context: &TestContext) -> TestResult<(FileStore, Sender<()>)> {
    let (addr, server, sender) = build_server(context.get_root())?;

    let _ = spawn(server);

    let fs = B2Backend::builder("foo", "bar")
        .host(&format!("http://{}", addr))
        .build()
        .await?;
    Ok((fs, sender))
}

async fn cleanup(sender: Sender<()>) -> TestResult<()> {
    sender.send(()).map_err(|()| {
        TestError::HarnessFailure(String::from("Failed to send shutdown to mock b2 server."))
    })
}

build_tests!(Backend::B2, build_fs, cleanup);
