//! A default executor to use when running futures from this crate.
//!
//! Ideally everything in this crate would run under any executor. In practice
//! some parts have dependencies on tokio's executor. Hopefully that will change
//! in the future though so for now this exposes a way to run futures that will
//! work for this crate.

use std::future::Future;
use std::sync::mpsc;

use ::futures::compat::{Compat, Future01CompatExt};
use ::futures::future::FutureExt;
use tokio_sync::oneshot;

/// Runs a future on the existing runtime.
pub fn spawn<F>(future: F) -> impl Future<Output = Result<F::Output, oneshot::error::RecvError>>
where
    F: Future + Send + Unpin + 'static,
    F::Output: Send,
{
    let (sender, receiver) = oneshot::channel::<F::Output>();

    let compat = Compat::new(future.map(move |r| match sender.send(r) {
        Ok(()) => Ok(()),
        Err(_) => Err(()),
    }));

    tokio::executor::spawn(compat);

    receiver.compat()
}

/// Runs a future to completion on a new tokio executor and returns the result.
pub fn run<F>(future: F) -> Result<F::Output, mpsc::TryRecvError>
where
    F: Future + Send + Unpin + 'static,
    F::Output: Send,
{
    let (sender, receiver) = mpsc::channel::<F::Output>();
    let compat = Compat::new(future.map(move |r| match sender.send(r) {
        Ok(()) => Ok(()),
        Err(_) => Err(()),
    }));
    tokio::run(compat);
    receiver.try_recv()
}
