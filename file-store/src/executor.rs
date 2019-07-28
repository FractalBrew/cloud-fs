//! A default executor to use when running futures from this crate.
//!
//! Ideally everything in this crate would run under any executor. In practice
//! some parts have dependencies on tokio's executor. Hopefully that will change
//! in the future though so for now this exposes an executor guaranteed to work
//! and used in tests to verify that.
use std::boxed::Box;
use std::future::Future;
use std::sync::mpsc;

use futures::channel::oneshot;
use futures::compat::Compat;
use futures::future::FutureExt;
use tokio::run as tokio_run;
use tokio_executor::spawn as tokio_spawn;

/// Spawns a future on the existing runtime returning its result.
pub fn spawn<F>(future: F) -> impl Future<Output = Result<F::Output, oneshot::Canceled>>
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    let (sender, receiver) = oneshot::channel::<F::Output>();

    let compat = Compat::new(Box::pin(future).map(move |r| match sender.send(r) {
        Ok(()) => Ok(()),
        Err(_) => Err(()),
    }));

    tokio_spawn(compat);

    receiver
}

/// Runs a future to completion on a new tokio executor and returns the result.
///
/// This blocks the calling thread.
pub fn run<F>(future: F) -> Result<F::Output, mpsc::TryRecvError>
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    let (sender, receiver) = mpsc::channel::<F::Output>();

    let compat = Compat::new(Box::pin(future).map(move |r| match sender.send(r) {
        Ok(()) => Ok(()),
        Err(_) => Err(()),
    }));

    tokio_run(compat);

    receiver.try_recv()
}
