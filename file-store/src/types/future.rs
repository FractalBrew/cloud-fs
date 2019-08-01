//! A module with some useful tools for working with futures.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub(crate) type FuturePoll<R> = Poll<R>;

pub(crate) type PinnedFuture<R> = Pin<Box<dyn Future<Output = R> + Send + 'static>>;

/// Wraps a future of an unknown type into a concrete type.
pub struct WrappedFuture<R>
where
    R: Send + 'static,
{
    base: PinnedFuture<R>,
}

impl<R> WrappedFuture<R>
where
    R: Send + 'static,
{
    pub(crate) fn from_future<F>(base: F) -> WrappedFuture<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        WrappedFuture {
            base: Box::pin(base),
        }
    }
}

impl<R> Future for WrappedFuture<R>
where
    R: Send + 'static,
{
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> FuturePoll<R> {
        self.base.as_mut().poll(cx)
    }
}
