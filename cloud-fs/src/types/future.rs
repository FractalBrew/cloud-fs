use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::ready;

use super::*;

pub(crate) type FuturePoll<R> = Poll<R>;
pub(crate) type FsFuturePoll<R> = FuturePoll<FsResult<R>>;

pub(crate) type PinnedFuture<R> = Pin<Box<dyn Future<Output = R> + Send + 'static>>;
pub(crate) type FsPinnedFuture<R> = PinnedFuture<FsResult<R>>;

/// A Future whose error is always an [`FsError`](struct.FsError.html).
///
/// This is mostly used to hide the underlying futures in use which may change
/// frequently.
pub struct FsFuture<R>
where
    R: Send + 'static,
{
    base: FsPinnedFuture<R>,
}

impl<R> FsFuture<R>
where
    R: Send + 'static,
{
    pub(crate) fn from_future<F>(base: F) -> Self
    where
        F: Future<Output = FsResult<R>> + Send + 'static,
    {
        FsFuture {
            base: Box::pin(base),
        }
    }

    pub(crate) fn from_error<E>(error: E) -> Self
    where
        E: Into<FsError>,
    {
        FsFuture::from_future(ready(Err(error.into())))
    }
}

impl<R> Future for FsFuture<R>
where
    R: Send + 'static,
{
    type Output = FsResult<R>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> FsFuturePoll<R> {
        self.base.as_mut().poll(cx)
    }
}
