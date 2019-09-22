// Copyright 2019 Dave Townsend
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A module with some useful tools for working with futures.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::ready;

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

    pub(crate) fn from_value(value: R) -> WrappedFuture<R> {
        WrappedFuture {
            base: Box::pin(ready(value)),
        }
    }

    pub(crate) fn poll_inner(&mut self, cx: &mut Context) -> FuturePoll<R> {
        self.base.as_mut().poll(cx)
    }
}

impl<R> Future for WrappedFuture<R>
where
    R: Send + 'static,
{
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> FuturePoll<R> {
        self.poll_inner(cx)
    }
}
