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

//! A module with some useful tools for working with streams.
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::Stream;

use super::StorageResult;

pub(crate) type StreamPoll<R> = Poll<Option<R>>;
pub(crate) type ResultStreamPoll<R> = StreamPoll<StorageResult<R>>;

pub(crate) type PinnedStream<R> = Pin<Box<dyn Stream<Item = R> + Send + 'static>>;

/// Wraps a stream of an unknown type into a concrete type.
pub struct WrappedStream<R>
where
    R: Send + 'static,
{
    base: PinnedStream<R>,
}

impl<R> WrappedStream<R>
where
    R: Send + 'static,
{
    pub(crate) fn from_stream<S>(base: S) -> WrappedStream<S::Item>
    where
        S: Stream + Send + 'static,
        S::Item: Send,
    {
        WrappedStream {
            base: Box::pin(base),
        }
    }
}

impl<R> Stream for WrappedStream<R>
where
    R: Send + 'static,
{
    type Item = R;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> StreamPoll<R> {
        self.base.as_mut().poll_next(cx)
    }
}

/// Merges a set of streams into a single stream that returns results whenever
/// they arrive, not necessarily in the order the streams were added.
///
/// Implements Stream, polling it will poll all the owned streams returning an
/// item if found. Once an owned stream returns None that stream will be
/// dropped. Once this stream returns None adding more streams will cause it to
/// start returning values again.
#[derive(Default)]
pub struct MergedStreams<R>
where
    R: Send + 'static,
{
    streams: Vec<Pin<Box<WrappedStream<R>>>>,
}

impl<R> MergedStreams<R>
where
    R: Send + 'static,
{
    /// Creates a new `MergedStreams`.
    pub fn new() -> MergedStreams<R> {
        MergedStreams {
            streams: Vec::new(),
        }
    }

    /// Adds a new stream to the set of streams polled.
    pub fn push<S>(&mut self, stream: S)
    where
        S: Stream<Item = R> + Send + 'static,
    {
        self.streams
            .push(Box::pin(WrappedStream::<R>::from_stream(stream)));
    }
}

impl<R> Stream for MergedStreams<R>
where
    R: Send + 'static,
{
    type Item = R;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> StreamPoll<R> {
        if self.streams.is_empty() {
            return Poll::Ready(None);
        }

        let mut i = 0;
        while i < self.streams.len() {
            let stream = &mut self.streams[i];
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(r)) => return Poll::Ready(Some(r)),
                Poll::Ready(None) => {
                    self.streams.remove(i);
                    if self.streams.is_empty() {
                        return Poll::Ready(None);
                    }
                    continue;
                }
                Poll::Pending => {
                    // Move on to the next stream.
                }
            }
            i += 1;
        }

        Poll::Pending
    }
}

pub struct AfterStream<F, S>
where
    S: Unpin + Stream + 'static,
    F: Unpin + FnOnce() -> (),
{
    inner: Pin<Box<S>>,
    callback: Option<F>,
}

impl<F, S> AfterStream<F, S>
where
    S: Unpin + Stream + 'static,
    F: Unpin + FnOnce() -> (),
{
    pub fn after(stream: S, f: F) -> AfterStream<F, S> {
        AfterStream {
            inner: Box::pin(stream),
            callback: Some(f),
        }
    }
}

impl<F, S> Drop for AfterStream<F, S>
where
    S: Unpin + Stream + 'static,
    F: Unpin + FnOnce() -> (),
{
    fn drop(&mut self) {
        if let Some(callback) = self.callback.take() {
            callback();
        }
    }
}

impl<F, S> Stream for AfterStream<F, S>
where
    S: Unpin + Stream + 'static,
    F: Unpin + FnOnce() -> (),
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<S::Item>> {
        let this = self.get_mut();
        let result = this.inner.as_mut().poll_next(cx);

        if let Poll::Ready(None) = result {
            if let Some(callback) = this.callback.take() {
                callback();
            }
        }

        result
    }
}
