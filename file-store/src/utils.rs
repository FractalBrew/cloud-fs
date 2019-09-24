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

//! A set of useful utilities for converting between the different asynchronous
//! types that this crate uses.
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::io;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bytes::buf::FromBuf;
use bytes::{BytesMut, IntoBuf};
use futures::future::FutureExt;
use futures::stream::{Stream, StreamExt};
use tokio_io::{AsyncRead, BufReader};

use crate::future::WrappedFuture;
use crate::types::{Data, StorageError};

/// Converts an AsyncRead into a stream that emits [`Data`](../type.Data.html).
pub struct ReaderStream<R>
where
    R: AsyncRead,
{
    reader: Pin<Box<R>>,
    buffer: BytesMut,
    initial_buffer_size: usize,
    minimum_buffer_size: usize,
}

impl<R> ReaderStream<R>
where
    R: AsyncRead,
{
    /// Creates a stream that emits [`Data`](../type.Data.html) from an `AsynRead`.
    ///
    /// Passed a reader this will generate a stream that emits buffers of data
    /// asynchronously. The stream will attempt to read a buffer's worth of data
    /// from the reader. Initially it will use a buffer of `initial_buffer_size`
    /// size. As data is read the read buffer decreases in size until it reaches
    /// `minimum_buffer_size` at which point a new buffer of
    /// `initial_buffer_size` is used.
    pub fn stream<T>(
        reader: T,
        initial_buffer_size: usize,
        minimum_buffer_size: usize,
    ) -> impl Stream<Item = io::Result<Data>>
    where
        T: AsyncRead + Send + 'static,
    {
        let buf_reader = BufReader::new(reader);

        let mut buffer = BytesMut::with_capacity(initial_buffer_size);
        unsafe {
            buffer.set_len(initial_buffer_size);
            buf_reader.prepare_uninitialized_buffer(&mut buffer);
        }

        ReaderStream {
            reader: Box::pin(buf_reader),
            buffer,
            initial_buffer_size,
            minimum_buffer_size,
        }
    }

    fn inner_poll(&mut self, cx: &mut Context) -> Poll<Option<io::Result<Data>>> {
        match self.reader.as_mut().poll_read(cx, &mut self.buffer) {
            Poll::Ready(Ok(0)) => Poll::Ready(None),
            Poll::Ready(Ok(size)) => {
                let data = self.buffer.split_to(size);

                if self.buffer.len() < self.minimum_buffer_size {
                    self.buffer = BytesMut::with_capacity(self.initial_buffer_size);
                    unsafe {
                        self.buffer.set_len(self.initial_buffer_size);
                        self.reader.prepare_uninitialized_buffer(&mut self.buffer);
                    }
                }

                Poll::Ready(Some(Ok(data.freeze())))
            }
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
        }
    }
}

impl<R> Stream for ReaderStream<R>
where
    R: AsyncRead,
{
    type Item = io::Result<Data>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<io::Result<Data>>> {
        self.inner_poll(cx)
    }
}

pub(crate) fn into_data_stream<S, I, E>(stream: S) -> impl Stream<Item = Result<Data, StorageError>>
where
    S: Stream<Item = Result<I, E>> + Send + 'static,
    I: IntoBuf,
    E: Into<StorageError>,
{
    stream.map(|r| match r {
        Ok(d) => Ok(Data::from_buf(d)),
        Err(e) => Err(e.into()),
    })
}

struct PoolState<C, T, E>
where
    C: fmt::Debug,
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
{
    context: C,
    callback: Box<dyn Fn(&C) -> WrappedFuture<Result<T, E>> + Send>,
    ready: Vec<T>,
    available: Option<usize>,
    wakers: Vec<Waker>,
}

impl<C, T, E> fmt::Debug for PoolState<C, T, E>
where
    C: fmt::Debug,
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LimitedState {{ context: {:?}, available: {:?}, ready: {}, wakers: {} }}",
            self.context,
            self.available,
            self.ready.len(),
            self.wakers.len()
        )
    }
}

impl<C, T, E> PoolState<C, T, E>
where
    C: fmt::Debug + Send,
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
{
    fn new<F>(context: C, count: Option<usize>, callback: F) -> PoolState<C, T, E>
    where
        F: Fn(&C) -> WrappedFuture<Result<T, E>> + Send + 'static,
    {
        PoolState {
            context,
            callback: Box::new(callback),
            ready: Default::default(),
            available: count,
            wakers: Default::default(),
        }
    }

    fn awaken(&mut self) {
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }

    fn release(&mut self, t: Option<T>) {
        match t {
            Some(t) => self.ready.push(t),
            None => {
                if let Some(count) = self.available.take() {
                    self.available = Some(count + 1);
                }
            }
        }
        self.awaken();
    }
}

#[derive(Debug)]
pub(crate) struct Pool<C, T, E>
where
    C: fmt::Debug,
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
{
    state: Arc<Mutex<PoolState<C, T, E>>>,
}

impl<C, T, E> Clone for Pool<C, T, E>
where
    C: fmt::Debug,
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
{
    fn clone(&self) -> Pool<C, T, E> {
        Pool {
            state: self.state.clone(),
        }
    }
}

impl<C, T, E> Pool<C, T, E>
where
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
    C: fmt::Debug + Send,
{
    pub fn new<F>(context: C, count: Option<usize>, callback: F) -> Pool<C, T, E>
    where
        F: Fn(&C) -> WrappedFuture<Result<T, E>> + Send + 'static,
    {
        Pool {
            state: Arc::new(Mutex::new(PoolState::new(context, count, callback))),
        }
    }

    pub async fn acquire(&self) -> Result<Acquired<C, T, E>, E> {
        let future = AcquireFuture {
            pending: None,
            state: self.state.clone(),
        };

        future.await
    }
}

pub(crate) struct AcquireFuture<C, T, E>
where
    C: fmt::Debug,
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
{
    pending: Option<WrappedFuture<Result<T, E>>>,
    state: Arc<Mutex<PoolState<C, T, E>>>,
}

impl<C, T, E> AcquireFuture<C, T, E>
where
    C: fmt::Debug + Send,
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
{
    fn result(&self, t: T) -> Acquired<C, T, E> {
        Acquired {
            state: self.state.clone(),
            inner: Some(t),
        }
    }

    fn poll_inner(
        &mut self,
        mut future: WrappedFuture<Result<T, E>>,
        cx: &mut Context,
    ) -> Poll<<Self as Future>::Output> {
        match future.poll_inner(cx) {
            Poll::Ready(Ok(t)) => Poll::Ready(Ok(self.result(t))),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                self.pending = Some(future);
                Poll::Pending
            }
        }
    }
}

impl<C, T, E> Future for AcquireFuture<C, T, E>
where
    C: fmt::Debug + Send,
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
{
    type Output = Result<Acquired<C, T, E>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(future) = this.pending.take() {
            return this.poll_inner(future, cx);
        }

        let future = {
            let mut state = this.state.lock().unwrap();

            if !state.ready.is_empty() {
                return Poll::Ready(Ok(this.result(state.ready.remove(0))));
            } else if let Some(avail) = state.available {
                if avail > 0 {
                    state.available = Some(avail - 1);
                    let callback = &state.callback;
                    callback(&state.context)
                } else {
                    state.wakers.push(cx.waker().clone());
                    return Poll::Pending;
                }
            } else {
                let callback = &state.callback;
                callback(&state.context)
            }
        };

        this.poll_inner(future, cx)
    }
}

pub(crate) struct Acquired<C, T, E>
where
    C: fmt::Debug + Send,
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
{
    state: Arc<Mutex<PoolState<C, T, E>>>,
    inner: Option<T>,
}

impl<C, T, E> Acquired<C, T, E>
where
    C: fmt::Debug + Send,
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
{
    pub fn destroy(&mut self) {
        if self.inner.take().is_some() {
            let mut state = self.state.lock().unwrap();
            state.release(None);
        }
    }

    pub fn release(&mut self) {
        if let Some(t) = self.inner.take() {
            let mut state = self.state.lock().unwrap();
            state.release(Some(t));
        }
    }
}

impl<C, T, E> Drop for Acquired<C, T, E>
where
    C: fmt::Debug + Send,
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
{
    fn drop(&mut self) {
        self.release();
    }
}

impl<C, T, E> Deref for Acquired<C, T, E>
where
    C: fmt::Debug + Send,
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner.as_ref().unwrap()
    }
}

impl<C, T, E> DerefMut for Acquired<C, T, E>
where
    C: fmt::Debug + Send,
    T: fmt::Debug + Send + 'static,
    E: Send + 'static,
{
    fn deref_mut(&mut self) -> &mut T {
        self.inner.as_mut().unwrap()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InfalliblePool<C, T>
where
    C: fmt::Debug,
    T: fmt::Debug + Send + 'static,
{
    inner: Pool<C, T, Infallible>,
}

impl<C, T> InfalliblePool<C, T>
where
    C: fmt::Debug + Send,
    T: fmt::Debug + Send + 'static,
{
    pub fn new<F>(context: C, count: Option<usize>, callback: F) -> InfalliblePool<C, T>
    where
        F: Fn(&C) -> WrappedFuture<T> + Send + Sync + 'static,
    {
        InfalliblePool {
            inner: Pool::new(context, count, move |c| {
                WrappedFuture::<T>::from_future(callback(c).map(Ok))
            }),
        }
    }

    pub async fn acquire(&self) -> Acquired<C, T, Infallible> {
        self.inner.acquire().await.unwrap()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CloningPool<T>
where
    T: fmt::Debug + Send + Clone + 'static,
{
    inner: InfalliblePool<T, T>,
}

impl<T> CloningPool<T>
where
    T: fmt::Debug + Send + Clone + 'static,
{
    pub fn new(base: T, count: Option<usize>) -> CloningPool<T> {
        CloningPool {
            inner: InfalliblePool::new(base, count, |t| WrappedFuture::from_value(t.clone())),
        }
    }

    pub async fn acquire(&self) -> Acquired<T, T, Infallible> {
        self.inner.acquire().await
    }
}
