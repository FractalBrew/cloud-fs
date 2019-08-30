//! A set of useful utilities for converting between the different asynchronous
//! types that this crate uses.
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures::stream::Stream;
use tokio_io::{AsyncRead, BufReader};

use crate::types::Data;

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
