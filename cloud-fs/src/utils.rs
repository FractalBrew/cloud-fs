//! A useful set of utilities for working with futures and streams.
use tokio::prelude::*;

/// Holds an instance of `Stream`. This can be useful when you want to simplify
/// the types you're working with.
pub struct StreamHolder<I, E>
where
    I: Sized + Send + Sync,
    E: Sized + Send + Sync,
{
    stream: Box<Stream<Item = I, Error = E> + Send + Sync + 'static>,
}

impl<I, E> StreamHolder<I, E>
where
    I: Sized + Send + Sync,
    E: Sized + Send + Sync,
{
    /// Creates a new `StreamHolder` to hold a stream.
    pub fn new<S>(stream: S) -> StreamHolder<I, E>
    where
        S: Stream<Item = I, Error = E> + Send + Sync + 'static,
    {
        StreamHolder { stream: Box::new(stream) }
    }
}

impl<I, E> Stream for StreamHolder<I, E>
where
    I: Sized + Send + Sync,
    E: Sized + Send + Sync,
{
    type Item = I;
    type Error = E;

    fn poll(&mut self) -> Result<Async<Option<I>>, E> {
        self.stream.poll()
    }
}

enum FutureOrStream<F, S>
where
    F: Future + Sized + Send + Sync,
    S: Stream + Sized + Send + Sync,
{
    Future(F),
    Stream(S),
}

/// Converts a `Future` that returns a stream into a stream.
///
/// Polling the stream first polls the future and then once resolved polls the
/// returned stream.
pub struct FutureStream<F, S>
where
    F: Future<Item = S, Error = S::Error> + Sized + Send + Sync,
    S: Stream + Sized + Send + Sync,
{
    current: FutureOrStream<F, S>,
}

impl<F, S> Stream for FutureStream<F, S>
where
    F: Future<Item = S, Error = S::Error> + Sized + Send + Sync,
    S: Stream + Sized + Send + Sync,
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        match self.current {
            FutureOrStream::Future(ref mut future) => match future.poll() {
                Ok(result) => match result {
                    Async::Ready(mut stream) => {
                        let result = stream.poll();
                        self.current = FutureOrStream::Stream(stream);
                        result
                    }
                    Async::NotReady => Ok(Async::NotReady),
                },
                Err(error) => Err(error),
            },
            FutureOrStream::Stream(ref mut stream) => stream.poll(),
        }
    }
}

/// Converts a `Future` that returns a stream into a stream. See [FutureStream](#struct.FutureStream)
/// for more details.
pub fn stream_from_future<F, S>(future: F) -> FutureStream<F, S>
where
    F: Future<Item = S, Error = S::Error> + Sized + Send + Sync,
    S: Stream + Sized + Send + Sync,
{
    FutureStream {
        current: FutureOrStream::Future(future),
    }
}

/// Merges a set of streams into a single stream that returns results in any
/// order.
///
/// Implements Stream, polling it will poll all the owned streams returning an
/// item if found. One an owned stream returns None that stream will be dropped.
/// Once this stream returns None adding more streams will cause it to start
/// returning values again.
#[derive(Default)]
pub struct MergedStreams<I, E>
where
    I: Sized + Send + Sync,
    E: Sized + Send + Sync,
{
    streams: Vec<StreamHolder<I, E>>,
}

impl<I, E> MergedStreams<I, E>
where
    I: Sized + Send + Sync,
    E: Sized + Send + Sync,
{
    /// Creates a new `MergedStreams`.
    pub fn new() -> MergedStreams<I, E> {
        MergedStreams {
            streams: Vec::new(),
        }
    }

    /// Returns a new MergedStreams using the initial stream given.
    pub fn start<A, B, S>(stream: S) -> MergedStreams<A, B>
    where
        A: Sized + Send + Sync,
        B: Sized + Send + Sync,
        S: Stream<Item = A, Error = B> + Sized + Send + Sync + 'static,
    {
        let mut merged = MergedStreams::new();
        merged.push(stream);
        merged
    }

    /// Adds a new stream to the set of streams polled.
    pub fn push<S>(&mut self, stream: S)
    where
        S: Stream<Item = I, Error = E> + Sized + Send + Sync + 'static,
    {
        self.streams.push(StreamHolder::new(stream));
    }
}

impl<I, E> Stream for MergedStreams<I, E>
where
    I: Sized + Send + Sync,
    E: Sized + Send + Sync,
{
    type Item = I;
    type Error = E;

    fn poll(&mut self) -> Result<Async<Option<I>>, E> {
        if self.streams.is_empty() {
            return Ok(Async::Ready(None));
        }

        let mut i = 0;
        while i < self.streams.len() {
            let stream = &mut self.streams[i];
            match stream.poll() {
                Ok(Async::Ready(Some(i))) => return Ok(Async::Ready(Some(i))),
                Ok(Async::Ready(None)) => {
                    self.streams.remove(i);
                    if self.streams.is_empty() {
                        return Ok(Async::Ready(None));
                    }
                    continue;
                }
                Ok(Async::NotReady) => {
                    // Move on to the next stream.
                }
                Err(error) => return Err(error),
            }
            i += 1;
        }

        Ok(Async::NotReady)
    }
}
