use tokio::prelude::*;

struct FutureHolder<I, E>
where
    I: Sized + Send + Sync,
    E: Sized + Send + Sync,
{
    future: Box<Future<Item = I, Error = E> + Send + Sync + 'static>,
}

impl<I, E> Future for FutureHolder<I, E>
where
    I: Sized + Send + Sync,
    E: Sized + Send + Sync,
{
    type Item = I;
    type Error = E;

    fn poll(&mut self) -> Result<Async<I>, E> {
        self.future.poll()
    }
}

struct StreamHolder<I, E>
where
    I: Sized + Send + Sync,
    E: Sized + Send + Sync,
{
    stream: Box<Stream<Item = I, Error = E> + Send + Sync + 'static>,
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

pub fn stream_from_future<F, S>(future: F) -> FutureStream<F, S>
where
    F: Future<Item = S, Error = S::Error> + Sized + Send + Sync,
    S: Stream + Sized + Send + Sync,
{
    FutureStream {
        current: FutureOrStream::Future(future),
    }
}

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
    pub fn new() -> MergedStreams<I, E> {
        MergedStreams {
            streams: Vec::new(),
        }
    }

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

    pub fn push<S>(&mut self, stream: S)
    where
        S: Stream<Item = I, Error = E> + Sized + Send + Sync + 'static,
    {
        self.streams.push(StreamHolder {
            stream: Box::new(stream),
        });
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
        let mut i = 0;
        while i < self.streams.len() {
            let stream = &mut self.streams[i];
            match stream.poll() {
                Ok(Async::Ready(Some(i))) => return Ok(Async::Ready(Some(i))),
                Ok(Async::Ready(None)) => {
                    self.streams.remove(i);
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
