use std::fs::File;
use std::io::{BufWriter, Error, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, BytesMut};
use futures::stream::Stream;

use file_store::*;

use super::{IntoTestResult, TestResult};

pub const MB: u64 = 1024 * 1024;

macro_rules! test_fail {
    ($message:expr) => {{
        return Err(crate::runner::TestError::TestFailure(
            format!("assertion failed at {}:{}: {}", file!(), line!(), $message)
        ));
    }};
    ($($info:tt)*) => {{
        return Err(crate::runner::TestError::TestFailure(
            format!("assertion failed at {}:{}: {}",
                file!(), line!(), std::fmt::format(format_args!($($info)*)))
        ));
    }};
}

macro_rules! test_assert {
    ($check:expr) => {{
        if !$check {
            return Err(crate::runner::TestError::TestFailure(
                format!("assertion failed: `{}` at {}:{}", stringify!($check), file!(), line!()),
            ));
        }
    }};
    ($check:expr, $message:expr) => {{
        if !$check {
            return Err(crate::runner::TestError::TestFailure(
                format!("assertion failed: `{}` at {}:{}: {}", stringify!($check), file!(), line!(), $message)
            ));
        }
    }};
    ($check:expr, $($info:tt)*) => {{
        if !$check {
            return Err(crate::runner::TestError::TestFailure(
                format!("assertion failed: `{}` at {}:{}: {}",
                    stringify!($check), file!(), line!(), std::fmt::format(format_args!($($info)*)))
            ));
        }
    }};
}

macro_rules! test_assert_eq {
    ($f:expr, $e:expr) => {{
        let found = $f;
        let expected = $e;
        if found != expected {
            return Err(crate::runner::TestError::TestFailure(
                format!("assertion failed: `{} == {}` at {}:{}\n    found: `{:?}`\n expected: `{:?}`",
                    stringify!($f), stringify!($e), file!(), line!(), found, expected),
            ));
        }
    }};
    ($f:expr, $e:expr, $message:expr) => {{
        let found = $f;
        let expected = $e;
        if found != expected {
            return Err(crate::runner::TestError::TestFailure(
                format!("assertion failed: `{} == {}` at {}:{}: {}\n    found: `{:?}`\n expected: `{:?}`",
                    stringify!($f), stringify!($e), file!(), line!(), $message, found, expected),
            ));
        }
    }};
    ($f:expr, $e:expr, $($info:tt)*) => {{
        let found = $f;
        let expected = $e;
        if found != expected {
            return Err(crate::runner::TestError::TestFailure(
                format!("assertion failed: `{} == {}` at {}:{}: {}\n    found: `{:?}`\n expected: `{:?}`",
                    stringify!($f), stringify!($e), file!(), line!(), std::fmt::format(format_args!($($info)*)), found, expected),
            ));
        }
    }};
}

pub struct IteratorStream<I>
where
    I: Iterator<Item = u8>,
{
    iterator: I,
    buffer_size: usize,
}

pub fn stream_iterator<I>(iterator: I, buffer_size: usize) -> IteratorStream<I>
where
    I: Iterator<Item = u8>,
{
    IteratorStream {
        iterator,
        buffer_size,
    }
}

impl<I> Stream for IteratorStream<I>
where
    I: Iterator<Item = u8> + Unpin,
{
    type Item = Result<Data, Error>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Result<Data, Error>>> {
        let mut buffer = BytesMut::with_capacity(self.buffer_size);

        while let Some(b) = self.iterator.next() {
            buffer.put_u8(b);
            if buffer.len() == self.buffer_size {
                break;
            }
        }

        if buffer.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Ready(Some(Ok(buffer.freeze())))
        }
    }
}

pub struct ContentIterator {
    seed: u8,
    value: u8,
    length: u64,
    count: u64,
}

impl ContentIterator {
    pub fn new(seed: u8, length: u64) -> ContentIterator {
        ContentIterator {
            seed,
            value: seed,
            length,
            count: 0,
        }
    }
}

impl Iterator for ContentIterator {
    type Item = u8;

    fn next(&mut self) -> Option<u8> {
        if self.count >= self.length {
            return None;
        }

        self.count += 1;
        let new_value = self.value;
        let (new_value, _) = new_value.overflowing_add(27);
        let (new_value, _) = new_value.overflowing_mul(9);
        let (new_value, _) = new_value.overflowing_sub(self.seed);
        let (new_value, _) = new_value.overflowing_add(5);
        self.value = new_value;
        Some(self.value)
    }
}

pub fn write_file<I>(dir: &PathBuf, name: &str, content: I) -> TestResult<()>
where
    I: IntoIterator<Item = u8>,
{
    let mut target = dir.clone();
    target.push(name);

    let file = File::create(target).into_test_result()?;
    let mut writer = BufWriter::new(file);

    for b in content {
        loop {
            if writer.write(&[b]).into_test_result()? == 1 {
                break;
            }
        }
    }

    writer.flush().into_test_result()?;

    Ok(())
}
