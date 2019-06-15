use cloud_fs::*;

use std::fmt::Debug;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub const MB: u64 = 1024 * 1024;

pub fn assert_eq<T: Debug + Eq, S: AsRef<str>>(left: T, right: T, message: S) -> FsResult<()> {
    if left == right {
        Ok(())
    } else {
        Err(FsError::new(
            FsErrorType::TestFailure,
            format!(
                "assertion failed: {}\n  left: `{:?}`\n right: `{:?}`",
                message.as_ref(),
                left,
                right
            ),
        ))
    }
}

pub struct ContentIterator {
    value: u8,
    length: u64,
    count: u64,
}

impl ContentIterator {
    pub fn new(seed: u8, length: u64) -> ContentIterator {
        ContentIterator {
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
        let (new_value, _) = new_value.overflowing_add(5);
        self.value = new_value;
        Some(self.value)
    }
}

pub fn write_file<I: IntoIterator<Item = u8>>(
    dir: &PathBuf,
    name: &str,
    content: I,
) -> FsResult<()> {
    let mut target = dir.clone();
    target.push(name);

    let file = File::create(target)?;
    let mut writer = BufWriter::new(file);

    for b in content {
        loop {
            if writer.write(&[b])? == 1 {
                break;
            }
        }
    }

    writer.flush()?;

    Ok(())
}
