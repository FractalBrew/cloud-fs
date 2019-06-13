extern crate cloud_fs;
extern crate tempfile;
extern crate tokio;

use std::fmt::Debug;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::mpsc;
use std::iter::empty;

use tempfile::{tempdir, TempDir};
use tokio::prelude::*;

use cloud_fs::*;

const MB: u64 = 1024 * 1024;

/*fn assert<S: AsRef<str>>(check: bool, message: S) -> FsResult<()> {
    if check {
        Ok(())
    } else {
        Err(FsError::new(FsErrorType::TestFailure, format!("assertion failed: {}", message.as_ref())))
    }
}*/

fn assert_eq<T: Debug + Eq, S: AsRef<str>>(left: T, right: T, message: S) -> FsResult<()> {
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

struct ContentIterator {
    value: u8,
    length: u64,
    count: u64,
}

impl ContentIterator {
    fn new(seed: u8, length: u64) -> ContentIterator {
        ContentIterator { value: seed, length, count: 0, }
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

fn write_file<I: IntoIterator<Item = u8>>(dir: &PathBuf, name: &str, content: I) -> FsResult<()> {
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

pub fn prepare_test() -> FsResult<TempDir> {
    let temp = tempdir()?;

    let mut dir = PathBuf::from(temp.path());
    dir.push("test1");
    dir.push("dir1");
    create_dir_all(dir.clone())?;

    write_file(&dir, "smallfile.txt", b"This is quite a short file.".iter().cloned())?;
    write_file(&dir, "largefile", ContentIterator::new(0, 100 * MB))?;
    write_file(&dir, "mediumfile", ContentIterator::new(58, 5 * MB))?;

    dir.push("dir2");
    create_dir_all(dir.clone())?;
    write_file(&dir, "foo", empty())?;
    write_file(&dir, "bar", empty())?;
    write_file(&dir, "0foo", empty())?;
    write_file(&dir, "5diz", empty())?;
    write_file(&dir, "1bar", empty())?;
    write_file(&dir, "daz", empty())?;
    write_file(&dir, "hop", empty())?;
    write_file(&dir, "yu", empty())?;

    Ok(temp)
}

struct FileChecker {
    path: FsPath,
    size: u64,
}

impl FileChecker {
    fn check(&self, file: &FsFile) -> FsResult<()> {
        assert_eq(file.path(), &self.path, "Should have the expected name.")?;
        assert_eq(
            file.size(),
            self.size,
            format!("Should have the expected size for {}", self.path),
        )?;

        Ok(())
    }

    fn check_files(found: Vec<FsFile>, expected: Vec<FileChecker>) -> FsResult<()> {
        for (file, checker) in found.iter().zip(expected.iter()) {
            checker.check(file)?;
        }

        assert_eq(
            found.len(),
            expected.len(),
            "Should have seen the right number of results.",
        )?;

        Ok(())
    }
}

fn test_list_files(fs: &Fs) -> impl Future<Item = (), Error = FsError> {
    fn expect(
        fs: &Fs,
        path: &FsPath,
        files: Vec<FileChecker>,
    ) -> impl Future<Item = (), Error = FsError> {
        fs.list_files(path)
            .and_then(|s| s.collect())
            .and_then(move |mut results| {
                results.sort();
                FileChecker::check_files(results, files)
            })
    }

    let all = vec![
        FileChecker {
            path: FsPath::new("/largefile").unwrap(),
            size: 100 * MB,
        },
        FileChecker {
            path: FsPath::new("/mediumfile").unwrap(),
            size: 5 * MB,
        },
        FileChecker {
            path: FsPath::new("/smallfile.txt").unwrap(),
            size: 27,
        },
        FileChecker {
            path: FsPath::new("/dir2/0foo").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/1bar").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/5diz").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/bar").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/daz").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/foo").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/hop").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/yu").unwrap(),
            size: 0,
        },
    ];

    let sub = vec![
        FileChecker {
            path: FsPath::new("/dir2/0foo").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/1bar").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/5diz").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/bar").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/daz").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/foo").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/hop").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/yu").unwrap(),
            size: 0,
        },
    ];

    expect(fs, &FsPath::new("/").unwrap(), all)
        .join(expect(fs, &FsPath::new("/dir2/").unwrap(), sub))
        .map(|_| ())
}

fn test_get_file(_fs: &Fs) -> impl Future<Item = (), Error = FsError> {
    future::finished::<(), FsError>(())
}

fn test_delete_file(_fs: &Fs) -> impl Future<Item = (), Error = FsError> {
    future::finished::<(), FsError>(())
}

fn test_get_file_stream(_fs: &Fs) -> impl Future<Item = (), Error = FsError> {
    future::finished::<(), FsError>(())
}

fn test_write_from_stream(_fs: &Fs) -> impl Future<Item = (), Error = FsError> {
    future::finished::<(), FsError>(())
}

fn run_read_tests(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    test_list_files(&fs)
        .join(test_get_file(&fs))
        .join(test_get_file_stream(&fs))
        .map(|_| fs)
}

fn run_write_tests(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    test_delete_file(&fs)
        .join(test_write_from_stream(&fs))
        .map(|_| fs)
}

pub fn run_test(fs: Fs) -> impl Future<Item = (), Error = FsError> {
    run_read_tests(fs).and_then(run_write_tests).map(|_| ())
}

pub fn run(future: ConnectFuture) {
    let (sender, receiver) = mpsc::channel::<FsResult<()>>();

    tokio::run(future.and_then(run_test).then(move |result| {
        sender.send(result).unwrap();
        future::finished(())
    }));

    if let Err(e) = receiver.recv().unwrap() {
        panic!("{}", e);
    }
}

pub fn run_from_settings(settings: FsSettings) {
    run(Fs::new(settings));
}

pub fn cleanup(temp: TempDir) -> FsResult<()> {
    temp.close()?;

    Ok(())
}
