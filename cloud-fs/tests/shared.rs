extern crate cloud_fs;
extern crate tempfile;
extern crate tokio;

use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::PathBuf;

use tempfile::{tempdir, TempDir};
use tokio::prelude::*;

use cloud_fs::*;

const MB: u64 = 1024 * 1024;

struct ContentIterator {
    value: u8,
}

impl ContentIterator {
    fn new(seed: u8) -> ContentIterator {
        ContentIterator { value: seed }
    }
}

impl Iterator for ContentIterator {
    type Item = u8;

    fn next(&mut self) -> Option<u8> {
        let new_value = self.value;
        let (new_value, _) = new_value.overflowing_add(27);
        let (new_value, _) = new_value.overflowing_mul(9);
        let (new_value, _) = new_value.overflowing_add(5);
        self.value = new_value;
        Some(self.value)
    }
}

fn build_content(seed: u8, length: u64) -> Vec<u8> {
    let mut buffer: Vec<u8> = vec![0; length as usize];

    let mut iter = ContentIterator::new(seed);
    for i in 0..buffer.len() {
        match iter.next() {
            Some(val) => buffer[i] = val,
            None => unreachable!(),
        }
    }

    buffer
}

fn write_file(dir: &PathBuf, name: &str, content: &[u8]) -> FsResult<()> {
    let mut target = dir.clone();
    target.push(name);

    let mut file = File::create(target)?;
    file.write_all(content)?;
    file.sync_all()?;

    Ok(())
}

pub fn prepare_test() -> FsResult<TempDir> {
    let temp = tempdir()?;

    let mut dir = PathBuf::from(temp.path());
    dir.push("test1");
    dir.push("dir1");
    create_dir_all(dir.clone())?;

    write_file(&dir, "smallfile.txt", b"This is quite a short file.")?;
    write_file(&dir, "largefile", &build_content(0, 100 * MB))?;
    write_file(&dir, "mediumfile", &build_content(58, 5 * MB))?;

    dir.push("dir2");
    create_dir_all(dir.clone())?;
    write_file(&dir, "foo", b"")?;
    write_file(&dir, "bar", b"")?;
    write_file(&dir, "0foo", b"")?;
    write_file(&dir, "5diz", b"")?;
    write_file(&dir, "1bar", b"")?;
    write_file(&dir, "daz", b"")?;
    write_file(&dir, "hop", b"")?;
    write_file(&dir, "yu", b"")?;

    Ok(temp)
}

struct FileChecker {
    path: FsPath,
    size: u64,
}

impl FileChecker {
    fn check(&self, file: &FsFile) {
        assert_eq!(file.path(), &self.path, "Should have the expected name.");
        assert_eq!(
            file.size(),
            self.size,
            "Should have the expected size for {}",
            &self.path.to_string()
        );
    }

    fn check_files(found: Vec<FsFile>, expected: Vec<FileChecker>) {
        for (file, checker) in found.iter().zip(expected.iter()) {
            checker.check(file);
        }

        assert_eq!(
            found.len(),
            expected.len(),
            "Should have seen the right number of results."
        );
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
            .map(move |mut results| {
                results.sort();
                FileChecker::check_files(results, files);
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

pub fn cleanup(temp: TempDir) -> FsResult<()> {
    temp.close()?;

    Ok(())
}
