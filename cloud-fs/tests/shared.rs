extern crate cloud_fs;
extern crate tempdir;
extern crate tokio;

use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::PathBuf;

use tempdir::TempDir;
use tokio::prelude::*;

use cloud_fs::*;

const MB: u64 = 1024 * 1024;
const GB: u64 = 1024 * 1024 * 1020;

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
    let mut buffer: Vec<u8> = Vec::with_capacity(length as usize);

    let mut iter = ContentIterator::new(seed);
    for i in 0..buffer.len() {
        match iter.next() {
            Some(val) => buffer[i] = val,
            None => return buffer,
        }
    }

    buffer
}

fn write_file(dir: &mut PathBuf, name: &str, content: &[u8]) -> FsResult<()> {
    dir.push(name);

    let mut file = File::create(dir.clone())?;
    file.write_all(content)?;
    file.sync_all()?;

    dir.pop();
    Ok(())
}

pub fn prepare_test() -> FsResult<TempDir> {
    let temp = TempDir::new("cloudfs")?;

    let mut dir = PathBuf::from(temp.path());
    dir.push("test1");
    dir.push("dir1");
    create_dir_all(dir.clone())?;

    write_file(&mut dir, "smallfile.txt", b"This is quite a short file.")?;
    write_file(&mut dir, "largefile", &build_content(0, GB))?;
    write_file(&mut dir, "mediumfile", &build_content(58, 5 * MB))?;

    dir.push("dir2");
    create_dir_all(dir.clone())?;
    write_file(&mut dir, "foo", b"")?;
    write_file(&mut dir, "bar", b"")?;
    write_file(&mut dir, "0foo", b"")?;
    write_file(&mut dir, "5diz", b"")?;
    write_file(&mut dir, "1bar", b"")?;
    write_file(&mut dir, "daz", b"")?;
    write_file(&mut dir, "hop", b"")?;
    write_file(&mut dir, "yu", b"")?;

    Ok(temp)
}

struct FileChecker {
    path: FsPath,
    size: Option<u64>,
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

fn test_list_files(fs: &Fs) -> impl Future<Item = (), Error = ()> {
    fn expect(
        fs: &Fs,
        path: &FsPath,
        files: Vec<FileChecker>,
    ) -> impl Future<Item = (), Error = ()> {
        fs.list_files(path)
            .and_then(|s| s.collect())
            .map(move |results| {
                let mut sorted = results.clone();
                sorted.sort();
                FileChecker::check_files(sorted, files);
            })
            .map_err(|e| panic!(e))
    }

    let all = vec![
        FileChecker {
            path: FsPath::new("/dir2/0foo").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/1bar").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/5diz").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/bar").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/daz").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/foo").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/hop").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/yu").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/largefile.txt").unwrap(),
            size: Some(GB),
        },
        FileChecker {
            path: FsPath::new("/mediumfile.txt").unwrap(),
            size: Some(5 * MB),
        },
        FileChecker {
            path: FsPath::new("/smallfile.txt").unwrap(),
            size: Some(7),
        },
    ];

    let sub = vec![
        FileChecker {
            path: FsPath::new("/dir2/0foo").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/1bar").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/5diz").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/bar").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/daz").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/foo").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/hop").unwrap(),
            size: Some(0),
        },
        FileChecker {
            path: FsPath::new("/dir2/yu").unwrap(),
            size: Some(0),
        },
    ];

    expect(fs, &FsPath::new("/").unwrap(), all)
        .join(expect(fs, &FsPath::new("/dir2").unwrap(), sub))
        .map(|_| ())
}

fn test_get_file(fs: &Fs) -> impl Future<Item = (), Error = ()> {
    future::finished::<(), ()>(())
}

fn test_delete_file(fs: &Fs) -> impl Future<Item = (), Error = ()> {
    future::finished::<(), ()>(())
}

fn test_get_file_stream(fs: &Fs) -> impl Future<Item = (), Error = ()> {
    future::finished::<(), ()>(())
}

fn test_write_from_stream(fs: &Fs) -> impl Future<Item = (), Error = ()> {
    future::finished::<(), ()>(())
}

fn run_read_tests(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    test_list_files(&fs)
        .map_err(|e| panic!(e))
        .join(test_get_file(&fs).map_err(|e| panic!(e)))
        .join(test_get_file_stream(&fs).map_err(|e| panic!(e)))
        .map(|_| fs)
}

fn run_write_tests(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    test_delete_file(&fs)
        .map_err(|e| panic!(e))
        .join(test_write_from_stream(&fs).map_err(|e| panic!(e)))
        .map(|_| fs)
}

pub fn run_test(fs: Fs) -> impl Future<Item = (), Error = FsError> {
    run_read_tests(fs).and_then(run_write_tests).map(|_| ())
}

pub fn cleanup(temp: TempDir) -> FsResult<()> {
    temp.close()?;

    Ok(())
}
