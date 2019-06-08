extern crate cloud_fs;
extern crate tokio;
extern crate tempdir;

use std::path::PathBuf;
use std::fs::{File, create_dir_all};
use std::io::Write;

use tempdir::TempDir;
use tokio::prelude::*;

use cloud_fs::*;

const MB: usize = 1024 * 1024;
const GB: usize = 1024 * 1024 * 1020;

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

fn build_content(seed: u8, length: usize) -> Vec<u8> {
    let mut buffer: Vec<u8> = Vec::with_capacity(length);

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

fn test_list_files(fs: &Fs) -> impl Future<Item = (), Error = ()> {
    fn expect(fs: &Fs, path: &FsPath, files: Vec<FsFile>) -> impl Future<Item = (), Error = ()> {
        fs.list_files(path)
            .collect()
            .map(move |results| {
                let mut sorted = results.clone();
                sorted.sort();
                assert_eq!(sorted, files);
            })
            .map_err(|e| panic!(e))
    }

    let all = vec![
        FsFile::new(FsPath::new("/largefile.txt").unwrap(), Some(GB)),
        FsFile::new(FsPath::new("/mediumfile.txt").unwrap(), Some(5 * MB)),
        FsFile::new(FsPath::new("/smallfile.txt").unwrap(), Some(7)),
        FsFile::new(FsPath::new("/dir2/0foo").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/1bar").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/5diz").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/bar").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/daz").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/foo").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/hop").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/yu").unwrap(), Some(0)),
    ];

    let sub = vec![
        FsFile::new(FsPath::new("/dir2/0foo").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/1bar").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/5diz").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/bar").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/daz").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/foo").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/hop").unwrap(), Some(0)),
        FsFile::new(FsPath::new("/dir2/yu").unwrap(), Some(0)),
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
    test_list_files(&fs).map_err(|e| panic!(e))
        .join(test_get_file(&fs).map_err(|e| panic!(e)))
        .join(test_get_file_stream(&fs).map_err(|e| panic!(e)))
        .map(|_| fs)
}

fn run_write_tests(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    test_delete_file(&fs).map_err(|e| panic!(e))
        .join(test_write_from_stream(&fs).map_err(|e| panic!(e)))
        .map(|_| fs)
}

pub fn run_test(settings: FsSettings) -> FsResult<()> {
    let future = Fs::new(settings)
        .and_then(run_read_tests)
        .and_then(run_write_tests)
        .map(|_| ())
        .map_err(|e| panic!(e));

    tokio::run(future);

    Ok(())
}

pub fn cleanup(temp: TempDir) -> FsResult<()> {
    temp.close()?;

    Ok(())
}
