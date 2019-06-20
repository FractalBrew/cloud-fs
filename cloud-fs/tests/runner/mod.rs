extern crate cloud_fs;
extern crate tempfile;
extern crate tokio;

#[macro_use]
mod utils;
pub mod read;
pub mod write;

use std::fs::create_dir_all;
use std::iter::empty;
use std::path::PathBuf;

use tempfile::{tempdir, TempDir};

use utils::*;

use cloud_fs::*;

pub struct TestContext {
    temp: TempDir,
    root: PathBuf,
}

impl TestContext {
    pub fn get_target(&self, path: &FsPath) -> PathBuf {
        let mut target = self.root.clone();
        target.push(
            FsPath::new("/")
                .unwrap()
                .relative(path)
                .unwrap()
                .as_std_path(),
        );
        target
    }

    pub fn get_root(&self) -> PathBuf {
        self.root.clone()
    }

    pub fn cleanup(self) -> FsResult<()> {
        Ok(self.temp.close()?)
    }
}

pub fn prepare_test() -> FsResult<TestContext> {
    let temp = tempdir()?;

    let mut dir = PathBuf::from(temp.path());
    dir.push("test1");
    dir.push("dir1");
    create_dir_all(dir.clone())?;

    let context = TestContext {
        temp,
        root: dir.clone(),
    };

    write_file(
        &dir,
        "smallfile.txt",
        b"This is quite a short file.".iter().cloned(),
    )?;
    write_file(&dir, "largefile", ContentIterator::new(0, 100 * MB))?;
    write_file(&dir, "mediumfile", ContentIterator::new(58, 5 * MB))?;

    let mut em = dir.clone();
    em.push("dir3");
    create_dir_all(em)?;

    dir.push("dir2");
    create_dir_all(dir.clone())?;
    write_file(&dir, "foo", empty())?;
    write_file(&dir, "bar", empty())?;
    write_file(&dir, "0foo", empty())?;
    write_file(&dir, "5diz", empty())?;
    write_file(&dir, "1bar", empty())?;
    write_file(&dir, "daz", ContentIterator::new(72, 300))?;
    write_file(&dir, "hop", empty())?;
    write_file(&dir, "yu", empty())?;

    Ok(context)
}

macro_rules! make_test {
    ($pkg:ident, $name:ident, $allow_incomplete:expr, $setup:expr, $cleanup:expr) => {
        #[test]
        fn $name() -> FsResult<()> {
            let test_context = crate::runner::prepare_test()?;
            let (fs_future, context) = $setup(&test_context)?;
            let future = fs_future
                .and_then(|fs| crate::runner::$pkg::$name(fs, test_context))
                .then(move |r| {
                    $cleanup(context);
                    r
                });

            match cloud_fs::utils::run_future(future) {
                Ok(Ok((_, test_context))) => test_context.cleanup(),
                Ok(Err(e)) => {
                    if e.kind() == cloud_fs::FsErrorKind::NotImplemented {
                        if $allow_incomplete {
                            eprintln!("Test attempts to use unimplemented feature: {}", e);
                            Ok(())
                        } else {
                            panic!("Test attempts to use unimplemented feature: {}", e);
                        }
                    } else {
                        panic!("{}", e);
                    }
                }
                Err(e) => panic!(
                    "{}::{} never completed: {}",
                    stringify!($pkg),
                    stringify!($name),
                    e
                ),
            }
        }
    };
}

macro_rules! build_tests {
    ($name:expr, $allow_incomplete:expr, $setup:expr, $cleanup:expr) => {
        make_test!(read, test_list_files, $allow_incomplete, $setup, $cleanup);
        make_test!(read, test_get_file, $allow_incomplete, $setup, $cleanup);
        make_test!(
            read,
            test_get_file_stream,
            $allow_incomplete,
            $setup,
            $cleanup
        );

        make_test!(write, test_delete_file, $allow_incomplete, $setup, $cleanup);
        make_test!(
            write,
            test_write_from_stream,
            $allow_incomplete,
            $setup,
            $cleanup
        );
    };
}
