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

pub fn prepare_test() -> FsResult<TempDir> {
    let temp = tempdir()?;

    let mut dir = PathBuf::from(temp.path());
    dir.push("test1");
    dir.push("dir1");
    create_dir_all(dir.clone())?;

    write_file(
        &dir,
        "smallfile.txt",
        b"This is quite a short file.".iter().cloned(),
    )?;
    write_file(&dir, "largefile", ContentIterator::new(0, 100 * MB))?;
    write_file(&dir, "mediumfile", ContentIterator::new(58, 5 * MB))?;

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

    Ok(temp)
}

pub fn cleanup(temp: TempDir) -> FsResult<()> {
    temp.close()?;

    Ok(())
}

macro_rules! make_test {
    ($pkg:ident, $name:ident, $allow_incomplete:expr, $setup:expr, $cleanup:expr) => {
        #[test]
        fn $name() -> FsResult<()> {
            let temp = crate::runner::prepare_test()?;
            let (fs_future, context) = $setup(temp.path())?;
            let future = fs_future
                .and_then(crate::runner::$pkg::$name)
                .then(move |r| {
                    $cleanup(context);
                    r
                });

            match cloud_fs::utils::run_future(future) {
                Ok(Err(e)) => {
                    if e.kind() == cloud_fs::FsErrorKind::NotImplemented {
                        if $allow_incomplete {
                            eprintln!("Test attempts to use unimplemented feature: {}", e);
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
                _ => (),
            }

            crate::runner::cleanup(temp)
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
