extern crate cloud_fs;
extern crate tempfile;

#[macro_use]
mod utils;
pub mod read;
//pub mod write;

use std::fs::create_dir_all;
use std::iter::empty;
use std::path::PathBuf;

use tempfile::{tempdir, TempDir};

use utils::*;

use cloud_fs::backends::Backend;
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
}

pub fn prepare_test(backend: Backend) -> FsResult<TestContext> {
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

    if backend == Backend::File {
        let mut em = dir.clone();
        em.push("dir3");
        create_dir_all(em)?;
    }

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
    ($backend:expr, $pkg:ident, $name:ident, $allow_incomplete:expr, $setup:expr, $cleanup:expr) => {
        #[test]
        fn $name() -> FsResult<()> {
            async fn runner(test_context: TestContext) -> FsResult<()> {
                let (fs, backend_context) = $setup(&test_context).await?;
                crate::runner::$pkg::$name(&fs, &test_context).await?;
                $cleanup(backend_context).await;
                Ok(())
            }

            let test_context = crate::runner::prepare_test($backend)?;

            let result = cloud_fs::executor::run(Box::pin(runner(test_context)));

            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => match error.kind() {
                    cloud_fs::FsErrorKind::NotImplemented => {
                        if $allow_incomplete {
                            eprintln!(
                                "Test for {} attempts to use unimplemented feature: {}",
                                $backend, error
                            );
                            Ok(())
                        } else {
                            panic!(
                                "Test for {} attempts to use unimplemented feature: {}",
                                $backend, error
                            );
                        }
                    }
                    cloud_fs::FsErrorKind::TestFailure => {
                        panic!("{}", error);
                    }
                    _ => Err(error),
                },
                Err(_) => panic!("Failed to receive test result."),
            }
        }
    };
}

macro_rules! build_tests {
    ($backend:expr, $allow_incomplete:expr, $setup:expr, $cleanup:expr) => {
        make_test!(
            $backend,
            read,
            test_list_files,
            $allow_incomplete,
            $setup,
            $cleanup
        );
        make_test!(
            $backend,
            read,
            test_get_file,
            $allow_incomplete,
            $setup,
            $cleanup
        );
        /*
                make_test!(
                    $backend,
                    read,
                    test_get_file_stream,
                    $allow_incomplete,
                    $setup,
                    $cleanup
                );

                make_test!(
                    $backend,
                    write,
                    test_delete_file,
                    $allow_incomplete,
                    $setup,
                    $cleanup
                );
                make_test!(
                    $backend,
                    write,
                    test_write_from_stream,
                    $allow_incomplete,
                    $setup,
                    $cleanup
                );
        */
    };
}
