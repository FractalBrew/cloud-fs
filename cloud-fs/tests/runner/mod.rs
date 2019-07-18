extern crate cloud_fs;
extern crate tempfile;

#[macro_use]
mod utils;
pub mod read;
pub mod write;

use std::fmt;
use std::fs::create_dir_all;
use std::iter::empty;
use std::path::PathBuf;

use tempfile::{tempdir, TempDir};

use utils::*;

use cloud_fs::backends::Backend;
use cloud_fs::*;

pub type TestResult<I> = Result<I, TestError>;

#[derive(Debug)]
pub enum TestError {
    Unexpected(FsError),
    HarnessFailure(String),
    TestFailure(String),
    NotImplemented,
}

impl TestError {
    fn from_error<E>(error: E) -> TestError
    where
        E: fmt::Display,
    {
        TestError::HarnessFailure(format!("{}", error))
    }
}

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TestError::Unexpected(error) => {
                f.write_fmt(format_args!("Unexpected FsError thrown: {}", error))
            }
            TestError::HarnessFailure(message) => f.write_str(message),
            TestError::TestFailure(message) => f.write_str(message),
            TestError::NotImplemented => f.write_str("Test attempts to use unimplemented feature."),
        }
    }
}

impl From<FsError> for TestError {
    fn from(error: FsError) -> TestError {
        TestError::Unexpected(error)
    }
}

trait IntoTestResult<O> {
    fn into_test_result(self) -> TestResult<O>;
}

impl<O, E> IntoTestResult<O> for Result<O, E>
where
    E: fmt::Display,
{
    fn into_test_result(self) -> TestResult<O> {
        self.map_err(TestError::from_error)
    }
}

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

pub fn prepare_test(backend: Backend) -> TestResult<TestContext> {
    let temp = tempdir().into_test_result()?;

    let mut dir = PathBuf::from(temp.path());
    dir.push("test1");
    dir.push("dir1");
    create_dir_all(dir.clone()).into_test_result()?;

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
        create_dir_all(em).into_test_result()?;
    }

    dir.push("dir2");
    create_dir_all(dir.clone()).into_test_result()?;
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
        fn $name() {
            async fn test() -> crate::runner::TestResult<()> {
                let test_context = crate::runner::prepare_test($backend)?;
                let (fs, backend_context) = $setup(&test_context).await?;
                crate::runner::$pkg::$name(&fs, &test_context).await?;
                $cleanup(backend_context).await;
                Ok(())
            }

            let result = cloud_fs::executor::run(Box::pin(test()));

            match result {
                Ok(Ok(())) => (),
                Ok(Err(error)) => match error {
                    crate::runner::TestError::NotImplemented => {
                        if $allow_incomplete {
                            eprintln!("{}", error)
                        } else {
                            panic!("{}", error)
                        }
                    }
                    _ => panic!("{}", error),
                },
                Err(_e) => panic!("Failed to receive test result."),
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
        /*
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
