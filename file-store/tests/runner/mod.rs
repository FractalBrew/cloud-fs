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

use file_store::backends::Backend;
use file_store::*;

pub type TestResult<I> = Result<I, TestError>;

#[derive(Debug)]
pub enum TestError {
    UnexpectedStorageError(StorageError),
    UnexpectedPathError(ObjectPathError),
    UnexpectedTransferError(TransferError),
    HarnessFailure(String),
    TestFailure(String),
}

impl TestError {
    fn from_error<E>(error: E) -> TestError
    where
        E: fmt::Display,
    {
        TestError::HarnessFailure(error.to_string())
    }
}

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TestError::UnexpectedStorageError(error) => {
                f.write_fmt(format_args!("Unexpected storage error thrown: {}", error))
            }
            TestError::UnexpectedPathError(error) => {
                f.write_fmt(format_args!("Unexpected path error thrown: {}", error))
            }
            TestError::UnexpectedTransferError(error) => match error {
                TransferError::SourceError(e) => {
                    f.write_fmt(format_args!("Unexpected source error thrown: {}", e))
                }
                TransferError::TargetError(e) => {
                    f.write_fmt(format_args!("Unexpected target error thrown: {}", e))
                }
            },
            TestError::HarnessFailure(message) => f.write_str(message),
            TestError::TestFailure(message) => f.write_str(message),
        }
    }
}

impl From<StorageError> for TestError {
    fn from(error: StorageError) -> TestError {
        TestError::UnexpectedStorageError(error)
    }
}

impl From<ObjectPathError> for TestError {
    fn from(error: ObjectPathError) -> TestError {
        TestError::UnexpectedPathError(error)
    }
}

impl From<TransferError> for TestError {
    fn from(error: TransferError) -> TestError {
        TestError::UnexpectedTransferError(error)
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
    _temp: TempDir,
    root: PathBuf,
}

impl TestContext {
    pub fn get_target(&self, path: &ObjectPath) -> PathBuf {
        let mut target = self.get_root();
        for part in path.parts() {
            target.push(part);
        }
        target
    }

    pub fn get_root(&self) -> PathBuf {
        self.root.clone()
    }
}

/// Creates a filesystem used for testing.
///
/// The filesystem looks like this:
///
/// ```
/// test1/dir1/smallfile.txt
/// test1/dir1/largefile
/// test1/dir1/mediumfile
/// test1/dir2/foo
/// test1/dir2/bar
/// test1/dir2/0foo
/// test1/dir2/5diz
/// test1/dir2/1bar
/// test1/dir2/daz
/// test1/dir2/hop
/// test1/dir2/yu
///
/// For File backend only:
///
/// test1/dir3/
///
/// Created by write tests:
///
/// test1/foobar
/// test1/dir3
/// test1/dir2/daz
///
/// ```
pub fn prepare_test(backend: Backend) -> TestResult<TestContext> {
    let temp = tempdir().into_test_result()?;

    let mut dir = PathBuf::from(temp.path());
    dir.push("test1");
    dir.push("dir1");
    create_dir_all(dir.clone()).into_test_result()?;

    let context = TestContext {
        _temp: temp,
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
        em.push("maybedir");
        create_dir_all(&em).into_test_result()?;
        write_file(&em, "foo", empty())?;
        write_file(&em, "bar", empty())?;
        write_file(&em, "baz", empty())?;

        em.push("foobar");
        create_dir_all(&em).into_test_result()?;
        write_file(&em, "foo", empty())?;
        write_file(&em, "bar", empty())?;
    } else {
        write_file(&dir, "maybedir", empty())?;
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
    ($backend:expr, $pkg:ident, $name:ident, $setup:expr, $cleanup:expr) => {
        #[test]
        fn $name() {
            let result: Result<crate::runner::TestResult<()>, std::sync::mpsc::TryRecvError> =
                file_store::executor::run(async {
                    let test_context = crate::runner::prepare_test($backend)?;
                    let (fs, backend_context) = $setup(&test_context).await?;
                    crate::runner::$pkg::$name(&fs, &test_context).await?;
                    $cleanup(backend_context).await?;
                    Ok(())
                });

            match result {
                Ok(Ok(())) => (),
                Ok(Err(error)) => panic!("{}", error),
                Err(_e) => panic!("Failed to receive test result."),
            }
        }
    };
}

macro_rules! build_tests {
    ($backend:expr, $setup:expr, $cleanup:expr) => {
        make_test!($backend, read, test_list_objects, $setup, $cleanup);
        make_test!($backend, read, test_get_object, $setup, $cleanup);
        make_test!($backend, read, test_get_file_stream, $setup, $cleanup);
        make_test!($backend, write, test_copy_file, $setup, $cleanup);
        make_test!($backend, write, test_move_file, $setup, $cleanup);
        make_test!($backend, write, test_delete_object, $setup, $cleanup);
        make_test!(
            $backend,
            write,
            test_write_file_from_stream,
            $setup,
            $cleanup
        );
    };
}
