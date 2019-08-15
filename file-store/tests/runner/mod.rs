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
                write!(f, "Unexpected storage error thrown: {}", error)
            }
            TestError::UnexpectedTransferError(error) => match error {
                TransferError::SourceError(e) => write!(f, "Unexpected source error thrown: {}", e),
                TransferError::TargetError(e) => write!(f, "Unexpected target error thrown: {}", e),
            },
            TestError::HarnessFailure(message) => f.pad(message),
            TestError::TestFailure(message) => f.pad(message),
        }
    }
}

impl From<StorageError> for TestError {
    fn from(error: StorageError) -> TestError {
        TestError::UnexpectedStorageError(error)
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
    fs_root: String,
}

impl TestContext {
    pub fn contains(&self, path: &str) -> bool {
        path != self.fs_root && path.starts_with(&self.fs_root)
    }

    pub fn get_path(&self, path: &str) -> ObjectPath {
        if !path.starts_with(&self.fs_root) {
            panic!(
                "Cannot get a path for {} with a root of {}",
                path, self.fs_root
            );
        }

        let mut target = &path[self.fs_root.len()..];
        if target.starts_with('/') {
            target = &target[1..]
        }
        ObjectPath::new(target).unwrap()
    }

    pub fn get_target(&self, path: &ObjectPath) -> PathBuf {
        let mut target = self.root.join(&self.fs_root);
        for part in path.parts() {
            target.push(part);
        }
        target
    }

    pub fn get_fs_root(&self) -> PathBuf {
        self.root.join(&self.fs_root)
    }
}

/// Creates a filesystem used for testing.
pub fn prepare_test(backend: Backend, test_root: &str) -> TestResult<TestContext> {
    let temp = tempdir().into_test_result()?;

    let mut dir = PathBuf::from(temp.path());

    let context = TestContext {
        _temp: temp,
        root: dir.clone(),
        fs_root: test_root.to_owned(),
    };

    dir.push("test1");
    dir.push("dir1");
    create_dir_all(dir.clone()).into_test_result()?;

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
    ($root:expr, $backend:expr, $pkg:ident, $name:ident, $setup:expr, $cleanup:expr) => {
        #[test]
        fn $name() {
            let result: Result<crate::runner::TestResult<()>, std::sync::mpsc::TryRecvError> =
                file_store::executor::run(async {
                    let test_context = crate::runner::prepare_test($backend, $root)?;
                    let (fs, backend_context) = $setup(&test_context).await?;
                    crate::runner::$pkg::$name(&fs, &test_context).await?;
                    $cleanup(backend_context).await?;
                    Ok(())
                });

            match result {
                Ok(Ok(())) => (),
                Ok(Err(error)) => panic!(error.to_string()),
                Err(_e) => panic!("Failed to receive test result."),
            }
        }
    };
}

macro_rules! build_tests {
    ($root:expr, $backend:expr, $setup:expr, $cleanup:expr) => {
        make_test!($root, $backend, read, test_list_objects, $setup, $cleanup);
        make_test!($root, $backend, read, test_get_object, $setup, $cleanup);
        make_test!(
            $root,
            $backend,
            read,
            test_get_file_stream,
            $setup,
            $cleanup
        );
        make_test!($root, $backend, write, test_copy_file, $setup, $cleanup);
        make_test!($root, $backend, write, test_move_file, $setup, $cleanup);
        make_test!($root, $backend, write, test_delete_object, $setup, $cleanup);
        make_test!(
            $root,
            $backend,
            write,
            test_write_file_from_stream,
            $setup,
            $cleanup
        );
    };
}
