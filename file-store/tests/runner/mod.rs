// Copyright 2019 Dave Townsend
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[macro_use]
mod utils;
pub mod read;
pub mod write;

use std::fmt;
use std::fs::create_dir_all;
use std::future::Future;
use std::iter::empty;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use filetime::{set_file_mtime, FileTime};
use futures::future::FutureExt;
use std::sync::Once;
use tempfile::{tempdir, TempDir};
use tokio::executor::spawn as tokio_spawn;
use tokio::runtime::current_thread::Runtime;
use tokio::sync::oneshot;

use utils::*;

use file_store::backends::Backend;
use file_store::*;

pub static INIT: Once = Once::new();

#[allow(non_snake_case)]
pub fn LARGE_FILE_MODIFIED() -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(1_568_259_129)
}

#[allow(non_snake_case)]
pub fn SMALL_FILE_MODIFIED() -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(1_603_257_714)
}

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

/// Runs a future on the existing runtime blocking until it completes.
pub fn run<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    let mut runtime = Runtime::new().unwrap();
    runtime.block_on(future)
}

/// Spawns a future on the existing runtime returning a future that resolves to
/// its result.
#[allow(dead_code)]
pub fn spawn<F>(future: F) -> impl Future<Output = Result<F::Output, oneshot::error::RecvError>>
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    let (sender, receiver) = oneshot::channel::<F::Output>();
    tokio_spawn(future.map(move |r| {
        if sender.send(r).is_err() {
            panic!("Failed to complete.")
        }
    }));
    receiver
}

pub struct TestContext {
    // Needed to keep the temp dir alive until the context is dropped.
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

    let file = write_file(
        &dir,
        "smallfile.txt",
        b"This is quite a short file.".iter().cloned(),
    )?;
    set_file_mtime(file, FileTime::from_system_time(SMALL_FILE_MODIFIED())).into_test_result()?;
    let file = write_file(&dir, "largefile", ContentIterator::new(0, 100 * MB))?;
    set_file_mtime(file, FileTime::from_system_time(LARGE_FILE_MODIFIED())).into_test_result()?;
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
            crate::runner::INIT.call_once(env_logger::init);
            let result: crate::runner::TestResult<()> = crate::runner::run(async {
                let test_context = crate::runner::prepare_test($backend, $root)?;
                let (fs, backend_context) = $setup(&test_context).await?;
                crate::runner::$pkg::$name(&fs, &test_context).await?;
                $cleanup(backend_context).await?;
                Ok(())
            });

            match result {
                Ok(()) => (),
                Err(error) => panic!(error.to_string()),
            }
        }
    };
}

macro_rules! build_tests {
    ($root:expr, $backend:expr, $setup:expr, $cleanup:expr) => {
        make_test!($root, $backend, read, test_list_objects, $setup, $cleanup);
        make_test!($root, $backend, read, test_list_directory, $setup, $cleanup);
        make_test!($root, $backend, read, test_get_object, $setup, $cleanup);
        make_test!(
            $root,
            $backend,
            read,
            test_get_file_stream,
            $setup,
            $cleanup
        );
        make_test!(
            $root,
            $backend,
            read,
            test_simultaneous_download,
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
