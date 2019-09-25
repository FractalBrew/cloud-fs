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

use std::fs::symlink_metadata;
use std::future::Future;
use std::iter::empty;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};

use super::utils::*;
use super::*;

use file_store::*;

const MAX_TIME_DIFFERENCE: u64 = 1;

fn test_file_matches(target: &Path, object: Object) -> TestResult<()> {
    let meta = symlink_metadata(&target).map_err(TestError::from_error)?;

    match object.object_type() {
        ObjectType::File => {
            test_assert!(
                meta.is_file(),
                "Should have seen the correct file type for {}.",
                object.path()
            );

            test_assert_eq!(
                object.len(),
                meta.len(),
                "Should have seen the correct size."
            );

            if let Some(expected_modified) = object.modified() {
                let modified = meta.modified().map_err(TestError::from_error)?;

                let modified_difference = if modified > expected_modified {
                    modified
                        .duration_since(expected_modified)
                        .map_err(TestError::from_error)?
                } else {
                    expected_modified
                        .duration_since(modified)
                        .map_err(TestError::from_error)?
                };

                if modified_difference.as_secs() > MAX_TIME_DIFFERENCE {
                    test_fail!(
                        "Should have seen the right modification time. Time differed by {} seconds.",
                        modified_difference.as_secs()
                    );
                }
            }
        }
        ObjectType::Directory => {
            test_assert!(
                meta.is_dir(),
                "Should have seen the correct file type for {}.",
                object.path()
            );

            test_assert_eq!(object.len(), 0, "Directories should have no size.");
            test_assert_eq!(
                object.modified(),
                None,
                "Directories should have no modification time."
            );
        }
        ObjectType::Symlink => {
            test_assert!(
                !meta.is_file() && !meta.is_dir(),
                "Should have seen the correct file type for {}.",
                object.path()
            );

            test_assert_eq!(object.len(), 0, "Links should have no size.");
            test_assert_eq!(
                object.modified(),
                None,
                "Links should have no modification time."
            );
        }
        ObjectType::Unknown => {
            test_fail!(
                "Should have seen the correct file type for {}.",
                object.path()
            );
        }
    }

    Ok(())
}

pub async fn test_list_objects(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_list<'a>(
        fs: &'a FileStore,
        context: &'a TestContext,
        path: &'static str,
        files: Vec<&'static str>,
    ) -> TestResult<()> {
        let mut expected_paths: Vec<ObjectPath> = files
            .iter()
            .filter_map(|path| {
                if context.contains(path) {
                    Some(context.get_path(path))
                } else {
                    None
                }
            })
            .collect();

        let mut results = fs
            .list_objects(context.get_path(path))
            .await?
            .try_collect::<Vec<Object>>()
            .await?;
        results.sort();
        expected_paths.sort();

        test_assert_eq!(
            results.len(),
            expected_paths.len(),
            "Should have seen the right number of results.",
        );

        while !expected_paths.is_empty() {
            let result = results.remove(0);
            let path = expected_paths.remove(0);

            test_assert_eq!(&result.path(), &path, "Should have seen the right path.");
            test_file_matches(&context.get_target(&path), result)?;
        }

        Ok(())
    }

    let mut allfiles = vec![
        "test1/dir1/largefile",
        "test1/dir1/mediumfile",
        "test1/dir1/smallfile.txt",
        "test1/dir1/dir2/0foo",
        "test1/dir1/dir2/1bar",
        "test1/dir1/dir2/5diz",
        "test1/dir1/dir2/bar",
        "test1/dir1/dir2/daz",
        "test1/dir1/dir2/foo",
        "test1/dir1/dir2/hop",
        "test1/dir1/dir2/yu",
    ];

    if fs.backend_type() == Backend::File {
        allfiles.extend(vec![
            "test1",
            "test1/dir1",
            "test1/dir1/dir2",
            "test1/dir1/maybedir",
            "test1/dir1/maybedir/foo",
            "test1/dir1/maybedir/bar",
            "test1/dir1/maybedir/baz",
            "test1/dir1/maybedir/foobar",
            "test1/dir1/maybedir/foobar/foo",
            "test1/dir1/maybedir/foobar/bar",
        ])
    } else {
        allfiles.extend(vec!["test1/dir1/maybedir"])
    }

    test_list(fs, context, "test1/dir1", allfiles).await?;

    let mut prefixed = vec![
        "test1/dir1/dir2/0foo",
        "test1/dir1/dir2/1bar",
        "test1/dir1/dir2/5diz",
        "test1/dir1/dir2/bar",
        "test1/dir1/dir2/daz",
        "test1/dir1/dir2/foo",
        "test1/dir1/dir2/hop",
        "test1/dir1/dir2/yu",
    ];

    test_list(fs, context, "test1/dir1/dir2/", prefixed.clone()).await?;

    if fs.backend_type() == Backend::File {
        prefixed.extend(vec!["test1/dir1/dir2"]);
    }

    test_list(fs, context, "test1/dir1/dir2", prefixed.clone()).await?;

    test_list(fs, context, "test1/dir1/dir", prefixed.clone()).await?;

    Ok(())
}

pub async fn test_list_directory(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_list<'a>(
        fs: &'a FileStore,
        context: &'a TestContext,
        path: &'static str,
        files: Vec<&'static str>,
    ) -> TestResult<()> {
        if !context.contains(path) {
            return Ok(());
        }

        let mut expected_paths: Vec<ObjectPath> = files
            .iter()
            .filter_map(|path| {
                if context.contains(path) {
                    Some(context.get_path(path))
                } else {
                    None
                }
            })
            .collect();

        let mut results = fs
            .list_directory(context.get_path(path))
            .await?
            .try_collect::<Vec<Object>>()
            .await?;
        results.sort();
        expected_paths.sort();

        test_assert_eq!(
            results.len(),
            expected_paths.len(),
            "Should have seen the right number of results.",
        );

        while !expected_paths.is_empty() {
            let result = results.remove(0);
            let path = expected_paths.remove(0);

            test_assert_eq!(&result.path(), &path, "Should have seen the right path.");
            test_file_matches(&context.get_target(&path), result)?;
        }

        Ok(())
    }

    let base = vec!["test1"];
    test_list(fs, context, "test1", base).await?;

    let dir1 = vec![
        "test1/dir1/largefile",
        "test1/dir1/mediumfile",
        "test1/dir1/smallfile.txt",
        "test1/dir1/dir2",
        "test1/dir1/maybedir",
    ];

    test_list(fs, context, "test1/dir1", dir1).await?;

    let dir2 = vec![
        "test1/dir1/dir2/foo",
        "test1/dir1/dir2/bar",
        "test1/dir1/dir2/0foo",
        "test1/dir1/dir2/5diz",
        "test1/dir1/dir2/1bar",
        "test1/dir1/dir2/daz",
        "test1/dir1/dir2/hop",
        "test1/dir1/dir2/yu",
    ];

    test_list(fs, context, "test1/dir1/dir2", dir2).await?;

    Ok(())
}

pub async fn test_get_object(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_pass(fs: &FileStore, context: &TestContext, path: &str) -> TestResult<()> {
        let path = context.get_path(path);
        let result = fs.get_object(path.clone()).await?;

        test_assert_eq!(&result.path(), &path, "Should have seen the right path.");
        test_file_matches(&context.get_target(&path), result)?;

        Ok(())
    }

    async fn test_fail(fs: &FileStore, context: &TestContext, path: &str) -> TestResult<()> {
        let fspath = context.get_path(path);
        let result = fs.get_object(fspath.clone()).await;
        test_assert!(result.is_err(), "Should have failed to find {}.", fspath);
        if let Err(e) = result {
            test_assert_eq!(
                e.kind(),
                StorageErrorKind::NotFound(fspath),
                "Should have returned a NotFound error."
            );
        }

        Ok(())
    }

    test_pass(fs, context, "test1/dir1/largefile").await?;
    test_pass(fs, context, "test1/dir1/smallfile.txt").await?;
    test_pass(fs, context, "test1/dir1/dir2/0foo").await?;
    test_pass(fs, context, "test1/dir1/dir2/daz").await?;

    test_fail(fs, context, "test1/dir1/daz").await?;
    test_fail(fs, context, "test1/dir1/foo/bar").await?;

    if fs.backend_type() == Backend::File {
        test_pass(fs, context, "test1/dir1/maybedir").await?;
    } else {
        test_fail(fs, context, "test1/dir1/dir2").await?;
        test_pass(fs, context, "test1/dir1/maybedir").await?;
    }

    Ok(())
}

pub async fn test_get_file_stream(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_pass<I>(
        fs: &FileStore,
        context: &TestContext,
        path: &str,
        mut data: I,
    ) -> TestResult<()>
    where
        I: Iterator<Item = u8>,
    {
        let target = context.get_path(path);
        let mut stream = Box::pin(fs.get_file_stream(target).await?);

        let mut pos: usize = 0;
        loop {
            let buf = stream.next().await;
            match buf {
                Some(Ok(buffer)) => {
                    for x in 0..buffer.len() {
                        match data.next() {
                            Some(b) => test_assert_eq!(
                                buffer[x],
                                b,
                                "Data should have matched at position {}.",
                                pos
                            ),
                            None => test_fail!("Ran out of expected data as position {}.", pos),
                        }
                        pos += 1;
                    }
                }
                Some(Err(e)) => {
                    return Err(e.into());
                }
                None => {
                    test_assert_eq!(
                        data.next(),
                        None,
                        "Expected data should have ended at position {}.",
                        pos
                    );
                    break;
                }
            }
        }

        Ok(())
    }

    async fn test_fail(fs: &FileStore, context: &TestContext, path: &str) -> TestResult<()> {
        let target = context.get_path(path);
        let result = fs.get_file_stream(target.clone()).await;
        test_assert!(result.is_err());
        if let Err(e) = result {
            test_assert_eq!(e.kind(), StorageErrorKind::NotFound(target));
        }

        Ok(())
    }

    test_pass(
        fs,
        context,
        "test1/dir1/smallfile.txt",
        b"This is quite a short file.".iter().cloned(),
    )
    .await?;
    test_pass(
        fs,
        context,
        "test1/dir1/largefile",
        ContentIterator::new(0, 100 * MB),
    )
    .await?;
    test_pass(fs, context, "test1/dir1/dir2/bar", empty()).await?;
    test_pass(
        fs,
        context,
        "test1/dir1/dir2/daz",
        ContentIterator::new(72, 300),
    )
    .await?;

    test_fail(fs, context, "test1/dir1/dir2").await?;
    test_fail(fs, context, "test1/dir1/daz").await?;
    test_fail(fs, context, "test1/dir1/foo/bar").await?;
    test_fail(fs, context, "test1/dir1/dir2/gaz").await?;

    Ok(())
}

struct Wrapper {
    inner: Pin<Box<dyn Future<Output = TestResult<()>> + Send + 'static>>,
}

impl Wrapper {
    pub fn new<F>(future: F) -> Wrapper
    where
        F: Future<Output = TestResult<()>> + Send + 'static,
    {
        Wrapper {
            inner: Box::pin(future),
        }
    }
}

impl Future for Wrapper {
    type Output = TestResult<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<TestResult<()>> {
        self.inner.as_mut().poll(cx)
    }
}

pub async fn test_simultaneous_download(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn get_checker<I>(fs: FileStore, path: ObjectPath, mut data: I) -> TestResult<()>
    where
        I: Iterator<Item = u8> + Send + 'static,
    {
        let mut stream = fs.get_file_stream(path).await?;
        let mut pos = 0;
        loop {
            let buf = stream.next().await;
            match buf {
                Some(Ok(buffer)) => {
                    for x in 0..buffer.len() {
                        match data.next() {
                            Some(b) => test_assert_eq!(
                                buffer[x],
                                b,
                                "Data should have matched at position {}.",
                                pos
                            ),
                            None => test_fail!("Ran out of expected data as position {}.", pos),
                        }
                        pos += 1;
                    }
                }
                Some(Err(e)) => {
                    return Err(e.into());
                }
                None => {
                    test_assert_eq!(
                        data.next(),
                        None,
                        "Expected data should have ended at position {}.",
                        pos
                    );
                    break;
                }
            }
        }

        Ok(())
    }

    let mut results = FuturesUnordered::<Wrapper>::new();

    results.push(Wrapper::new(get_checker(
        fs.clone(),
        context.get_path("test1/dir1/smallfile.txt"),
        b"This is quite a short file.".iter().cloned(),
    )));
    results.push(Wrapper::new(get_checker(
        fs.clone(),
        context.get_path("test1/dir1/largefile"),
        ContentIterator::new(0, 100 * MB),
    )));

    while let Some(result) = results.next().await {
        if let Err(e) = result {
            return Err(e);
        }
    }

    Ok(())
}
