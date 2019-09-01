use std::iter::empty;

use futures::stream::{StreamExt, TryStreamExt};

use super::utils::*;
use super::*;

use file_store::*;

fn compare_file(
    file: &Object,
    expected_path: ObjectPath,
    expected_type: ObjectType,
    expected_size: u64,
) -> TestResult<()> {
    test_assert_eq!(
        file.path(),
        expected_path.clone(),
        "Should have the expected path."
    );
    test_assert_eq!(
        file.object_type(),
        expected_type,
        "Should have the expected type."
    );
    test_assert_eq!(
        file.size(),
        expected_size,
        "Should have the expected size for {}",
        expected_path,
    );
    Ok(())
}

pub async fn test_list_objects(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_list<'a>(
        fs: &'a FileStore,
        context: &'a TestContext,
        path: &'static str,
        files: Vec<(&'static str, ObjectType, u64)>,
    ) -> TestResult<()> {
        let mut expected_files: Vec<(ObjectPath, ObjectType, u64)> = files
            .iter()
            .filter_map(|info| {
                if context.contains(info.0) {
                    let path = context.get_path(info.0);
                    Some((path, info.1, info.2))
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
        expected_files.sort();

        test_assert_eq!(
            results.len(),
            expected_files.len(),
            "Should have seen the right number of results.",
        );

        for _ in 0..expected_files.len() {
            let result = results.remove(0);
            let (path, file_type, size) = expected_files.remove(0);
            compare_file(&result, path, file_type, size)?;
        }

        Ok(())
    }

    let mut allfiles = vec![
        ("test1/dir1/largefile", ObjectType::File, 100 * MB),
        ("test1/dir1/mediumfile", ObjectType::File, 5 * MB),
        ("test1/dir1/smallfile.txt", ObjectType::File, 27),
        ("test1/dir1/dir2/0foo", ObjectType::File, 0),
        ("test1/dir1/dir2/1bar", ObjectType::File, 0),
        ("test1/dir1/dir2/5diz", ObjectType::File, 0),
        ("test1/dir1/dir2/bar", ObjectType::File, 0),
        ("test1/dir1/dir2/daz", ObjectType::File, 300),
        ("test1/dir1/dir2/foo", ObjectType::File, 0),
        ("test1/dir1/dir2/hop", ObjectType::File, 0),
        ("test1/dir1/dir2/yu", ObjectType::File, 0),
    ];

    if fs.backend_type() == Backend::File {
        allfiles.extend(vec![
            ("test1", ObjectType::Directory, 0),
            ("test1/dir1", ObjectType::Directory, 0),
            ("test1/dir1/dir2", ObjectType::Directory, 0),
            ("test1/dir1/maybedir", ObjectType::Directory, 0),
            ("test1/dir1/maybedir/foo", ObjectType::File, 0),
            ("test1/dir1/maybedir/bar", ObjectType::File, 0),
            ("test1/dir1/maybedir/baz", ObjectType::File, 0),
            ("test1/dir1/maybedir/foobar", ObjectType::Directory, 0),
            ("test1/dir1/maybedir/foobar/foo", ObjectType::File, 0),
            ("test1/dir1/maybedir/foobar/bar", ObjectType::File, 0),
        ])
    } else {
        allfiles.extend(vec![("test1/dir1/maybedir", ObjectType::File, 0)])
    }

    test_list(fs, context, "test1/dir1", allfiles).await?;

    let mut prefixed = vec![
        ("test1/dir1/dir2/0foo", ObjectType::File, 0),
        ("test1/dir1/dir2/1bar", ObjectType::File, 0),
        ("test1/dir1/dir2/5diz", ObjectType::File, 0),
        ("test1/dir1/dir2/bar", ObjectType::File, 0),
        ("test1/dir1/dir2/daz", ObjectType::File, 300),
        ("test1/dir1/dir2/foo", ObjectType::File, 0),
        ("test1/dir1/dir2/hop", ObjectType::File, 0),
        ("test1/dir1/dir2/yu", ObjectType::File, 0),
    ];

    test_list(fs, context, "test1/dir1/dir2/", prefixed.clone()).await?;

    if fs.backend_type() == Backend::File {
        prefixed.extend(vec![("test1/dir1/dir2", ObjectType::Directory, 0)]);
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
        files: Vec<(&'static str, ObjectType, u64)>,
    ) -> TestResult<()> {
        if !context.contains(path) {
            return Ok(());
        }

        let mut expected_files: Vec<(ObjectPath, ObjectType, u64)> = files
            .iter()
            .filter_map(|info| {
                if context.contains(info.0) {
                    let path = context.get_path(info.0);
                    Some((path, info.1, info.2))
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
        expected_files.sort();

        test_assert_eq!(
            results.len(),
            expected_files.len(),
            "Should have seen the right number of results.",
        );

        for _ in 0..expected_files.len() {
            let result = results.remove(0);
            let (path, file_type, size) = expected_files.remove(0);
            compare_file(&result, path, file_type, size)?;
        }

        Ok(())
    }

    let base = vec![("test1", ObjectType::Directory, 0)];
    test_list(fs, context, "test1", base).await?;

    let mut dir1 = vec![
        ("test1/dir1/largefile", ObjectType::File, 100 * MB),
        ("test1/dir1/mediumfile", ObjectType::File, 5 * MB),
        ("test1/dir1/smallfile.txt", ObjectType::File, 27),
        ("test1/dir1/dir2", ObjectType::Directory, 0),
    ];

    if fs.backend_type() == Backend::File {
        dir1.extend(vec![("test1/dir1/maybedir", ObjectType::Directory, 0)])
    } else {
        dir1.extend(vec![("test1/dir1/maybedir", ObjectType::File, 0)])
    }

    test_list(fs, context, "test1/dir1", dir1).await?;

    let dir2 = vec![
        ("test1/dir1/dir2/foo", ObjectType::File, 0),
        ("test1/dir1/dir2/bar", ObjectType::File, 0),
        ("test1/dir1/dir2/0foo", ObjectType::File, 0),
        ("test1/dir1/dir2/5diz", ObjectType::File, 0),
        ("test1/dir1/dir2/1bar", ObjectType::File, 0),
        ("test1/dir1/dir2/daz", ObjectType::File, 300),
        ("test1/dir1/dir2/hop", ObjectType::File, 0),
        ("test1/dir1/dir2/yu", ObjectType::File, 0),
    ];

    test_list(fs, context, "test1/dir1/dir2", dir2).await?;

    Ok(())
}

pub async fn test_get_object(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_pass(
        fs: &FileStore,
        context: &TestContext,
        path: &str,
        expected_type: ObjectType,
        size: u64,
    ) -> TestResult<()> {
        let expected_path = context.get_path(path);
        let file = fs.get_object(expected_path.clone()).await?;
        compare_file(&file, expected_path, expected_type, size)?;

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

    test_pass(
        fs,
        context,
        "test1/dir1/largefile",
        ObjectType::File,
        100 * MB,
    )
    .await?;
    test_pass(
        fs,
        context,
        "test1/dir1/smallfile.txt",
        ObjectType::File,
        27,
    )
    .await?;
    test_pass(fs, context, "test1/dir1/dir2/0foo", ObjectType::File, 0).await?;
    test_pass(fs, context, "test1/dir1/dir2/daz", ObjectType::File, 300).await?;

    test_fail(fs, context, "test1/dir1/daz").await?;
    test_fail(fs, context, "test1/dir1/foo/bar").await?;

    if fs.backend_type() == Backend::File {
        test_pass(fs, context, "test1/dir1/maybedir", ObjectType::Directory, 0).await?;
    } else {
        test_fail(fs, context, "test1/dir1/dir2").await?;
        test_pass(fs, context, "test1/dir1/maybedir", ObjectType::File, 0).await?;
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
