use std::convert::TryInto;
use std::iter::empty;

use futures::stream::{StreamExt, TryStreamExt};

use super::utils::*;
use super::*;

use file_store::*;

fn compare_file(
    file: &Object,
    mut expected_path: StoragePath,
    expected_type: ObjectType,
    expected_size: u64,
) -> TestResult<()> {
    if expected_type == ObjectType::Directory && !expected_path.is_directory() {
        if let Some(name) = expected_path.filename() {
            expected_path.push_dir(&name);
        }
    }

    test_assert_eq!(
        file.path(),
        expected_path.clone(),
        "Should have the expected path."
    );
    test_assert_eq!(
        file.file_type(),
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

pub async fn test_list_files(fs: &FileStore, _context: &TestContext) -> TestResult<()> {
    async fn test_list<'a>(
        fs: &'a FileStore,
        path: &'static str,
        mut files: Vec<(&'static str, ObjectType, u64)>,
    ) -> TestResult<()> {
        let mut results = fs
            .list_objects(StoragePath::new(path)?)
            .await?
            .try_collect::<Vec<Object>>()
            .await?;
        results.sort();
        files.sort();

        test_assert_eq!(
            results.len(),
            files.len(),
            "Should have seen the right number of results.",
        );

        for _ in 0..files.len() {
            let result = results.remove(0);
            let (pathstr, file_type, size) = files.remove(0);
            compare_file(&result, StoragePath::new(pathstr)?, file_type, size)?;
        }

        Ok(())
    }

    let mut allfiles = vec![
        ("/largefile", ObjectType::File, 100 * MB),
        ("/mediumfile", ObjectType::File, 5 * MB),
        ("/smallfile.txt", ObjectType::File, 27),
        ("/dir2/0foo", ObjectType::File, 0),
        ("/dir2/1bar", ObjectType::File, 0),
        ("/dir2/5diz", ObjectType::File, 0),
        ("/dir2/bar", ObjectType::File, 0),
        ("/dir2/daz", ObjectType::File, 300),
        ("/dir2/foo", ObjectType::File, 0),
        ("/dir2/hop", ObjectType::File, 0),
        ("/dir2/yu", ObjectType::File, 0),
    ];

    if fs.backend_type() == Backend::File {
        allfiles.extend(vec![
            ("/dir2/", ObjectType::Directory, 0),
            ("/maybedir/", ObjectType::Directory, 0),
            ("/maybedir/foo", ObjectType::File, 0),
            ("/maybedir/bar", ObjectType::File, 0),
            ("/maybedir/baz", ObjectType::File, 0),
            ("/maybedir/foobar/", ObjectType::Directory, 0),
            ("/maybedir/foobar/foo", ObjectType::File, 0),
            ("/maybedir/foobar/bar", ObjectType::File, 0),
        ])
    } else {
        allfiles.extend(vec![("/maybedir", ObjectType::File, 0)])
    }

    test_list(fs, "/", allfiles).await?;

    test_list(
        fs,
        "/dir2/",
        vec![
            ("/dir2/0foo", ObjectType::File, 0),
            ("/dir2/1bar", ObjectType::File, 0),
            ("/dir2/5diz", ObjectType::File, 0),
            ("/dir2/bar", ObjectType::File, 0),
            ("/dir2/daz", ObjectType::File, 300),
            ("/dir2/foo", ObjectType::File, 0),
            ("/dir2/hop", ObjectType::File, 0),
            ("/dir2/yu", ObjectType::File, 0),
        ],
    )
    .await?;

    Ok(())
}

pub async fn test_get_file(fs: &FileStore, _context: &TestContext) -> TestResult<()> {
    async fn test_pass(
        fs: &FileStore,
        path: &str,
        expected_type: ObjectType,
        size: u64,
    ) -> TestResult<()> {
        let expected_path = StoragePath::new(path)?;
        let file = fs.get_object(expected_path.clone()).await?;
        compare_file(&file, expected_path, expected_type, size)?;

        Ok(())
    }

    async fn test_fail(fs: &FileStore, path: &str) -> TestResult<()> {
        let fspath = StoragePath::new(path)?;
        let result = fs.get_object(fspath.clone()).await;
        test_assert!(result.is_err(), "Should have failed to find {}.", fspath);
        if let Err(e) = result {
            test_assert_eq!(
                e.try_into(),
                Ok(StorageErrorKind::NotFound(fspath)),
                "Should have returned a NotFound error."
            );
        }

        Ok(())
    }

    test_pass(fs, "/largefile", ObjectType::File, 100 * MB).await?;
    test_pass(fs, "/smallfile.txt", ObjectType::File, 27).await?;
    test_pass(fs, "/dir2/0foo", ObjectType::File, 0).await?;
    test_pass(fs, "/dir2/daz", ObjectType::File, 300).await?;

    test_fail(fs, "/daz").await?;
    test_fail(fs, "/foo/bar").await?;

    if fs.backend_type() == Backend::File {
        test_pass(fs, "/maybedir", ObjectType::Directory, 0).await?;
    } else {
        test_fail(fs, "/dir2").await?;
        test_fail(fs, "/maybedir").await?;
    }

    Ok(())
}

pub async fn test_get_file_stream(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_pass<I>(
        fs: &FileStore,
        _context: &TestContext,
        path: &str,
        mut data: I,
    ) -> TestResult<()>
    where
        I: Iterator<Item = u8>,
    {
        let target = StoragePath::new(path)?;
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

    async fn test_fail(fs: &FileStore, _context: &TestContext, path: &str) -> TestResult<()> {
        let target = StoragePath::new(path)?;
        let result = fs.get_file_stream(target.clone()).await;
        test_assert!(result.is_err());
        if let Err(e) = result {
            test_assert_eq!(e.try_into(), Ok(StorageErrorKind::NotFound(target)));
        }

        Ok(())
    }

    test_pass(
        fs,
        context,
        "/smallfile.txt",
        b"This is quite a short file.".iter().cloned(),
    )
    .await?;
    test_pass(fs, context, "/largefile", ContentIterator::new(0, 100 * MB)).await?;
    test_pass(fs, context, "/dir2/bar", empty()).await?;
    test_pass(fs, context, "/dir2/daz", ContentIterator::new(72, 300)).await?;

    test_fail(fs, context, "/dir2").await?;
    test_fail(fs, context, "/daz").await?;
    test_fail(fs, context, "/foo/bar").await?;

    Ok(())
}
