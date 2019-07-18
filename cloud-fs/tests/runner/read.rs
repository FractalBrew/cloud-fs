use std::iter::empty;

use futures::stream::{StreamExt, TryStreamExt};

use super::utils::*;
use super::*;

use cloud_fs::*;

fn compare_file(file: &FsFile, expected_path: &FsPath, expected_size: u64) -> TestResult<()> {
    test_assert_eq!(file.path(), expected_path, "Should have the expected path.");
    test_assert_eq!(
        file.size(),
        expected_size,
        "Should have the expected size for {}",
        expected_path,
    );
    Ok(())
}

pub async fn test_list_files(fs: &Fs, _context: &TestContext) -> TestResult<()> {
    async fn test_list<'a>(
        fs: &'a Fs,
        path: &'static str,
        mut files: Vec<(&'static str, u64)>,
    ) -> TestResult<()> {
        let mut results = fs
            .list_files(FsPath::new(path)?)
            .await?
            .try_collect::<Vec<FsFile>>()
            .await?;
        results.sort();

        test_assert_eq!(
            results.len(),
            files.len(),
            "Should have seen the right number of results.",
        );

        for _ in 0..files.len() {
            let result = results.remove(0);
            let (pathstr, size) = files.remove(0);
            compare_file(&result, &FsPath::new(pathstr)?, size)?;
        }

        Ok(())
    }

    test_list(
        fs,
        "/",
        vec![
            ("/largefile", 100 * MB),
            ("/mediumfile", 5 * MB),
            ("/smallfile.txt", 27),
            ("/dir2/0foo", 0),
            ("/dir2/1bar", 0),
            ("/dir2/5diz", 0),
            ("/dir2/bar", 0),
            ("/dir2/daz", 300),
            ("/dir2/foo", 0),
            ("/dir2/hop", 0),
            ("/dir2/yu", 0),
        ],
    )
    .await?;

    test_list(
        fs,
        "/dir2/",
        vec![
            ("/dir2/0foo", 0),
            ("/dir2/1bar", 0),
            ("/dir2/5diz", 0),
            ("/dir2/bar", 0),
            ("/dir2/daz", 300),
            ("/dir2/foo", 0),
            ("/dir2/hop", 0),
            ("/dir2/yu", 0),
        ],
    )
    .await?;

    Ok(())
}

pub async fn test_get_file(fs: &Fs, _context: &TestContext) -> TestResult<()> {
    async fn test_pass(fs: &Fs, path: &str, size: u64) -> TestResult<()> {
        let expected_path = FsPath::new(path)?;
        let file = fs.get_file(expected_path.clone()).await?;
        compare_file(&file, &expected_path, size)?;

        Ok(())
    }

    async fn test_fail(fs: &Fs, path: &str) -> TestResult<()> {
        let fspath = FsPath::new(path)?;
        let result = fs.get_file(fspath.clone()).await;
        test_assert!(result.is_err(), "Should have failed to find {}.", fspath);
        if let Err(e) = result {
            test_assert_eq!(
                e.kind(),
                FsErrorKind::NotFound(fspath),
                "Should have returned a NotFound error."
            );
        }

        Ok(())
    }

    test_pass(fs, "/largefile", 100 * MB).await?;
    test_pass(fs, "/smallfile.txt", 27).await?;
    test_pass(fs, "/dir2/0foo", 0).await?;
    test_pass(fs, "/dir2/daz", 300).await?;
    test_fail(fs, "/dir2").await?;
    test_fail(fs, "/daz").await?;
    test_fail(fs, "/foo/bar").await?;

    Ok(())
}

pub async fn test_get_file_stream(fs: &Fs, context: &TestContext) -> TestResult<()> {
    async fn test_pass<I>(
        fs: &Fs,
        _context: &TestContext,
        path: &str,
        mut data: I,
    ) -> TestResult<()>
    where
        I: Iterator<Item = u8>,
    {
        let target = FsPath::new(path)?;
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
                    return Err(TestError::Unexpected(e));
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

    async fn test_fail(fs: &Fs, _context: &TestContext, path: &str) -> TestResult<()> {
        let target = FsPath::new(path)?;
        let result = fs.get_file_stream(target.clone()).await;
        test_assert!(result.is_err());
        if let Err(e) = result {
            test_assert_eq!(e.kind(), FsErrorKind::NotFound(target));
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

/*
pub fn test_get_file_stream(
    fs: Fs,
    context: TestContext,
) -> impl Future<Item = (Fs, TestContext), Error = FsError> {
    fn test_stream<I>(
        fs: Fs,
        context: TestContext,
        path: &str,
        data: I,
    ) -> impl Future<Item = (Fs, TestContext), Error = FsError>
    where
        I: Iterator<Item = u8>,
    {
        fs.get_file_stream(FsPath::new(path).unwrap())
            .and_then(move |stream| {
                stream
                    .fold(
                        (data, 0 as u64),
                        |(mut data, count), bytes| -> FsResult<(I, u64)> {
                            let mut expected: Vec<u8> = Vec::with_capacity(bytes.len());
                            for i in 0..bytes.len() as u64 {
                                let byte = data.next();
                                test_assert!(
                                    byte.is_some(),
                                    "Found an unexpected byte at index {}",
                                    count + i
                                );
                                expected.push(byte.unwrap());
                            }

                            test_assert_eq!(
                                &bytes as &[u8],
                                &expected as &[u8],
                                "Should have seen the right data at {}.",
                                count,
                            );

                            Ok((data, count + bytes.len() as u64))
                        },
                    )
                    .and_then(|(mut data, _)| {
                        test_assert!(data.next().is_none(), "Should have read the entire file.");
                        Ok(())
                    })
            })
            .map(move |_| (fs, context))
    }

    fn test_fail(
        fs: Fs,
        context: TestContext,
        path: &str,
    ) -> impl Future<Item = (Fs, TestContext), Error = FsError> {
        fs.get_file_stream(FsPath::new(path).unwrap())
            .then(move |result| {
                test_assert!(result.is_err());
                if let Err(e) = result {
                    test_assert_eq!(e.kind(), FsErrorKind::NotFound);
                }

                Ok((fs, context))
            })
    }

    test_stream(
        fs,
        context,
        "/smallfile.txt",
        b"This is quite a short file.".iter().cloned(),
    )
    .and_then(|(fs, context)| {
        test_stream(fs, context, "/largefile", ContentIterator::new(0, 100 * MB))
    })
    .and_then(|(fs, context)| test_stream(fs, context, "/dir2/bar", empty()))
    .and_then(|(fs, context)| test_stream(fs, context, "/dir2/daz", ContentIterator::new(72, 300)))
    .and_then(|(fs, context)| test_fail(fs, context, "/dir2"))
    .and_then(|(fs, context)| test_fail(fs, context, "/daz"))
    .and_then(|(fs, context)| test_fail(fs, context, "/foo/bar"))
}
*/
