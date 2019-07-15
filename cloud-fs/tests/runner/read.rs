use std::iter::empty;

use futures::stream::TryStreamExt;

use super::utils::*;
use super::*;

use cloud_fs::*;

fn compare_file(file: &FsFile, expected_path: &FsPath, expected_size: u64) -> FsResult<()> {
    test_assert_eq!(file.path(), expected_path, "Should have the expected path.");
    test_assert_eq!(
        file.size(),
        expected_size,
        "Should have the expected size for {}",
        expected_path,
    );
    Ok(())
}

pub async fn test_list_files(fs: &Fs, _context: &TestContext) -> FsResult<()> {
    async fn test_list<'a>(
        fs: &'a Fs,
        path: &'static str,
        mut files: Vec<(&'static str, u64)>,
    ) -> FsResult<()> {
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

pub async fn test_get_file(fs: &Fs, _context: &TestContext) -> FsResult<()> {
    async fn test_pass(fs: &Fs, path: &str, size: u64) -> FsResult<()> {
        let expected_path = FsPath::new(path)?;
        let file = fs.get_file(expected_path.clone()).await?;
        compare_file(&file, &expected_path, size)?;

        Ok(())
    }

    async fn test_fail(fs: &Fs, path: &str) -> FsResult<()> {
        let result = fs.get_file(FsPath::new(path)?).await;
        test_assert!(result.is_err());
        if let Err(e) = result {
            test_assert_eq!(e.kind(), FsErrorKind::NotFound);
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

/*pub fn test_get_file(
    fs: Fs,
    context: TestContext,
) -> impl Future<Item = (Fs, TestContext), Error = FsError> {
    fn test_get(
        fs: Fs,
        context: TestContext,
        path: &str,
        size: u64,
    ) -> impl Future<Item = (Fs, TestContext), Error = FsError> {
        let expected_path = FsPath::new(path).unwrap();
        fs.get_file(FsPath::new(path).unwrap())
            .and_then(move |result| {
                compare_file(&result, &expected_path, size)?;
                Ok((fs, context))
            })
    }

    fn test_fail(
        fs: Fs,
        context: TestContext,
        path: &str,
    ) -> impl Future<Item = (Fs, TestContext), Error = FsError> {
        fs.get_file(FsPath::new(path).unwrap()).then(move |result| {
            test_assert!(result.is_err());
            if let Err(e) = result {
                test_assert_eq!(e.kind(), FsErrorKind::NotFound);
            }

            Ok((fs, context))
        })
    }

    test_get(fs, context, "/largefile", 100 * MB)
        .and_then(|(fs, context)| test_get(fs, context, "/smallfile.txt", 27))
        .and_then(|(fs, context)| test_get(fs, context, "/dir2/0foo", 0))
        .and_then(|(fs, context)| test_get(fs, context, "/dir2/daz", 300))
        .and_then(|(fs, context)| test_fail(fs, context, "/dir2"))
        .and_then(|(fs, context)| test_fail(fs, context, "/daz"))
        .and_then(|(fs, context)| test_fail(fs, context, "/foo/bar"))
}

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
