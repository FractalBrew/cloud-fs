use std::iter::empty;

use tokio::prelude::*;

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

pub fn test_list_files(fs: Fs, _local_path: PathBuf) -> impl Future<Item = Fs, Error = FsError> {
    fn test_list(
        fs: Fs,
        path: &str,
        mut files: Vec<(&'static str, u64)>,
    ) -> impl Future<Item = Fs, Error = FsError> {
        fs.list_files(FsPath::new(path).unwrap())
            .and_then(|s| s.collect())
            .and_then(move |mut results| {
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
                Ok(fs)
            })
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
    .and_then(|fs| {
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
    })
}

pub fn test_get_file(fs: Fs, _local_path: PathBuf) -> impl Future<Item = Fs, Error = FsError> {
    fn test_get(fs: Fs, path: &str, size: u64) -> impl Future<Item = Fs, Error = FsError> {
        let expected_path = FsPath::new(path).unwrap();
        fs.get_file(FsPath::new(path).unwrap())
            .and_then(move |result| {
                compare_file(&result, &expected_path, size)?;
                Ok(fs)
            })
    }

    fn test_fail(fs: Fs, path: &str) -> impl Future<Item = Fs, Error = FsError> {
        fs.get_file(FsPath::new(path).unwrap()).then(move |result| {
            test_assert!(result.is_err());
            if let Err(e) = result {
                test_assert_eq!(e.kind(), FsErrorKind::NotFound);
            }

            Ok(fs)
        })
    }

    test_get(fs, "/largefile", 100 * MB)
        .and_then(|fs| test_get(fs, "/smallfile.txt", 27))
        .and_then(|fs| test_get(fs, "/dir2/0foo", 0))
        .and_then(|fs| test_get(fs, "/dir2/daz", 300))
        .and_then(|fs| test_fail(fs, "/dir2"))
        .and_then(|fs| test_fail(fs, "/daz"))
}

pub fn test_get_file_stream(
    fs: Fs,
    _local_path: PathBuf,
) -> impl Future<Item = Fs, Error = FsError> {
    fn test_stream<I>(fs: Fs, path: &str, data: I) -> impl Future<Item = Fs, Error = FsError>
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
            .map(move |_| fs)
    }

    fn test_fail(fs: Fs, path: &str) -> impl Future<Item = Fs, Error = FsError> {
        fs.get_file_stream(FsPath::new(path).unwrap())
            .then(move |result| {
                test_assert!(result.is_err());
                if let Err(e) = result {
                    test_assert_eq!(e.kind(), FsErrorKind::NotFound);
                }

                Ok(fs)
            })
    }

    test_stream(
        fs,
        "/smallfile.txt",
        b"This is quite a short file.".iter().cloned(),
    )
    .and_then(|fs| test_stream(fs, "/largefile", ContentIterator::new(0, 100 * MB)))
    .and_then(|fs| test_stream(fs, "/dir2/bar", empty()))
    .and_then(|fs| test_stream(fs, "/dir2/daz", ContentIterator::new(72, 300)))
    .and_then(|fs| test_fail(fs, "/dir2"))
    .and_then(|fs| test_fail(fs, "/daz"))
}
