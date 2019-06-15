use tokio::prelude::*;

use super::utils::*;

use cloud_fs::*;

fn compare_file(file: &FsFile, expected_path: &FsPath, expected_size: u64) -> FsResult<()> {
    assert_eq(file.path(), expected_path, "Should have the expected path.")?;
    assert_eq(
        file.size(),
        expected_size,
        format!("Should have the expected size for {}", expected_path),
    )?;
    Ok(())
}

pub fn test_list_files(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    fn test_list(
        fs: Fs,
        path: &str,
        mut files: Vec<(&'static str, u64)>,
    ) -> impl Future<Item = Fs, Error = FsError> {
        fs.list_files(FsPath::new(path).unwrap())
            .and_then(|s| s.collect())
            .and_then(move |mut results| {
                results.sort();
                assert_eq(
                    results.len(),
                    files.len(),
                    "Should have seen the right number of results.",
                )?;

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
            ("/dir2/daz", 0),
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
                ("/dir2/daz", 0),
                ("/dir2/foo", 0),
                ("/dir2/hop", 0),
                ("/dir2/yu", 0),
            ],
        )
    })
}

pub fn test_get_file(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    fn test_get(fs: Fs, path: &str, size: u64) -> impl Future<Item = Fs, Error = FsError> {
        let expected_path = FsPath::new(path).unwrap();
        fs.get_file(FsPath::new(path).unwrap())
            .and_then(move |result| {
                compare_file(&result, &expected_path, size)?;
                Ok(fs)
            })
    }

    test_get(fs, "/largefile", 100 * MB)
}

pub fn test_get_file_stream(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    fn test_stream<I>(
        fs: Fs,
        path: &str,
        data: I,
    ) -> impl Future<Item = Fs, Error = FsError>
    where
        I: Iterator<Item = u8>,
    {
        fs.get_file_stream(FsPath::new(path).unwrap())
            .and_then(move |stream| {
                stream.fold((data, 0), |(mut data, mut count), bytes| -> FsResult<(I, u64)> {
                    let mut iter = bytes.into_iter();
                    loop {
                        let found = iter.next();
                        if found.is_none() {
                            break;
                        }
                        count += 1;

                        let expected = data.next();
                        assert_eq(found, expected, format!("Should have seen the right data at {}.", count))?;
                    }

                    Ok((data, count))
                })
                .and_then(|(mut data, _count)| {
                    assert_eq(data.next(), None, "Should have read the entire file.")
                })
            })
            .map(move |_| fs)

    }

    test_stream(fs, "/largefile", ContentIterator::new(0, 100 * MB))
}
