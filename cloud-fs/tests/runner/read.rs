use tokio::prelude::*;

use super::utils::*;

use cloud_fs::*;

fn test_list_files(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    fn test_list(
        fs: Fs,
        path: FsPath,
        mut files: Vec<(&'static str, u64)>,
    ) -> impl Future<Item = Fs, Error = FsError> {
        fs.list_files(path)
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
                    let path = FsPath::new(pathstr)?;

                    assert_eq(result.path(), &path, "Should have the expected name.")?;
                    assert_eq(
                        result.size(),
                        size,
                        format!("Should have the expected size for {}", path),
                    )?;
                }
                Ok(())
            })
            .map(|_| fs)
    }

    test_list(
        fs,
        FsPath::new("/").unwrap(),
        vec![
            ("/largefile", 100 * MB),
            ("/mediumfile", 5 * MB),
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
            FsPath::new("/dir2/").unwrap(),
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

fn test_get_file(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    let path = FsPath::new("/largefile").unwrap();
    fs.get_file(path).and_then(move |file| {
        assert_eq(
            file.path().to_string(),
            String::from("/largefile"),
            "Should have seen the right path.",
        )?;
        assert_eq(file.size(), 100 * MB, "Should have seen the right size.")?;
        Ok(fs)
    })
}

fn test_get_file_stream(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    future::finished(fs)
}

pub fn run_tests(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    test_list_files(fs)
        .and_then(test_get_file)
        .and_then(test_get_file_stream)
}
