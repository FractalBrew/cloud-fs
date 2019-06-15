use tokio::prelude::*;

use super::utils::*;

use cloud_fs::*;

struct FileChecker {
    path: FsPath,
    size: u64,
}

impl FileChecker {
    fn check(&self, file: &FsFile) -> FsResult<()> {
        assert_eq(file.path(), &self.path, "Should have the expected name.")?;
        assert_eq(
            file.size(),
            self.size,
            format!("Should have the expected size for {}", self.path),
        )?;

        Ok(())
    }

    fn check_files(found: Vec<FsFile>, expected: Vec<FileChecker>) -> FsResult<()> {
        for (file, checker) in found.iter().zip(expected.iter()) {
            checker.check(file)?;
        }

        assert_eq(
            found.len(),
            expected.len(),
            "Should have seen the right number of results.",
        )?;

        Ok(())
    }
}

fn test_list_files(fs: Fs) -> impl Future<Item = Fs, Error = FsError> {
    fn expect(
        fs: Fs,
        path: FsPath,
        files: Vec<FileChecker>,
    ) -> impl Future<Item = Fs, Error = FsError> {
        fs.list_files(path)
            .and_then(|s| s.collect())
            .and_then(move |mut results| {
                results.sort();
                FileChecker::check_files(results, files)
            })
            .map(move |_| fs)
    }

    let all = vec![
        FileChecker {
            path: FsPath::new("/largefile").unwrap(),
            size: 100 * MB,
        },
        FileChecker {
            path: FsPath::new("/mediumfile").unwrap(),
            size: 5 * MB,
        },
        FileChecker {
            path: FsPath::new("/smallfile.txt").unwrap(),
            size: 27,
        },
        FileChecker {
            path: FsPath::new("/dir2/0foo").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/1bar").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/5diz").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/bar").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/daz").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/foo").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/hop").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/yu").unwrap(),
            size: 0,
        },
    ];

    let sub = vec![
        FileChecker {
            path: FsPath::new("/dir2/0foo").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/1bar").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/5diz").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/bar").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/daz").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/foo").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/hop").unwrap(),
            size: 0,
        },
        FileChecker {
            path: FsPath::new("/dir2/yu").unwrap(),
            size: 0,
        },
    ];

    expect(fs, FsPath::new("/").unwrap(), all)
        .and_then(|fs| expect(fs, FsPath::new("/dir2/").unwrap(), sub))
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
