use std::fs::{metadata, File};
use std::io::{BufReader, ErrorKind};

use super::utils::*;
use super::*;

use cloud_fs::*;

pub async fn test_delete_file(fs: &Fs, context: &TestContext) -> TestResult<()> {
    async fn test_pass(fs: &Fs, context: &TestContext, path: &str) -> TestResult<()> {
        let remote = FsPath::new(path)?;
        let target = context.get_target(&remote);

        fs.delete_file(remote).await?;

        match metadata(target.clone()) {
            Ok(m) => {
                test_assert!(m.is_file(), "Failed to delete {}", target.display());
            }
            Err(e) => {
                test_assert_eq!(
                    e.kind(),
                    ErrorKind::NotFound,
                    "Should have failed to find {}",
                    target.display()
                );
            }
        }

        Ok(())
    }

    async fn test_fail(fs: &Fs, context: &TestContext, path: &str) -> TestResult<()> {
        let fspath = FsPath::new(path)?;
        let target = context.get_target(&fspath);

        match fs.delete_file(fspath.clone()).await {
            Ok(()) => test_fail!("Should have failed to delete {}", fspath),
            Err(e) => test_assert_eq!(
                e.kind(),
                FsErrorKind::NotFound(fspath.clone()),
                "The file {} should have not been found.",
                fspath
            ),
        }

        if let Ok(m) = metadata(target) {
            test_assert!(m.is_dir(), "Shouldn't have deleted {}.", fspath);
        }

        Ok(())
    }

    test_pass(fs, context, "/largefile").await?;
    test_pass(fs, context, "/smallfile.txt").await?;
    test_pass(fs, context, "/dir2/daz").await?;
    test_fail(fs, context, "/biz").await?;
    test_fail(fs, context, "/dir2").await?;

    Ok(())
}

/*
pub fn test_write_from_stream(
    fs: Fs,
    context: TestContext,
) -> impl Future<Item = (Fs, TestContext), Error = FsError> {
    fn test_write(
        fs: Fs,
        context: TestContext,
        path: &str,
        seed: u8,
        length: u64,
    ) -> impl Future<Item = (Fs, TestContext), Error = FsError> {
        let remote = FsPath::new(path).unwrap();
        let target = context.get_target(&remote);
        fs.write_from_stream(
            remote.clone(),
            stream_iterator(ContentIterator::new(seed, length), (length / 10) as usize),
        )
        .and_then(move |()| {
            let meta = metadata(target.clone());
            test_assert!(meta.is_ok(), "Should be a new {} file.", remote);
            if let Ok(m) = meta {
                test_assert!(m.is_file(), "Should have written a {} file.", remote);
                test_assert_eq!(
                    m.len(),
                    length,
                    "Should have a {} file of the right length.",
                    remote
                );
            }

            let mut reader =
                BufReader::new(File::open(&target).map_err(FsError::from_error)?).bytes();
            let mut expected = ContentIterator::new(seed, length);
            let mut pos = 0;
            loop {
                match reader.next() {
                    Some(r) => {
                        let byte = r?;
                        let wanted = expected.next();
                        test_assert_eq!(
                            Some(byte),
                            wanted,
                            "File content of {} should have matched expected at pos {}",
                            remote,
                            pos
                        );
                    }
                    None => {
                        test_assert_eq!(
                            expected.next(),
                            None::<u8>,
                            "Content of {} should have the same length as the file.",
                            remote
                        );
                        break;
                    }
                }

                pos += 1;
            }

            Ok((fs, context))
        })
    }

    test_write(fs, context, "/foobar", 58, 300)
        .and_then(|(fs, context)| test_write(fs, context, "/dir3", 27, 500))
        .and_then(|(fs, context)| test_write(fs, context, "/dir2/daz", 27, 100 * MB))
}*/
