use std::fs::{metadata, File};
use std::io::{BufReader, ErrorKind, Read};

use super::utils::*;
use super::*;

use file_store::*;

pub async fn test_delete_file(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_pass(fs: &FileStore, context: &TestContext, path: &str) -> TestResult<()> {
        let remote = StoragePath::new(path)?;
        let target = context.get_target(&remote);

        fs.delete_object(remote).await?;

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

    async fn test_fail(fs: &FileStore, context: &TestContext, path: &str) -> TestResult<()> {
        let fspath = StoragePath::new(path)?;
        let target = context.get_target(&fspath);

        match fs.delete_object(fspath.clone()).await {
            Ok(()) => test_fail!("Should have failed to delete {}", fspath),
            Err(e) => test_assert_eq!(
                e.kind(),
                StorageErrorKind::NotFound(fspath.clone()),
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
    test_pass(fs, context, "/maybedir").await?;
    test_pass(fs, context, "/dir2").await?;

    test_fail(fs, context, "/biz").await?;

    Ok(())
}

pub async fn test_write_from_stream(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_write(
        fs: &FileStore,
        context: &TestContext,
        path: &str,
        seed: u8,
        length: u64,
    ) -> TestResult<()> {
        let remote = StoragePath::new(path)?;
        let target = context.get_target(&remote);

        fs.write_file_from_stream(
            remote.clone(),
            stream_iterator(ContentIterator::new(seed, length), (length / 10) as usize),
        )
        .await?;

        let meta = metadata(target.clone());
        test_assert!(meta.is_ok(), "Should haver created the file {}.", remote);
        if let Ok(m) = meta {
            test_assert!(m.is_file(), "Should have written the file {}.", remote);
            test_assert_eq!(
                m.len(),
                length,
                "File {} should have the right length.",
                remote
            );
        }

        let mut found = BufReader::new(File::open(&target).map_err(TestError::from_error)?).bytes();
        let mut expected = ContentIterator::new(seed, length);
        let mut pos = 0;
        loop {
            match (found.next(), expected.next()) {
                (Some(Err(e)), _) => {
                    return Err(TestError::from_error(e));
                }
                (Some(Ok(f)), Some(e)) => {
                    test_assert_eq!(
                        f,
                        e,
                        "File content of {} at {} should have matched expected.",
                        remote,
                        pos
                    );
                }
                (Some(_), None) => {
                    test_fail!("Found too many bytes in {}.", remote);
                }
                (None, Some(_)) => {
                    test_fail!("Found too few bytes in {}.", remote);
                }
                (None, None) => break,
            }

            pos += 1;
        }

        Ok(())
    }

    test_write(fs, context, "/foobar", 58, 300).await?;
    test_write(fs, context, "/maybedir", 27, 500).await?;
    test_write(fs, context, "/dir2/daz", 27, 100 * MB).await?;

    Ok(())
}
