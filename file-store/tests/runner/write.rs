use std::fs::{metadata, File};
use std::io::{BufReader, ErrorKind, Read};

use super::utils::*;
use super::*;

use file_store::backends::Backend;
use file_store::*;

pub async fn test_copy_file(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_pass(
        fs: &FileStore,
        context: &TestContext,
        path: &str,
        target: &str,
        seed: u8,
        length: u64,
    ) -> TestResult<()> {
        let remote_current = context.get_path(path);
        let local_current = context.get_target(&remote_current);
        let remote_target = context.get_path(target);
        let local_target = context.get_target(&remote_target);

        fs.copy_file(remote_current.clone(), remote_target.clone())
            .await?;

        let result = metadata(local_current.clone());
        if let Ok(m) = result {
            test_assert!(m.is_file(), "File {} should still exist.", remote_current);
        } else {
            test_fail!(
                "Should not have seen an error getting metadata for the old file {}",
                remote_current
            );
        }

        let mut found =
            BufReader::new(File::open(&local_target).map_err(TestError::from_error)?).bytes();
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
                        remote_target,
                        pos
                    );
                }
                (Some(_), None) => {
                    test_fail!("Found too many bytes in {}.", remote_target);
                }
                (None, Some(_)) => {
                    test_fail!("Found too few bytes in {}.", remote_target);
                }
                (None, None) => break,
            }

            pos += 1;
        }

        Ok(())
    }

    async fn test_fail(
        fs: &FileStore,
        context: &TestContext,
        path: &str,
        target: &str,
    ) -> TestResult<()> {
        let remote_current = context.get_path(path);
        let remote_target = context.get_path(target);
        let local_target = context.get_target(&remote_target);

        let result = fs
            .copy_file(remote_current.clone(), remote_target.clone())
            .await;

        if let Err(e) = result {
            if let TransferError::SourceError(s) = e {
                test_assert_eq!(
                    s.kind(),
                    StorageErrorKind::NotFound(remote_current.clone()),
                    "Should have been unable to find {}.",
                    remote_current
                );
            } else {
                test_fail!("Should have received a source error.");
            }
        } else {
            test_fail!("Expected to fail to copy {}.", remote_current);
        }

        let result = metadata(local_target);
        if let Err(e) = result {
            test_assert_eq!(
                e.kind(),
                ErrorKind::NotFound,
                "File {} should not exist.",
                remote_target
            );
        }

        Ok(())
    }

    test_pass(
        fs,
        context,
        "test1/dir1/mediumfile",
        "test1/dir1/testfile",
        58,
        5 * MB,
    )
    .await?;
    test_pass(
        fs,
        context,
        "test1/dir1/largefile",
        "test1/dir1/dir2/hop",
        0,
        100 * MB,
    )
    .await?;
    test_pass(
        fs,
        context,
        "test1/dir1/dir2/daz",
        "test1/dir1/bazza",
        72,
        300,
    )
    .await?;

    test_fail(fs, context, "test1/dir1/dir2/gaz", "test1/dir1/bazza").await?;
    test_fail(fs, context, "test1/dir1/fooish", "test1/dir1/dir2/too").await?;

    Ok(())
}

pub async fn test_move_file(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_pass(
        fs: &FileStore,
        context: &TestContext,
        path: &str,
        target: &str,
        seed: u8,
        length: u64,
    ) -> TestResult<()> {
        let remote_current = context.get_path(path);
        let local_current = context.get_target(&remote_current);
        let remote_target = context.get_path(target);
        let local_target = context.get_target(&remote_target);

        fs.move_file(remote_current.clone(), remote_target.clone())
            .await?;

        let result = metadata(local_current.clone());
        if result.is_ok() {
            test_fail!("File {} should no longer exist.", remote_current);
        }

        let mut found =
            BufReader::new(File::open(&local_target).map_err(TestError::from_error)?).bytes();
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
                        remote_target,
                        pos
                    );
                }
                (Some(_), None) => {
                    test_fail!("Found too many bytes in {}.", remote_target);
                }
                (None, Some(_)) => {
                    test_fail!("Found too few bytes in {}.", remote_target);
                }
                (None, None) => break,
            }

            pos += 1;
        }

        Ok(())
    }

    async fn test_fail(
        fs: &FileStore,
        context: &TestContext,
        path: &str,
        target: &str,
    ) -> TestResult<()> {
        let remote_current = context.get_path(path);
        let remote_target = context.get_path(target);
        let local_target = context.get_target(&remote_target);

        let result = fs
            .move_file(remote_current.clone(), remote_target.clone())
            .await;

        if let Err(e) = result {
            if let TransferError::SourceError(s) = e {
                test_assert_eq!(
                    s.kind(),
                    StorageErrorKind::NotFound(remote_current.clone()),
                    "Should have been unable to find {}.",
                    remote_current
                );
            } else {
                test_fail!("Should have received a source error.");
            }
        } else {
            test_fail!("Expected to fail to copy {}.", remote_current);
        }

        let result = metadata(local_target);
        if let Err(e) = result {
            test_assert_eq!(
                e.kind(),
                ErrorKind::NotFound,
                "File {} should not exist.",
                remote_target
            );
        }

        Ok(())
    }

    test_pass(
        fs,
        context,
        "test1/dir1/mediumfile",
        "test1/dir1/testfile",
        58,
        5 * MB,
    )
    .await?;
    test_pass(
        fs,
        context,
        "test1/dir1/largefile",
        "test1/dir1/dir2/hop",
        0,
        100 * MB,
    )
    .await?;
    test_pass(
        fs,
        context,
        "test1/dir1/dir2/daz",
        "test1/dir1/bazza",
        72,
        300,
    )
    .await?;

    test_fail(fs, context, "test1/dir1/dir2/gaz", "test1/dir1/bazza").await?;
    test_fail(fs, context, "test1/dir1/fooish", "test1/dir1/dir2/too").await?;

    Ok(())
}

pub async fn test_delete_object(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_pass(fs: &FileStore, context: &TestContext, path: &str) -> TestResult<()> {
        let remote = context.get_path(path);
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
        let fspath = context.get_path(path);
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

    test_pass(fs, context, "test1/dir1/largefile").await?;
    test_pass(fs, context, "test1/dir1/smallfile.txt").await?;
    test_pass(fs, context, "test1/dir1/dir2/daz").await?;
    test_pass(fs, context, "test1/dir1/maybedir").await?;

    if fs.backend_type() == Backend::File {
        test_pass(fs, context, "test1/dir1/dir2").await?;
    } else {
        test_fail(fs, context, "test1/dir1/dir2").await?;
    }

    test_fail(fs, context, "test1/dir1/biz").await?;

    Ok(())
}

pub async fn test_write_file_from_stream(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_write(
        fs: &FileStore,
        context: &TestContext,
        path: &str,
        seed: u8,
        length: u64,
    ) -> TestResult<()> {
        let remote = context.get_path(path);
        let target = context.get_target(&remote);

        fs.write_file_from_stream(
            remote.clone(),
            stream_iterator(ContentIterator::new(seed, length), (length / 10) as usize),
        )
        .await?;

        let meta = metadata(target.clone());
        test_assert!(meta.is_ok(), "Should have created the file {}.", remote);
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

    test_write(fs, context, "test1/dir1/foobar", 58, 300).await?;
    test_write(fs, context, "test1/dir1/maybedir", 27, 500).await?;
    test_write(fs, context, "test1/dir1/dir2/daz", 27, 100 * MB).await?;

    Ok(())
}
