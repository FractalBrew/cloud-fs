// Copyright 2019 Dave Townsend
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs::{symlink_metadata, File};
use std::io::{BufReader, ErrorKind, Read};
use std::path::Path;

use super::utils::*;
use super::*;

use file_store::backends::Backend;
use file_store::*;

fn test_file_matches<I>(target: &Path, info: UploadInfo, mut expected: I) -> TestResult<()>
where
    I: Iterator<Item = u8>,
{
    let meta = symlink_metadata(&target).map_err(TestError::from_error)?;

    let mut found = BufReader::new(File::open(target).map_err(TestError::from_error)?).bytes();
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
                    info.path,
                    pos
                );
            }
            (Some(_), None) => {
                test_fail!("Found too many bytes in {}.", info.path);
            }
            (None, Some(_)) => {
                test_fail!("Found too few bytes in {}.", info.path);
            }
            (None, None) => break,
        }

        pos += 1;
    }

    if let Some(time) = info.modified.as_ref() {
        test_assert_eq!(
            time,
            &meta.modified().map_err(TestError::from_error)?,
            "Should have seen the right modification time for {}.",
            info.path
        );
    }

    Ok(())
}

pub async fn test_copy_file(fs: &FileStore, context: &TestContext) -> TestResult<()> {
    async fn test_pass(
        fs: &FileStore,
        context: &TestContext,
        path: &str,
        target: UploadInfo,
        seed: u8,
        length: u64,
    ) -> TestResult<()> {
        let remote_current = context.get_path(path);
        let local_current = context.get_target(&remote_current);
        let local_target = context.get_target(&target.path);

        fs.copy_file(remote_current.clone(), target.clone()).await?;

        let result = symlink_metadata(local_current.clone());
        if let Ok(m) = result {
            test_assert!(m.is_file(), "File {} should still exist.", remote_current);
        } else {
            test_fail!(
                "Should not have seen an error getting metadata for the old file {}",
                remote_current
            );
        }

        test_file_matches(&local_target, target, ContentIterator::new(seed, length))?;

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

        let result = symlink_metadata(local_target);
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
        UploadInfo {
            path: context.get_path("test1/dir1/testfile"),
            modified: None,
        },
        58,
        5 * MB,
    )
    .await?;
    test_pass(
        fs,
        context,
        "test1/dir1/largefile",
        UploadInfo {
            path: context.get_path("test1/dir1/dir2/hop"),
            modified: None,
        },
        0,
        100 * MB,
    )
    .await?;
    test_pass(
        fs,
        context,
        "test1/dir1/dir2/daz",
        UploadInfo {
            path: context.get_path("test1/dir1/bazza"),
            modified: Some(UNIX_EPOCH + Duration::from_millis(1_703_257_714)),
        },
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
        target: UploadInfo,
        seed: u8,
        length: u64,
    ) -> TestResult<()> {
        let remote_current = context.get_path(path);
        let local_current = context.get_target(&remote_current);
        let local_target = context.get_target(&target.path);

        fs.move_file(remote_current.clone(), target.clone()).await?;

        let result = symlink_metadata(local_current.clone());
        if result.is_ok() {
            test_fail!("File {} should no longer exist.", remote_current);
        }

        test_file_matches(&local_target, target, ContentIterator::new(seed, length))?;

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

        let result = symlink_metadata(local_target);
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
        UploadInfo {
            path: context.get_path("test1/dir1/testfile"),
            modified: Some(UNIX_EPOCH + Duration::from_millis(1_703_257_714)),
        },
        58,
        5 * MB,
    )
    .await?;
    test_pass(
        fs,
        context,
        "test1/dir1/largefile",
        UploadInfo {
            path: context.get_path("test1/dir1/dir2/hop"),
            modified: None,
        },
        0,
        100 * MB,
    )
    .await?;
    test_pass(
        fs,
        context,
        "test1/dir1/dir2/daz",
        UploadInfo {
            path: context.get_path("test1/dir1/bazza"),
            modified: None,
        },
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

        match symlink_metadata(target.clone()) {
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

        if let Ok(m) = symlink_metadata(target) {
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
        target: UploadInfo,
        seed: u8,
        length: u64,
    ) -> TestResult<()> {
        let local_target = context.get_target(&target.path);

        fs.write_file_from_stream(
            target.clone(),
            stream_iterator(ContentIterator::new(seed, length), (length / 10) as usize),
        )
        .await?;

        test_file_matches(&local_target, target, ContentIterator::new(seed, length))?;

        Ok(())
    }

    test_write(
        fs,
        context,
        UploadInfo {
            path: context.get_path("test1/dir1/foobar"),
            modified: Some(UNIX_EPOCH + Duration::from_millis(1_703_257_714)),
        },
        58,
        300,
    )
    .await?;
    test_write(
        fs,
        context,
        UploadInfo {
            path: context.get_path("test1/dir1/maybedir"),
            modified: None,
        },
        27,
        500,
    )
    .await?;
    test_write(
        fs,
        context,
        UploadInfo {
            path: context.get_path("test1/dir1/dir2/daz"),
            modified: None,
        },
        27,
        100 * MB,
    )
    .await?;

    Ok(())
}
