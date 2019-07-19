use std::iter::empty;

use futures::stream::{StreamExt, TryStreamExt};

use super::utils::*;
use super::*;

use cloud_fs::*;

fn compare_file(
    file: &FsFile,
    mut expected_path: FsPath,
    expected_type: FsFileType,
    expected_size: u64,
) -> TestResult<()> {
    if expected_type == FsFileType::Directory && !expected_path.is_directory() {
        if let Some(name) = expected_path.filename() {
            expected_path.push_dir(&name);
        }
    }

    test_assert_eq!(
        file.path(),
        expected_path.clone(),
        "Should have the expected path."
    );
    test_assert_eq!(
        file.file_type(),
        expected_type,
        "Should have the expected type."
    );
    test_assert_eq!(
        file.size(),
        expected_size,
        "Should have the expected size for {}",
        expected_path,
    );
    Ok(())
}

#[allow(irrefutable_let_patterns)]
pub async fn test_list_files(fs: &Fs, _context: &TestContext) -> TestResult<()> {
    async fn test_list<'a>(
        fs: &'a Fs,
        path: &'static str,
        mut files: Vec<(&'static str, FsFileType, u64)>,
    ) -> TestResult<()> {
        let mut results = fs
            .list_files(FsPath::new(path)?)
            .await?
            .try_collect::<Vec<FsFile>>()
            .await?;
        results.sort();
        files.sort();

        test_assert_eq!(
            results.len(),
            files.len(),
            "Should have seen the right number of results.",
        );

        for _ in 0..files.len() {
            let result = results.remove(0);
            let (pathstr, file_type, size) = files.remove(0);
            compare_file(&result, FsPath::new(pathstr)?, file_type, size)?;
        }

        Ok(())
    }

    let mut allfiles = vec![
        ("/largefile", FsFileType::File, 100 * MB),
        ("/mediumfile", FsFileType::File, 5 * MB),
        ("/smallfile.txt", FsFileType::File, 27),
        ("/dir2/0foo", FsFileType::File, 0),
        ("/dir2/1bar", FsFileType::File, 0),
        ("/dir2/5diz", FsFileType::File, 0),
        ("/dir2/bar", FsFileType::File, 0),
        ("/dir2/daz", FsFileType::File, 300),
        ("/dir2/foo", FsFileType::File, 0),
        ("/dir2/hop", FsFileType::File, 0),
        ("/dir2/yu", FsFileType::File, 0),
    ];

    if fs.backend_type() == Backend::File {
        allfiles.extend(vec![
            ("/dir2/", FsFileType::Directory, 0),
            ("/maybedir/", FsFileType::Directory, 0),
            ("/maybedir/foo", FsFileType::File, 0),
            ("/maybedir/bar", FsFileType::File, 0),
            ("/maybedir/baz", FsFileType::File, 0),
            ("/maybedir/foobar/", FsFileType::Directory, 0),
            ("/maybedir/foobar/foo", FsFileType::File, 0),
            ("/maybedir/foobar/bar", FsFileType::File, 0),
        ])
    } else {
        allfiles.extend(vec![("/maybedir", FsFileType::File, 0)])
    }

    test_list(fs, "/", allfiles).await?;

    test_list(
        fs,
        "/dir2/",
        vec![
            ("/dir2/0foo", FsFileType::File, 0),
            ("/dir2/1bar", FsFileType::File, 0),
            ("/dir2/5diz", FsFileType::File, 0),
            ("/dir2/bar", FsFileType::File, 0),
            ("/dir2/daz", FsFileType::File, 300),
            ("/dir2/foo", FsFileType::File, 0),
            ("/dir2/hop", FsFileType::File, 0),
            ("/dir2/yu", FsFileType::File, 0),
        ],
    )
    .await?;

    Ok(())
}

#[allow(irrefutable_let_patterns)]
pub async fn test_get_file(fs: &Fs, _context: &TestContext) -> TestResult<()> {
    async fn test_pass(
        fs: &Fs,
        path: &str,
        expected_type: FsFileType,
        size: u64,
    ) -> TestResult<()> {
        let expected_path = FsPath::new(path)?;
        let file = fs.get_file(expected_path.clone()).await?;
        compare_file(&file, expected_path, expected_type, size)?;

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

    test_pass(fs, "/largefile", FsFileType::File, 100 * MB).await?;
    test_pass(fs, "/smallfile.txt", FsFileType::File, 27).await?;
    test_pass(fs, "/dir2/0foo", FsFileType::File, 0).await?;
    test_pass(fs, "/dir2/daz", FsFileType::File, 300).await?;

    test_fail(fs, "/daz").await?;
    test_fail(fs, "/foo/bar").await?;

    if fs.backend_type() == Backend::File {
        test_pass(fs, "/maybedir", FsFileType::Directory, 0).await?;
    } else {
        test_fail(fs, "/dir2").await?;
        test_fail(fs, "/maybedir").await?;
    }

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
                    return Err(e.into());
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
