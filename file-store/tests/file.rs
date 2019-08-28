#![cfg(feature = "file")]
#![allow(clippy::needless_lifetimes)]

extern crate file_store;

#[macro_use]
mod runner;

mod dir1 {
    use crate::runner::{TestContext, TestResult};
    use file_store::backends::file::FileBackend;
    use file_store::backends::Backend;
    use file_store::FileStore;

    async fn build_fs(context: &TestContext) -> TestResult<(FileStore, ())> {
        Ok((FileBackend::connect(&context.get_fs_root()).await?, ()))
    }

    async fn cleanup(_: ()) -> TestResult<()> {
        Ok(())
    }

    build_tests!("test1/dir1", Backend::File, build_fs, cleanup);
}

mod test1 {
    use crate::runner::{TestContext, TestResult};
    use file_store::backends::file::FileBackend;
    use file_store::backends::Backend;
    use file_store::FileStore;

    async fn build_fs(context: &TestContext) -> TestResult<(FileStore, ())> {
        Ok((FileBackend::connect(&context.get_fs_root()).await?, ()))
    }

    async fn cleanup(_: ()) -> TestResult<()> {
        Ok(())
    }

    build_tests!("test1", Backend::File, build_fs, cleanup);
}
