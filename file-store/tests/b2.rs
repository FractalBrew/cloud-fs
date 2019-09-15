#![cfg(feature = "b2")]

extern crate file_store;

#[macro_use]
mod runner;
mod mocks;

mod test1 {
    use futures::channel::oneshot::Sender;

    use file_store::backends::b2::B2Backend;
    use file_store::backends::Backend;
    use file_store::FileStore;

    use crate::mocks::b2_server::start_server;
    use crate::runner::{TestContext, TestError, TestResult};

    async fn build_fs(context: &TestContext) -> TestResult<(FileStore, Sender<()>)> {
        let (addr, sender) = start_server(context.get_fs_root(), 6)?;

        let fs = B2Backend::builder("foo", "bar")
            .host(&format!("http://{}", addr))
            .limit_small_file_size(20 * 1024 * 1024)
            .limit_requests(5)
            .connect()
            .await?;
        Ok((fs, sender))
    }

    async fn cleanup(sender: Sender<()>) -> TestResult<()> {
        sender.send(()).map_err(|()| {
            TestError::HarnessFailure(String::from("Failed to send shutdown to mock b2 server."))
        })
    }

    build_tests!("test1", Backend::B2, build_fs, cleanup);
}

mod retries {
    use futures::channel::oneshot::Sender;

    use file_store::backends::b2::B2Backend;
    use file_store::backends::Backend;
    use file_store::FileStore;

    use crate::mocks::b2_server::start_server;
    use crate::runner::{TestContext, TestError, TestResult};

    async fn build_fs(context: &TestContext) -> TestResult<(FileStore, Sender<()>)> {
        let (addr, sender) = start_server(context.get_fs_root(), 2)?;

        let fs = B2Backend::builder("foo", "bar")
            .host(&format!("http://{}", addr))
            .limit_small_file_size(20 * 1024 * 1024)
            .limit_requests(2)
            .connect()
            .await?;
        Ok((fs, sender))
    }

    async fn cleanup(sender: Sender<()>) -> TestResult<()> {
        sender.send(()).map_err(|()| {
            TestError::HarnessFailure(String::from("Failed to send shutdown to mock b2 server."))
        })
    }

    build_tests!("test1", Backend::B2, build_fs, cleanup);
}
