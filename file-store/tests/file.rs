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

#![cfg(feature = "file")]

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
