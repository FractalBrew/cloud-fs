# file-store

[![Azure DevOps build status](https://img.shields.io/azure-devops/build/FractalBrew/4572c68a-521b-44c3-bf78-88b056b098be/1)](https://dev.azure.com/FractalBrew/File%20Store/_build?definitionId=1)
[![Code coverage](https://img.shields.io/coveralls/github/FractalBrew/file-store-rs)](https://coveralls.io/github/FractalBrew/file-store-rs?branch=master)
[![Open issues](https://img.shields.io/github/issues-raw/FractalBrew/file-store-rs)](https://github.com/FractalBrew/file-store-rs/issues)
![Code status](https://img.shields.io/badge/status-pre--alpha-red)

`file-store` is a Rust library providing asynchronous file storage. The files may be hosted locally or remotely. Different backends provide access to different storage systems including the local filesystem and storage in various cloud providers.

The public API for reading and writing files is identical regardless of the chosen storage backend..

## Backends

The currently provided backends are:

* FileBackend allows accessing files within a directory on the local computer.
* B2Backend allows accessing files stored on Backblaze B2.

It is possible to choose which backends are included in the library based on cargo features. The default is to include all backends and so in order to reduce the set you must disable the default features and then list all of the backends you want.
