# cloud-fs

[![Build Status](https://travis-ci.org/FractalBrew/cloud-fs.svg?branch=master)](https://travis-ci.org/FractalBrew/cloud-fs)
[![Coverage Status](https://coveralls.io/repos/github/FractalBrew/cloud-fs/badge.svg?branch=master)](https://coveralls.io/github/FractalBrew/cloud-fs?branch=master)

`cloud-fs` is a Rust library providing asynchronous access to filesystems that may be hosted locally or remotely. Different backends provide access to different types of filesystems including local files and files held on cloud storage.

Aside from the code used to instantiate a backend (and even that is pretty similar) the actual API for reading and writing files to the backend is identical.

## Backends

The currently provided backends are:

* FileBackend allows accessing files within a directory on the local computer.
* B2Backend allows accessing files stored on Backblaze B2.

It is possible to choose which backends are included in the library based on cargo features. The default is to include all backends and so in order to reduce the set you must disable the default features and then list all of the backends you want.