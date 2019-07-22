//! An asynchronous API for accessing files that could be on any of a number of different storage backends.
//!
//! The API offers functions for listing, reading, writing and deleting files
//! from a storage backend. Each backend offers the same API plus in some cases
//! additional backend specific functionality.
//!
//! Obviously offering the same API across all backends means the API is fairly
//! basic, but if all you want to do is write, read or list files it should be
//! plenty. Past that some of the backends provide access to internal functions.
//! You can get the backend via the `TryFrom` train.
//!
//! Which backend is available depends on the features cloud-fs is compiled
//! with. See the [`backends`](backends/index.html) module.
//!
//! The [FileStore](struct.FileStore.html) is the main API used to access storage.
#![warn(missing_docs)]
#![feature(async_await)]

#[macro_use]
pub mod backends;
pub mod executor;
mod filestore;
mod types;

pub use filestore::FileStore;
pub use types::*;
