//! An asynchronous API for accessing files that could be on any of a number of
//! different storage backends.
//!
//! The API offers functions for listing, reading, writing and deleting files
//! from a storage backend. Each backend offers the same API plus in some cases
//! additional backend specific functionality.
//!
//! Obviously offering the same API across all backends means the API is fairly
//! basic, but if all you want to do is write, read or list files it should be
//! plenty. Past that some of the backends provide access to internal functions.
//! You can get the backend implementation via the `TryFrom` train.
//!
//! Which backend is available depends on the features that file-store is
//! compiled with. See the [`backends`](backends/index.html) module.
//!
//! The [FileStore](struct.FileStore.html) is the main struct used to access
//! storage. A [FileStore](struct.FileStore.html) is created from one of the
//! backend specific structs.
#![warn(missing_docs)]

#[macro_use]
pub mod backends;
pub mod executor;
mod filestore;
mod types;

pub use filestore::FileStore;
pub use types::*;
