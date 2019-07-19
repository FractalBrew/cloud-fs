//! An abstract asynchronous API for accessing a filesystem that could be on any of a number of different local and cloud storage backends.
//!
//! The API offers functions for listing, reading, writing and deleting files
//! from a storage backend. Each backend offers the same API plus in some cases
//! some additional backend specific functionality.
//!
//! Obviously offering the same API across all backends means the API is fairly
//! basic, but if all you want to do is write, read or list files it should be
//! plenty. Past that some of the backends provide access to internal functions
//! via a `from_fs` function.
//!
//! Which backend is available depends on the features cloud-fs is compiled
//! with. See the [`backends`](backends/index.html) module.
//!
//! The [`Fs`](struct.Fs.html) is the main API used to access storage.
#![warn(missing_docs)]
#![feature(async_await)]

pub mod backends;
pub mod executor;
mod fs;
mod types;

pub use fs::Fs;
pub use types::*;
