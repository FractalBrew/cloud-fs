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

//! Contains the different storage backend implementations.
//!
//! Each backend allows for accessing files in a different storage system.
//! Normally you just crate a [`FileStore`](../enum.FileStore.html) from the
//! backend and then everything else is done by calls to the `FileStore` which
//! generally behave the same regardless of the backend.
#[cfg(feature = "b2")]
pub mod b2;
#[cfg(feature = "file")]
pub mod file;

use std::fmt;

/// An enumeration of the available backends.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Backend {
    #[cfg(feature = "file")]
    /// The [file backend](file/index.html). Included with the "file" feature.
    File,
    #[cfg(feature = "b2")]
    /// The [b2 backend](b2/index.html). Included with the "b2" feature.
    B2,
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            #[cfg(feature = "file")]
            Backend::File => f.pad("file"),
            #[cfg(feature = "b2")]
            Backend::B2 => f.pad("b2"),
        }
    }
}
