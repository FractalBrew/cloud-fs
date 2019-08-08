//! Accesses files in a Backblaze B2 bucket. Included with the feature "b2".
//!
//! The [`B2Backend`](struct.B2Backend.html) can be initialized with as little
//! as a key id and key (these can be the master key or an application key). It
//! also supports a [`builder`](struct.B2Backend.html#method.builder) pattern to
//! add additional configuration including a root path to restrict the files
//! visible.
//!
//! [`ObjectPath`](../../struct.ObjectPath.html)'s represent the names of files.
//! The first directory part of a path (the string up until the first `/`) is
//! use as the name of the bucket. The rest can be freeform though people
//! generally use a regular path string separated by `/` characters to form
//! a hierarchy. Attempting to write a file at the bucket level will fail
//! however writing a file inside a bucket that does not yet exist will create
//! the bucket (assuming the key has permission to do so).
//!
//! In order to be compatible with other backends, but still include some useful
//! functionality file versioning (if enabled for the bucket) is currently
//! handled as follows:
//! * Deleting a file will delete all of its versions.
//! * Replacing a file will add a new version.
//!
//! Setting a file's mimetype on uploas is not currently supported. The backend
//! will rely on B2's automatic mimetype detection to set the mimetype. This
//! uses the file's extension to set a mimetype from a [list of mappings](https://www.backblaze.com/b2/docs/content-types.html)
//! and falls back to `application/octet-stream` in case of failure.
//!
//! The last modified time of an uploaded file will be set to the time that the
//! upload began.
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::Arc;

use base64::encode;
use futures::compat::*;
use futures::lock::Mutex;
use http::request;
use hyper::body::Body;
use hyper::client::connect::HttpConnector;
use hyper::client::Client as HyperClient;
use hyper::Request;
use hyper_tls::HttpsConnector;
use serde_json::from_reader;

use storage_types::b2::responses::*;

use super::{Backend, BackendImplementation, StorageImpl};
use crate::filestore::FileStore;
use crate::types::*;

type Client = HyperClient<HttpsConnector<HttpConnector>>;

const DEFAULT_API_HOST: &str = "https://api.backblazeb2.com";
const API_VERSION: &str = "2";

impl From<http::Error> for StorageError {
    fn from(error: http::Error) -> StorageError {
        error::other_error(&error.to_string(), Some(error))
    }
}

impl From<hyper::error::Error> for StorageError {
    fn from(error: hyper::error::Error) -> StorageError {
        if error.is_parse() || error.is_user() {
            error::invalid_data(&error.to_string(), Some(error))
        } else if error.is_canceled() {
            error::cancelled(&error.to_string(), Some(error))
        } else if error.is_closed() {
            error::connection_closed(&error.to_string(), Some(error))
        } else if error.is_connect() {
            error::connection_failed(&error.to_string(), Some(error))
        } else if error.is_incomplete_message() {
            error::connection_closed(&error.to_string(), Some(error))
        } else {
            error::invalid_data(&error.to_string(), Some(error))
        }
    }
}

#[derive(Clone, Debug)]
struct B2Settings {
    key_id: String,
    key: String,
    host: String,
}

#[derive(Clone, Debug)]
struct B2Client {
    client: Client,
    settings: B2Settings,
    session: Arc<Mutex<Option<AuthorizeAccountResponse>>>,
}

impl B2Client {
    async fn build(settings: B2Settings) -> StorageResult<B2Client> {
        let connector = match HttpsConnector::new(4) {
            Ok(c) => c,
            Err(e) => {
                return Err(error::connection_failed(
                    "Could not create http connection.",
                    Some(e),
                ))
            }
        };

        let client = HyperClient::builder().build(connector);

        let b2_client = B2Client {
            client,
            settings,
            session: Arc::new(Mutex::new(None)),
        };

        b2_client.session().await?;
        Ok(b2_client)
    }

    fn api_url(&self, host: &str, method: &str) -> String {
        format!("{}/b2api/v{}/{}", host, API_VERSION, method)
    }

    async fn request<R>(
        &self,
        session: Option<AuthorizeAccountResponse>,
        method: &str,
        mut builder: request::Builder,
        body: Body,
    ) -> StorageResult<R>
    where
        for<'de> R: serde::de::Deserialize<'de>,
    {
        match session {
            Some(sess) => {
                builder
                    .uri(self.api_url(&sess.api_url, method))
                    .header("Authorization", sess.authorization_token);
            }
            None => {
                let secret = format!(
                    "Basic {}",
                    encode(&format!("{}:{}", self.settings.key_id, self.settings.key))
                );
                builder
                    .uri(self.api_url(&self.settings.host, method))
                    .header("Authorization", secret);
            }
        };

        let request = builder.body(body)?;

        let response = self.client.request(request).compat().await?;
        let (meta, body) = response.into_parts();

        if meta.status.is_success() {
            match from_reader(BlockingStreamReader::from_stream(body.compat())) {
                Ok(r) => Ok(r),
                Err(e) => Err(error::invalid_data(
                    &format!("Unable to parse response from {}.", method),
                    Some(e),
                )),
            }
        } else {
            let error: ErrorResponse =
                match from_reader(BlockingStreamReader::from_stream(body.compat())) {
                    Ok(r) => r,
                    Err(e) => {
                        return Err(error::invalid_data(
                            &format!("Unable to parse error response from {}.", method),
                            Some(e),
                        ))
                    }
                };

            match (error.status, error.code.as_str()) {
                (400, "bad_request") => {
                    Err(error::internal_error::<StorageError>(&error.message, None))
                }
                (401, "unauthorized") => Err(error::access_denied::<StorageError>(
                    "The application key id or key were not recognized.",
                    None,
                )),
                (401, "bad_auth_token") => Err(error::access_denied::<StorageError>(
                    "The application key id or key were not recognized.",
                    None,
                )),
                (401, "unsupported") => {
                    Err(error::internal_error::<StorageError>(&error.message, None))
                }
                _ => Err(error::other_error::<StorageError>(
                    &format!(
                        "Unknown B2 API failure {}: {}, {}",
                        error.status, error.code, error.message
                    ),
                    None,
                )),
            }
        }
    }

    async fn b2_authorize_account(&self) -> StorageResult<AuthorizeAccountResponse> {
        let mut builder = Request::builder();
        builder.method("GET");

        self.request(None, "b2_authorize_account", builder, Body::empty())
            .await
    }

    async fn session(&self) -> StorageResult<AuthorizeAccountResponse> {
        let mut session = self.session.lock().await;
        if let Some(ref s) = session.deref() {
            Ok(s.clone())
        } else {
            let new_session = self.b2_authorize_account().await?;
            session.replace(new_session.clone());
            Ok(new_session)
        }
    }
}

/// The backend implementation for B2 storage.
#[derive(Debug, Clone)]
pub struct B2Backend {
    settings: B2Settings,
    client: B2Client,
}

impl B2Backend {
    /// Creates a new [`FileStore`](../../struct.FileStore.html) instance using the
    /// b2 backend.
    ///
    /// When constructed in this manner the root for all paths will be at the
    /// account level.
    pub fn connect(key_id: &str, key: &str) -> ConnectFuture {
        B2Backend::builder(key_id, key).build()
    }

    /// Creates a new [`B2BackendBuilder`](struct.B2BackendBuilder.html).
    pub fn builder(key_id: &str, key: &str) -> B2BackendBuilder {
        B2BackendBuilder {
            settings: B2Settings {
                key_id: key_id.to_owned(),
                key: key.to_owned(),
                host: DEFAULT_API_HOST.to_owned(),
            },
        }
    }
}

#[derive(Debug, Clone)]
/// Used to build a [`B2Backend`](struct.B2Backend.html) with some custom
/// settings.
pub struct B2BackendBuilder {
    settings: B2Settings,
}

impl B2BackendBuilder {
    /// Sets the API host for B2.
    ///
    /// This is generally only used for testing purposes.
    pub fn host(mut self, host: &str) -> B2BackendBuilder {
        self.settings.host = host.to_owned();
        self
    }

    /// Creates a new B2 based [`FileStore`](../../struct.FileStore.html) using
    /// this builder's settings.
    pub fn build(self) -> ConnectFuture {
        ConnectFuture::from_future(async {
            let client = B2Client::build(self.settings.clone()).await?;

            Ok(FileStore {
                backend: BackendImplementation::B2(Box::new(B2Backend {
                    settings: self.settings,
                    client,
                })),
            })
        })
    }
}

impl TryFrom<FileStore> for B2Backend {
    type Error = StorageError;

    fn try_from(file_store: FileStore) -> StorageResult<B2Backend> {
        if let BackendImplementation::B2(b) = file_store.backend {
            Ok(b.deref().clone())
        } else {
            Err(error::invalid_settings::<StorageError>(
                "FileStore does not hold a FileBackend",
                None,
            ))
        }
    }
}

impl StorageImpl for B2Backend {
    fn backend_type(&self) -> Backend {
        Backend::B2
    }

    fn list_objects(&self, _path: ObjectPath) -> ObjectStreamFuture {
        unimplemented!();
    }

    fn get_object(&self, _path: ObjectPath) -> ObjectFuture {
        unimplemented!();
    }

    fn get_file_stream(&self, _path: ObjectPath) -> DataStreamFuture {
        unimplemented!();
    }

    fn delete_object(&self, _path: ObjectPath) -> OperationCompleteFuture {
        unimplemented!();
    }

    fn write_file_from_stream(
        &self,
        _path: ObjectPath,
        _stream: DataStream,
    ) -> WriteCompleteFuture {
        unimplemented!();
    }
}
