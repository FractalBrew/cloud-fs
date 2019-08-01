//! Accesses files in a Backblaze B2 bucket. Included with the feature "b2".
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::Arc;

use base64::encode;
use futures::compat::*;
use futures::lock::Mutex;
use hyper::body::Body;
use hyper::client::connect::HttpConnector;
use hyper::client::Client as HyperClient;
use hyper::Request;
use hyper_tls::HttpsConnector;

use super::{Backend, BackendImplementation, StorageImpl};
use crate::filestore::FileStore;
use crate::types::*;

type Client = HyperClient<HttpsConnector<HttpConnector>>;

const DEFAULT_API_HOST: &str = "https://api.backblazeb2.com";
const API_VERSION: &str = "2";

impl From<http::Error> for StorageError {
    fn from(error: http::Error) -> StorageError {
        error::other_error(&format!("{}", error), Some(error))
    }
}

impl From<hyper::error::Error> for StorageError {
    fn from(error: hyper::error::Error) -> StorageError {
        if error.is_parse() || error.is_user() {
            error::invalid_data(&format!("{}", error), Some(error))
        } else if error.is_canceled() {
            error::cancelled(&format!("{}", error), Some(error))
        } else if error.is_closed() {
            error::connection_closed(&format!("{}", error), Some(error))
        } else if error.is_connect() {
            error::connection_failed(&format!("{}", error), Some(error))
        } else if error.is_incomplete_message() {
            error::connection_closed(&format!("{}", error), Some(error))
        } else {
            error::invalid_data(&format!("{}", error), Some(error))
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
struct B2Session {
    authorization_token: String,
    host: String,
}

#[derive(Clone, Debug)]
struct B2Client {
    client: Client,
    settings: B2Settings,
    session: Arc<Mutex<Option<B2Session>>>,
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

    async fn start_session(&self) -> StorageResult<B2Session> {
        let secret = format!(
            "Basic {}",
            encode(&format!("{}:{}", self.settings.key_id, self.settings.key))
        );
        let request = Request::get(self.api_url(&self.settings.host, "b2_authorize_account"))
            .header("Authorization", secret)
            .body(Body::empty())?;

        let response = self.client.request(request).compat().await?;

        Err(error::other_error::<StorageError>("foo", None))
    }

    async fn session(&self) -> StorageResult<B2Session> {
        let mut session = self.session.lock().await;
        if let Some(ref s) = session.deref() {
            Ok(s.clone())
        } else {
            let new_session = self.start_session().await?;
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
    /// Creates a new [`FileStore`](../struct.FileStore.html) instance using the
    /// b2 backend.
    pub fn connect(key_id: &str, key: &str) -> ConnectFuture {
        B2BackendBuilder::new(key_id, key).build()
    }
}

#[derive(Debug, Clone)]
pub struct B2BackendBuilder {
    settings: B2Settings,
}

impl B2BackendBuilder {
    pub fn new(key_id: &str, key: &str) -> B2BackendBuilder {
        B2BackendBuilder {
            settings: B2Settings {
                key_id: key_id.to_owned(),
                key: key.to_owned(),
                host: DEFAULT_API_HOST.to_owned(),
            },
        }
    }

    pub fn build(self) -> ConnectFuture {
        ConnectFuture::from_future(async {
            let client = B2Client::build(self.settings.clone()).await?;

            Ok(FileStore {
                backend: BackendImplementation::B2(B2Backend {
                    settings: self.settings,
                    client,
                }),
            })
        })
    }
}

impl TryFrom<FileStore> for B2Backend {
    type Error = StorageError;

    fn try_from(file_store: FileStore) -> StorageResult<B2Backend> {
        if let BackendImplementation::B2(b) = file_store.backend {
            Ok(b)
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
