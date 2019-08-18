//! Accesses files in a Backblaze B2 bucket. Included with the feature "b2".
//!
//! The [`B2Backend`](struct.B2Backend.html) can be initialized with as little
//! as a key id and key (these can be the master key or an application key). It
//! also supports a [`builder`](struct.B2Backend.html#method.builder) pattern to
//! add additional configuration including a path prefix to restrict the files
//! visible.
//!
//! [`ObjectPath`](../../struct.ObjectPath.html)'s represent the names of files.
//! The first directory part of a path (the string up until the first `/`) is
//! used as the name of the bucket. The rest can be freeform though people
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
use std::convert::{TryFrom, TryInto};
use std::future::Future;
use std::io::Read;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use base64::encode;
use bytes::IntoBuf;
use futures::compat::*;
use futures::future::{ready, TryFutureExt};
use futures::lock::Mutex;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use http::method::Method;
use hyper::body::Body;
use hyper::client::connect::HttpConnector;
use hyper::client::Client as HyperClient;
use hyper::{Request, Response};
use hyper_tls::HttpsConnector;
use serde::de::DeserializeOwned;
use serde_json::{from_str, to_string};

use storage_types::b2::v2::requests::*;
use storage_types::b2::v2::responses::*;

use super::{Backend, BackendImplementation, ObjectInternals, StorageBackend};
use crate::filestore::FileStore;
use crate::types::stream::{MergedStreams, ResultStreamPoll};
use crate::types::*;

type Client = HyperClient<HttpsConnector<HttpConnector>>;

const API_RETRIES: usize = 3;

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

impl From<serde_json::error::Error> for StorageError {
    fn from(error: serde_json::error::Error) -> StorageError {
        error::internal_error("Failes to encode request data.", Some(error))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct B2ObjectInternals {
    infos: Vec<FileInfo>,
}

impl B2ObjectInternals {
    fn new(mut infos: Vec<FileInfo>) -> B2ObjectInternals {
        infos.sort_by(|a, b| a.upload_timestamp.cmp(&b.upload_timestamp));

        B2ObjectInternals { infos }
    }

    fn latest(&self) -> &FileInfo {
        &self.infos[self.infos.len() - 1]
    }

    fn file_name(&self) -> &str {
        &self.latest().file_name
    }

    fn size(&self) -> u64 {
        self.latest().content_length
    }
}

fn new_object(bucket: &str, info: B2ObjectInternals, prefix: &ObjectPath) -> StorageResult<Object> {
    let mut path = ObjectPath::new(info.file_name())?;
    path.shift_part(bucket);
    let is_dir = path.is_dir_prefix();

    let o_type = if is_dir {
        path.pop_part();
        ObjectType::Directory
    } else {
        ObjectType::File
    };

    for _ in prefix.parts() {
        path.unshift_part();
    }

    Ok(Object {
        object_type: o_type,
        path,
        size: info.size(),
        internals: ObjectInternals::B2(info),
    })
}

#[derive(Clone, Debug)]
struct B2Settings {
    key_id: String,
    key: String,
    host: String,
    prefix: ObjectPath,
}

macro_rules! b2_api {
    ($method:ident, $request:ident, $response:ident) => {
        #[allow(dead_code)]
        pub fn $method(
            &self,
            path: ObjectPath,
            request: $request,
        ) -> impl Future<Output = StorageResult<$response>> {
            self.clone().b2_api_call(stringify!($method), path, request)
        }
    }
}

#[derive(Clone, Debug)]
struct B2Client {
    client: Client,
    settings: B2Settings,
    session: Arc<Mutex<Option<AuthorizeAccountResponse>>>,
}

impl B2Client {
    fn api_url(&self, host: &str, method: &str) -> String {
        format!("{}/b2api/{}/{}", host, B2_VERSION, method)
    }

    async fn account_id(&self) -> StorageResult<String> {
        let session = self.session().await?;
        Ok(session.account_id)
    }

    async fn request(
        &self,
        method: &str,
        path: ObjectPath,
        request: Request<Body>,
    ) -> StorageResult<Response<Body>> {
        let response = self.client.request(request).compat().await?;

        if response.status().is_success() {
            Ok(response)
        } else {
            let (_, body) = response.into_parts();

            let mut data: String = String::new();
            BlockingStreamReader::from_stream(body.compat())
                .read_to_string(&mut data)
                .unwrap();
            Err(generate_error(method, &path, &data))
        }
    }

    async fn basic_request<R>(
        &self,
        method: &str,
        path: ObjectPath,
        request: Request<Body>,
    ) -> StorageResult<R>
    where
        R: DeserializeOwned,
    {
        let response = self.request(method, path, request).await?;
        let (_, body) = response.into_parts();

        let mut data: String = String::new();
        BlockingStreamReader::from_stream(body.compat())
            .read_to_string(&mut data)
            .unwrap();

        match from_str(&data) {
            Ok(r) => Ok(r),
            Err(e) => Err(error::invalid_data(
                &format!("Unable to parse response from {}.", method),
                Some(e),
            )),
        }
    }

    async fn b2_authorize_account(&self) -> StorageResult<AuthorizeAccountResponse> {
        let secret = format!(
            "Basic {}",
            encode(&format!("{}:{}", self.settings.key_id, self.settings.key))
        );

        let request = Request::builder()
            .method(Method::GET)
            .uri(self.api_url(&self.settings.host, "b2_authorize_account"))
            .header("Authorization", secret)
            .body(Body::empty())?;

        let empty = ObjectPath::empty();
        self.basic_request("b2_authorize_account", empty, request)
            .await
    }

    async fn b2_api_call<S, Q>(self, method: &str, path: ObjectPath, request: S) -> StorageResult<Q>
    where
        S: serde::ser::Serialize + Clone,
        for<'de> Q: serde::de::Deserialize<'de>,
    {
        let mut tries: usize = 0;
        loop {
            let (api_url, authorization) = {
                let session = self.session().await?;
                (session.api_url.clone(), session.authorization_token.clone())
            };

            let data = to_string(&request)?;

            let request = Request::builder()
                .method(Method::POST)
                .uri(self.api_url(&api_url, method))
                .header("Authorization", &authorization)
                .body(data.into())?;

            match self.basic_request(method, path.clone(), request).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    if e.kind() == error::StorageErrorKind::AccessExpired {
                        self.reset_session(&authorization).await;

                        tries += 1;
                        if tries < API_RETRIES {
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn b2_download_file_by_name(
        self,
        path: ObjectPath,
        bucket: String,
        file: String,
    ) -> StorageResult<Body> {
        let mut tries: usize = 0;
        loop {
            let (download_url, authorization) = {
                let session = self.session().await?;
                (
                    session.download_url.clone(),
                    session.authorization_token.clone(),
                )
            };

            let request = Request::builder()
                .method(Method::GET)
                .uri(format!("{}/file/{}/{}", download_url, bucket, file))
                .header("Authorization", &authorization)
                .body(Body::empty())?;

            match self
                .request("b2_download_file_by_name", path.clone(), request)
                .await
            {
                Ok(response) => {
                    let (_, body) = response.into_parts();
                    return Ok(body);
                }
                Err(e) => {
                    if e.kind() == error::StorageErrorKind::AccessExpired {
                        self.reset_session(&authorization).await;

                        tries += 1;
                        if tries < API_RETRIES {
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    b2_api!(b2_list_buckets, ListBucketsRequest, ListBucketsResponse);
    b2_api!(b2_get_file_info, GetFileInfoRequest, GetFileInfoResponse);
    b2_api!(
        b2_list_file_names,
        ListFileNamesRequest,
        ListFileNamesResponse
    );
    b2_api!(
        b2_list_file_versions,
        ListFileVersionsRequest,
        ListFileVersionsResponse
    );

    async fn reset_session(&self, auth_token: &str) {
        let mut session = self.session.lock().await;
        if let Some(ref s) = session.deref() {
            if s.authorization_token == auth_token {
                session.take();
            }
        }
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

trait ListRequestor<S>
where
    S: Send + 'static,
{
    fn next_request(&self) -> Option<WrappedFuture<StorageResult<S>>>;
    fn take_response(&mut self, response: S) -> Vec<FileInfo>;
}

/*struct FileNamesRequestor {
    client: B2Client,
    path: ObjectPath,
    options: Option<ListFileNamesRequest>,
}

impl FileNamesRequestor {
    fn new(
        client: B2Client,
        path: ObjectPath,
        options: ListFileNamesRequest,
    ) -> FileNamesRequestor {
        FileNamesRequestor {
            client,
            path,
            options: Some(options),
        }
    }
}

impl ListRequestor<ListFileNamesResponse> for FileNamesRequestor {
    fn next_request(&self) -> Option<WrappedFuture<StorageResult<ListFileNamesResponse>>> {
        self.options.as_ref().map(|o| {
            WrappedFuture::<StorageResult<ListFileNamesResponse>>::from_future(
                self.client.b2_list_file_names(self.path.clone(), o.clone()),
            )
        })
    }

    fn take_response(&mut self, response: ListFileNamesResponse) -> Vec<FileInfo> {
        if response.next_file_name.is_none() {
            self.options = None;
        } else if let Some(ref mut options) = self.options {
            options.start_file_name = response.next_file_name;
        }

        response.files
    }
}*/

struct FileVersionsRequestor {
    client: B2Client,
    path: ObjectPath,
    options: Option<ListFileVersionsRequest>,
}

impl FileVersionsRequestor {
    fn new(
        client: B2Client,
        path: ObjectPath,
        options: ListFileVersionsRequest,
    ) -> FileVersionsRequestor {
        FileVersionsRequestor {
            client,
            path,
            options: Some(options),
        }
    }
}

impl ListRequestor<ListFileVersionsResponse> for FileVersionsRequestor {
    fn next_request(&self) -> Option<WrappedFuture<StorageResult<ListFileVersionsResponse>>> {
        self.options.as_ref().map(|o| {
            WrappedFuture::<StorageResult<ListFileNamesResponse>>::from_future(
                self.client
                    .b2_list_file_versions(self.path.clone(), o.clone()),
            )
        })
    }

    fn take_response(&mut self, response: ListFileVersionsResponse) -> Vec<FileInfo> {
        if response.next_file_name.is_none() {
            self.options = None;
        } else if let Some(ref mut options) = self.options {
            options.start_file_name = response.next_file_name;
            options.start_file_id = response.next_file_id;
        }

        response.files
    }
}

/// A stream of objects from B2.
struct ListStream<R, S>
where
    R: ListRequestor<S> + Unpin + Send + 'static,
    S: Send + 'static,
{
    current: Vec<FileInfo>,
    results: Vec<FileInfo>,
    requestor: R,
    future: Option<Pin<Box<WrappedFuture<StorageResult<S>>>>>,
}

impl<R, S> ListStream<R, S>
where
    R: ListRequestor<S> + Send + Unpin + 'static,
    S: Send + 'static,
{
    fn new(requestor: R) -> ListStream<R, S> {
        ListStream {
            requestor,
            current: Vec::new(),
            results: Vec::new(),
            future: None,
        }
    }

    fn poll_next_info(&mut self, cx: &mut Context) -> ResultStreamPoll<FileInfo> {
        loop {
            if !self.results.is_empty() {
                return Poll::Ready(Some(Ok(self.results.remove(0))));
            } else if let Some(ref mut fut) = self.future {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(response)) => {
                        self.future = None;
                        self.results = self.requestor.take_response(response);
                    }
                    Poll::Ready(Err(e)) => {
                        self.future = None;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            } else if let Some(fut) = self.requestor.next_request() {
                self.future = Some(Box::pin(fut));
            } else {
                return Poll::Ready(None);
            }
        }
    }
}

impl<R, S> Stream for ListStream<R, S>
where
    R: ListRequestor<S> + Send + Unpin + 'static,
    S: Send + 'static,
{
    type Item = StorageResult<B2ObjectInternals>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> ResultStreamPoll<B2ObjectInternals> {
        match self.poll_next_info(cx) {
            Poll::Ready(Some(Ok(info))) => {
                if self.current.is_empty() {
                    self.current.push(info);
                    self.poll_next(cx)
                } else if self.current[0].file_name == info.file_name {
                    self.current.push(info);
                    self.poll_next(cx)
                } else {
                    let internals = B2ObjectInternals::new(self.current.drain(..).collect());
                    self.current.push(info);
                    Poll::Ready(Some(Ok(internals)))
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                if !self.current.is_empty() {
                    let internals = B2ObjectInternals::new(self.current.drain(..).collect());
                    Poll::Ready(Some(Ok(internals)))
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The backend implementation for B2 storage.
#[derive(Debug, Clone)]
pub struct B2Backend {
    settings: B2Settings,
    client: Client,
    session: Arc<Mutex<Option<AuthorizeAccountResponse>>>,
}

impl B2Backend {
    /// Creates a new [`FileStore`](../../struct.FileStore.html) instance using the
    /// b2 backend.
    ///
    /// When constructed in this manner the root for all paths will be at the
    /// account level.
    pub fn connect(key_id: &str, key: &str) -> ConnectFuture {
        B2Backend::builder(key_id, key).connect()
    }

    /// Creates a new [`B2BackendBuilder`](struct.B2BackendBuilder.html).
    pub fn builder(key_id: &str, key: &str) -> B2BackendBuilder {
        B2BackendBuilder {
            settings: B2Settings {
                key_id: key_id.to_owned(),
                key: key.to_owned(),
                host: B2_API_HOST.to_owned(),
                prefix: ObjectPath::empty(),
            },
        }
    }

    /// Creates a new [`B2Client`](struct.B2Client.html) that can be used for
    /// making B2 API calls.
    fn client(&self) -> B2Client {
        B2Client {
            settings: self.settings.clone(),
            client: self.client.clone(),
            session: self.session.clone(),
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

    /// Sets a path prefix for this storage.
    ///
    /// Essentially sets the 'root directory' for this storage, any paths
    /// requested will be joined with this with a `/` character in between, so
    /// this can be either the name of a bucket or a bucket followed by some
    /// directory parts within that bucket.
    pub fn prefix(mut self, prefix: ObjectPath) -> B2BackendBuilder {
        self.settings.prefix = prefix;
        self
    }

    /// Creates a new B2 based [`FileStore`](../../struct.FileStore.html) using
    /// this builder's settings.
    pub fn connect(self) -> ConnectFuture {
        ConnectFuture::from_future(async {
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

            let backend = B2Backend {
                settings: self.settings,
                client,
                session: Arc::new(Mutex::new(None)),
            };

            // Make sure we can connect.
            let b2_client = backend.client();
            b2_client.session().await?;

            Ok(FileStore {
                backend: BackendImplementation::B2(Box::new(backend)),
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

impl StorageBackend for B2Backend {
    fn backend_type(&self) -> Backend {
        Backend::B2
    }

    fn list_objects<P>(&self, prefix: P) -> ObjectStreamFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        async fn list(
            client: B2Client,
            backend_prefix: ObjectPath,
            prefix: ObjectPath,
        ) -> StorageResult<ObjectStream> {
            let mut file_part = backend_prefix.join(&prefix);
            let is_dir = file_part.is_dir_prefix();
            let bucket = file_part.unshift_part().unwrap_or_else(String::new);

            let mut request = ListBucketsRequest {
                account_id: client.account_id().await?,
                bucket_id: None,
                bucket_name: None,
                bucket_types: Default::default(),
            };

            if !file_part.is_empty() || is_dir {
                // Only include the bucket named `bucket`.
                request.bucket_name = Some(bucket.clone());
            }

            let path = ObjectPath::new(&bucket)?;
            let listers = client
                .b2_list_buckets(path, request)
                .await?
                .buckets
                .drain(..)
                .filter(|b| b.bucket_name.starts_with(&bucket))
                .map(move |b| {
                    let options = ListFileVersionsRequest {
                        bucket_id: b.bucket_id.clone(),
                        start_file_name: None,
                        start_file_id: None,
                        max_file_count: None,
                        prefix: Some(file_part.to_string()),
                        delimiter: None,
                    };

                    let requestor =
                        FileVersionsRequestor::new(client.clone(), prefix.clone(), options);
                    let temp_prefix = backend_prefix.clone();
                    ListStream::new(requestor)
                        .and_then(move |i| ready(new_object(&b.bucket_name, i, &temp_prefix)))
                })
                .fold(MergedStreams::new(), |mut m, s| {
                    m.push(s);
                    m
                });

            Ok(ObjectStream::from_stream(listers))
        }

        let prefix = match prefix.try_into() {
            Ok(p) => p,
            Err(e) => return ObjectStreamFuture::from_value(Err(e.into())),
        };

        ObjectStreamFuture::from_future(list(self.client(), self.settings.prefix.clone(), prefix))
    }

    fn list_directory<P>(&self, dir: P) -> ObjectStreamFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        let mut path = match dir.try_into() {
            Ok(p) => p,
            Err(e) => return ObjectStreamFuture::from_value(Err(e.into())),
        };

        if !path.is_empty() && path.is_dir_prefix() {
            path.pop_part();
        }

        unimplemented!();
    }

    fn get_object<P>(&self, path: P) -> ObjectFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        async fn get(
            client: B2Client,
            backend_prefix: ObjectPath,
            path: ObjectPath,
        ) -> StorageResult<Object> {
            let mut file_part = backend_prefix.join(&path);
            let bucket = match file_part.unshift_part() {
                Some(b) => b,
                None => return Err(error::not_found::<StorageError>(path, None)),
            };

            if file_part.is_empty() {
                return Err(error::not_found::<StorageError>(path, None));
            }

            let request = ListBucketsRequest {
                account_id: client.account_id().await?,
                bucket_id: None,
                bucket_name: Some(bucket),
                bucket_types: Default::default(),
            };

            let mut buckets = client.b2_list_buckets(path.clone(), request).await?.buckets;
            if buckets.len() != 1 {
                return Err(error::not_found::<StorageError>(path, None));
            }

            let bucket = buckets.remove(0);

            let file = file_part.to_string();

            let options = ListFileVersionsRequest {
                bucket_id: bucket.bucket_id.clone(),
                start_file_name: None,
                start_file_id: None,
                max_file_count: None,
                prefix: Some(file.clone()),
                delimiter: Some(String::from("/")),
            };

            let requestor = FileVersionsRequestor::new(client.clone(), path.clone(), options);
            let mut internals: Vec<B2ObjectInternals> = ListStream::new(requestor)
                .try_filter(|internals| ready(internals.file_name() == file))
                .try_collect()
                .await?;
            if internals.len() != 1 {
                return Err(error::not_found::<StorageError>(path, None));
            }

            new_object(&bucket.bucket_name, internals.remove(0), &backend_prefix)
        }

        let path = match path.try_into() {
            Ok(p) => p,
            Err(e) => return ObjectFuture::from_value(Err(e.into())),
        };

        if path.is_dir_prefix() {
            return ObjectFuture::from_value(Err(error::invalid_path(
                path,
                "Object paths cannot be empty or end with a '/' character.",
            )));
        }

        ObjectFuture::from_future(get(self.client(), self.settings.prefix.clone(), path))
    }

    fn get_file_stream<O>(&self, reference: O) -> DataStreamFuture
    where
        O: ObjectReference,
    {
        let mut path = match reference.into_path() {
            Ok(p) => p,
            Err(e) => return DataStreamFuture::from_value(Err(e)),
        };

        if path.is_dir_prefix() {
            return DataStreamFuture::from_value(Err(error::invalid_path(
                path,
                "Object paths cannot be empty or end with a '/' character.",
            )));
        }

        let requested = path.clone();
        let bucket = match path.unshift_part() {
            Some(b) => b,
            _ => {
                return DataStreamFuture::from_value(Err(error::invalid_path(
                    path,
                    "Object paths cannot be empty or end with a '/' character.",
                )));
            }
        };

        let file = path.to_string();
        let future = self
            .client()
            .b2_download_file_by_name(requested, bucket, file)
            .map_ok(|body| {
                DataStream::from_stream(body.compat().map(|result| match result {
                    Ok(chunk) => Result::<Data, StorageError>::Ok(chunk.into_bytes()),
                    Err(e) => Result::<Data, StorageError>::Err(e.into()),
                }))
            });

        DataStreamFuture::from_future(future)
    }

    fn delete_object<O>(&self, _reference: O) -> OperationCompleteFuture
    where
        O: ObjectReference,
    {
        unimplemented!();
    }

    fn write_file_from_stream<S, I, E, P>(&self, _path: P, _stream: S) -> WriteCompleteFuture
    where
        S: Stream<Item = Result<I, E>> + Send + 'static,
        I: IntoBuf + 'static,
        E: 'static + std::error::Error + Send + Sync,
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        unimplemented!();
    }
}

fn generate_error(method: &str, path: &ObjectPath, response: &str) -> StorageError {
    let error: ErrorResponse = match from_str(response) {
        Ok(r) => r,
        Err(e) => {
            return error::invalid_data(
                &format!("Unable to parse error response from {}.", method),
                Some(e),
            )
        }
    };

    match (method, error.status, error.code.as_str()) {
        ("b2_authorize_account", 401, "bad_auth_token") => error::access_denied::<StorageError>(
            "The application key id or key were not recognized.",
            None,
        ),
        (_, 400, "bad_request") => error::internal_error::<StorageError>(&error.message, None),
        (_, 400, "invalid_bucket_id") => error::not_found::<StorageError>(path.to_owned(), None),
        (_, 400, "out_of_range") => error::internal_error::<StorageError>(&error.message, None),
        (_, 401, "unauthorized") => error::access_denied::<StorageError>(
            "The application key id or key were not recognized.",
            None,
        ),
        (_, 401, "bad_auth_token") => {
            error::access_expired::<StorageError>("The authentication token is invalid.", None)
        }
        (_, 401, "expired_auth_token") => {
            error::access_expired::<StorageError>("The authentication token has expired.", None)
        }
        (_, 401, "unsupported") => error::internal_error::<StorageError>(&error.message, None),
        (_, 404, "not_found") => error::not_found::<StorageError>(path.clone(), None),
        (_, 503, "bad_request") => error::connection_failed::<StorageError>(&error.message, None),
        _ => error::other_error::<StorageError>(
            &format!(
                "Unknown B2 API failure {}: {}, {}",
                error.status, error.code, error.message
            ),
            None,
        ),
    }
}
