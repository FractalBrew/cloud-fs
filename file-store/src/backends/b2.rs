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
use std::cmp::max;
use std::convert::TryInto;
use std::fmt;
use std::future::Future;
use std::io::Read;
use std::ops::Deref;
use std::pin::Pin;
use std::slice::Iter;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::encode;
use bytes::buf::FromBuf;
use bytes::IntoBuf;
use futures::future::{ready, TryFutureExt};
use futures::lock::Mutex;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use http::method::Method;
use hyper::body::Body;
use hyper::client::connect::HttpConnector;
use hyper::client::Client as HyperClient;
use hyper::{Request, Response};
use hyper_tls::HttpsConnector;
use log::trace;
use serde::de::DeserializeOwned;
use serde_json::{from_str, to_string};
use sha1::Sha1;

use storage_types::b2::v2::requests::*;
use storage_types::b2::v2::responses::*;
use storage_types::b2::v2::{
    percent_encode, FileAction, UserFileInfo, FILE_INFO_PREFIX, LAST_MODIFIED_KEY,
};

use super::Backend;
use crate::types::future::{FuturePoll, ResultFuturePoll};
use crate::types::stream::{MergedStreams, ResultStreamPoll, VecStream};
use crate::types::*;
use crate::utils::into_data_stream;
use crate::{FileStore, StorageBackend};

type Client = HyperClient<HttpsConnector<HttpConnector>>;
type StringFuture = WrappedFuture<StorageResult<String>>;

const API_RETRIES: usize = 3;
const TOTAL_MAX_SMALL_FILE_SIZE: u64 = 5 * 1000 * 1000 * 1000;
const DEFAULT_MAX_SMALL_FILE_SIZE: u64 = 200 * 1000 * 1000;

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

#[derive(Clone, Debug)]
struct FileVersions {
    versions: Vec<FileInfo>,
}

impl FileVersions {
    fn new(mut versions: Vec<FileInfo>) -> FileVersions {
        versions.sort_by(|a, b| a.upload_timestamp.cmp(&b.upload_timestamp));

        FileVersions { versions }
    }

    fn latest(&self) -> &FileInfo {
        &self.versions[self.versions.len() - 1]
    }

    fn iter(&self) -> Iter<FileInfo> {
        self.versions.iter()
    }
}

/// The B2 implementation for [`Object`](../../enum.Object.html).
#[derive(Clone, Debug)]
pub struct B2Object {
    path: ObjectPath,
    versions: FileVersions,
}

impl B2Object {
    fn versions(&self) -> Iter<FileInfo> {
        self.versions.iter()
    }
}

impl ObjectInfo for B2Object {
    fn path(&self) -> ObjectPath {
        self.path.clone()
    }

    fn len(&self) -> u64 {
        self.versions.latest().content_length
    }

    fn object_type(&self) -> ObjectType {
        match &self.versions.latest().action {
            FileAction::Upload => ObjectType::File,
            FileAction::Folder => ObjectType::Directory,
            _ => ObjectType::Unknown,
        }
    }

    fn modified(&self) -> Option<SystemTime> {
        let version = self.versions.latest();
        if version.action != FileAction::Upload {
            return None;
        }

        version
            .file_info
            .get(LAST_MODIFIED_KEY)
            .and_then(|s| {
                let time = match s.parse::<u64>() {
                    Ok(t) => t,
                    Err(_) => return None,
                };

                Some(UNIX_EPOCH + Duration::from_millis(time))
            })
            .or_else(|| {
                if version.upload_timestamp > 0 {
                    Some(UNIX_EPOCH + Duration::from_millis(version.upload_timestamp))
                } else {
                    None
                }
            })
    }
}

fn new_object(bucket: &str, versions: FileVersions, prefix: &ObjectPath) -> StorageResult<Object> {
    let mut path = ObjectPath::new(&versions.latest().file_name)?;
    path.shift_part(bucket);
    if path.is_dir_prefix() {
        path.pop_part();
    }

    for _ in prefix.parts() {
        path.unshift_part();
    }

    Ok(Object::from(B2Object { path, versions }))
}

#[derive(Clone, Debug)]
struct B2Settings {
    key_id: String,
    key: String,
    host: String,
    prefix: ObjectPath,
    max_small_file_size: u64,
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
        trace!("Requesting {}", request.uri());
        let response = match self.client.request(request).await {
            Ok(r) => {
                trace!("{} b2 api call succeeded", method);
                r
            }
            Err(e) => {
                trace!("{} b2 api call failed: {}", method, e);
                return Err(e.into());
            }
        };

        if response.status().is_success() {
            Ok(response)
        } else {
            let (_, body) = response.into_parts();

            let mut data: String = String::new();
            BlockingStreamReader::from_stream(body)
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
        R: DeserializeOwned + fmt::Debug,
    {
        let response = self.request(method, path, request).await?;
        let (_, body) = response.into_parts();

        let mut data: String = String::new();
        BlockingStreamReader::from_stream(body)
            .read_to_string(&mut data)
            .unwrap();

        match from_str(&data) {
            Ok(r) => {
                trace!("{} api method returned {:?}", method, r);
                Ok(r)
            }
            Err(e) => {
                trace!("{} api method failed: {}", method, e);
                Err(error::invalid_data(
                    &format!("Unable to parse response from {}.", method),
                    Some(e),
                ))
            }
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
        S: serde::ser::Serialize + Clone + fmt::Debug,
        for<'de> Q: serde::de::Deserialize<'de> + fmt::Debug,
    {
        let mut tries: usize = 0;
        loop {
            let (api_url, authorization) = {
                let session = self.session().await?;
                (session.api_url.clone(), session.authorization_token.clone())
            };

            trace!("Starting {} api call with {:?}", method, request);
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
                .uri(format!(
                    "{}/file/{}/{}",
                    download_url,
                    percent_encode(&bucket),
                    percent_encode(&file)
                ))
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

    #[allow(clippy::too_many_arguments)]
    async fn b2_upload_file(
        self,
        path: ObjectPath,
        url: String,
        auth: String,
        file_name: String,
        content_type: String,
        info: UserFileInfo,
        length: u64,
        hash: String,
        stream: VecStream<StorageResult<Data>>,
    ) -> StorageResult<UploadFileResponse> {
        let mut builder = Request::builder();
        builder
            .method(Method::POST)
            .uri(url)
            .header("Authorization", auth)
            .header("X-Bz-File-Name", percent_encode(&file_name))
            .header("Content-Type", content_type)
            .header("Content-Length", length)
            .header("X-Bz-Content-Sha1", hash);

        for (key, value) in info {
            builder.header(&format!("{}{}", FILE_INFO_PREFIX, key), value);
        }

        let request = builder.body(Body::wrap_stream(stream))?;

        let result = self.basic_request("b2_upload_file", path, request).await?;
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    async fn b2_upload_part(
        self,
        path: ObjectPath,
        url: String,
        auth: String,
        part: usize,
        length: u64,
        hash: String,
        stream: VecStream<StorageResult<Data>>,
    ) -> StorageResult<UploadPartResponse> {
        let request = Request::builder()
            .method(Method::POST)
            .uri(url)
            .header("Authorization", auth)
            .header("X-Bz-Part-Number", part)
            .header("Content-Length", length)
            .header("X-Bz-Content-Sha1", hash)
            .body(Body::wrap_stream(stream))?;

        let result = self.basic_request("b2_upload_part", path, request).await?;
        Ok(result)
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
    b2_api!(
        b2_delete_file_version,
        DeleteFileVersionRequest,
        DeleteFileVersionResponse
    );
    b2_api!(b2_get_upload_url, GetUploadUrlRequest, GetUploadUrlResponse);
    b2_api!(
        b2_start_large_file,
        StartLargeFileRequest,
        StartLargeFileResponse
    );
    b2_api!(
        b2_get_upload_part_url,
        GetUploadPartUrlRequest,
        GetUploadPartUrlResponse
    );
    b2_api!(
        b2_finish_large_file,
        FinishLargeFileRequest,
        FinishLargeFileResponse
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

enum LargeFileState {
    Starting(Pin<Box<WrappedFuture<StorageResult<GetUploadPartUrlResponse>>>>),
    Uploading(GetUploadPartUrlResponse),
    Finishing(Pin<Box<OperationCompleteFuture>>),
    Complete,
}

enum PartState {
    Waiting(Vec<Data>),
    Pending(Pin<Box<StringFuture>>),
    Complete(String),
}

struct LargeFileUploader {
    client: B2Client,
    path: ObjectPath,
    done: bool,
    parts: Vec<PartState>,
    state: LargeFileState,
}

impl LargeFileUploader {
    fn new(client: B2Client, info: UploadInfo, bucket_id: &str, file: &str) -> LargeFileUploader {
        LargeFileUploader {
            client: client.clone(),
            path: info.path.clone(),
            done: false,
            parts: Default::default(),
            state: LargeFileState::Starting(Box::pin(WrappedFuture::<
                StorageResult<GetUploadPartUrlResponse>,
            >::from_future(
                LargeFileUploader::start_upload(
                    client,
                    info,
                    bucket_id.to_owned(),
                    file.to_owned(),
                ),
            ))),
        }
    }

    async fn start_upload(
        client: B2Client,
        info: UploadInfo,
        bucket_id: String,
        file_name: String,
    ) -> StorageResult<GetUploadPartUrlResponse> {
        let mut file_info = UserFileInfo::new();
        if let Some(time) = info.modified {
            if let Ok(duration) = time.duration_since(UNIX_EPOCH) {
                file_info.insert(
                    LAST_MODIFIED_KEY.to_owned(),
                    duration.as_millis().to_string(),
                );
            }
        }

        let request = StartLargeFileRequest {
            bucket_id,
            file_name,
            content_type: String::from("b2/x-auto"),
            file_info: Some(file_info),
        };

        let result = client
            .b2_start_large_file(info.path.clone(), request)
            .await?;

        let file_id = match result.file_id {
            Some(s) => s,
            None => {
                return Err(error::invalid_data::<StorageError>(
                    "Attempt to request large file upload failed.",
                    None,
                ))
            }
        };

        let request = GetUploadPartUrlRequest {
            file_id: file_id.clone(),
        };

        client.b2_get_upload_part_url(info.path, request).await
    }

    async fn upload_part(
        client: B2Client,
        part: usize,
        path: ObjectPath,
        mut buffers: Vec<Data>,
        upload_info: GetUploadPartUrlResponse,
    ) -> StorageResult<String> {
        assert!(part > 0);

        let mut hasher = Sha1::new();
        let mut len: u64 = 0;

        for b in buffers.iter() {
            hasher.update(&b);
            len += b.len() as u64;
        }

        let hash = hasher.hexdigest();

        client
            .b2_upload_part(
                path,
                upload_info.upload_url,
                upload_info.authorization_token,
                part,
                len,
                hash.clone(),
                VecStream::from(buffers.drain(..).map(Ok).collect()),
            )
            .await?;

        Ok(hash)
    }

    async fn finish_upload(
        client: B2Client,
        file_id: String,
        path: ObjectPath,
        parts: Vec<String>,
    ) -> StorageResult<()> {
        client
            .b2_finish_large_file(
                path,
                FinishLargeFileRequest {
                    file_id,
                    part_sha1_array: parts,
                },
            )
            .await?;

        Ok(())
    }

    fn all_parts_added(&mut self) {
        self.done = true;
    }

    fn add_part(&mut self, buffers: Vec<Data>) {
        assert!(!self.done);

        match self.state {
            LargeFileState::Starting(_) => self.parts.push(PartState::Waiting(buffers)),
            LargeFileState::Uploading(ref id) => {
                let part = self.parts.len() + 1;
                self.parts
                    .push(PartState::Pending(Box::pin(StringFuture::from_future(
                        LargeFileUploader::upload_part(
                            self.client.clone(),
                            part,
                            self.path.clone(),
                            buffers,
                            id.clone(),
                        ),
                    ))))
            }
            _ => unreachable!(),
        }
    }

    fn inner_poll(&mut self, cx: &mut Context) -> Option<ResultFuturePoll<()>> {
        match self.state {
            LargeFileState::Starting(ref mut fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(id)) => {
                    self.state = LargeFileState::Uploading(id.clone());
                    let client = self.client.clone();
                    let path = self.path.clone();
                    for i in 0..self.parts.len() {
                        match self.parts[i] {
                            PartState::Waiting(ref mut buffers) => {
                                self.parts[i] = PartState::Pending(Box::pin(
                                    StringFuture::from_future(LargeFileUploader::upload_part(
                                        client.clone(),
                                        i + 1,
                                        path.clone(),
                                        buffers.to_vec(),
                                        id.clone(),
                                    )),
                                ))
                            }
                            _ => unreachable!(),
                        }
                    }
                    return None;
                }
                Poll::Ready(Err(e)) => return Some(Poll::Ready(Err(e))),
                Poll::Pending => return Some(Poll::Pending),
            },
            LargeFileState::Uploading(ref upload_state) => {
                let mut all_done = true;
                for i in 0..self.parts.len() {
                    if let PartState::Pending(ref mut fut) = self.parts[i] {
                        match fut.as_mut().poll(cx) {
                            Poll::Ready(Ok(hash)) => {
                                self.parts[i] = PartState::Complete(hash);
                            }
                            Poll::Ready(Err(e)) => return Some(Poll::Ready(Err(e))),
                            Poll::Pending => all_done = false,
                        }
                    }
                }

                if !all_done {
                    return Some(Poll::Pending);
                }

                if self.done {
                    let hashes = self
                        .parts
                        .drain(..)
                        .map(|p| match p {
                            PartState::Complete(s) => s,
                            _ => unreachable!(),
                        })
                        .collect();
                    self.state = LargeFileState::Finishing(Box::pin(
                        OperationCompleteFuture::from_future(LargeFileUploader::finish_upload(
                            self.client.clone(),
                            upload_state.file_id.clone(),
                            self.path.clone(),
                            hashes,
                        )),
                    ));
                }
            }
            LargeFileState::Finishing(ref mut fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    self.state = LargeFileState::Complete;
                    return Some(Poll::Ready(Ok(())));
                }
                Poll::Ready(Err(e)) => {
                    self.state = LargeFileState::Complete;
                    return Some(Poll::Ready(Err(e)));
                }
                Poll::Pending => return Some(Poll::Pending),
            },
            LargeFileState::Complete => return Some(Poll::Ready(Ok(()))),
        }

        None
    }
}

impl Future for LargeFileUploader {
    type Output = StorageResult<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> ResultFuturePoll<()> {
        loop {
            if let Some(r) = self.inner_poll(cx) {
                return r;
            }
        }
    }
}

enum UploadState {
    Buffering(Sha1),
    SmallFileUpload(Pin<Box<OperationCompleteFuture>>),
    LargeFileUpload(Pin<Box<LargeFileUploader>>),
}

struct Uploader<S>
where
    S: Stream<Item = StorageResult<Data>> + Send + 'static,
{
    recommended_part_size: u64,
    maximum_small_file_size: u64,
    stream: Pin<Box<S>>,
    bucket_id: String,
    info: UploadInfo,
    file: String,
    client: B2Client,
    buffers: Vec<Data>,
    len: u64,
    state: UploadState,
    done: bool,
}

impl<S> Uploader<S>
where
    S: Stream<Item = StorageResult<Data>> + Send + 'static,
{
    async fn small_upload(
        client: B2Client,
        info: UploadInfo,
        bucket_id: String,
        file_name: String,
        mut data: Vec<Data>,
        size: u64,
        hash: String,
    ) -> StorageResult<()> {
        let response = client
            .clone()
            .b2_get_upload_url(info.path.clone(), GetUploadUrlRequest { bucket_id })
            .await?;

        let mut user_info = UserFileInfo::new();
        if let Some(time) = info.modified.as_ref() {
            if let Ok(duration) = time.duration_since(UNIX_EPOCH) {
                user_info.insert(
                    LAST_MODIFIED_KEY.to_owned(),
                    duration.as_millis().to_string(),
                );
            }
        }

        client
            .b2_upload_file(
                info.path,
                response.upload_url,
                response.authorization_token,
                file_name,
                String::from("b2/x-auto"),
                user_info,
                size,
                hash,
                VecStream::from(data.drain(..).map(Ok).collect()),
            )
            .await?;

        Ok(())
    }

    async fn upload(
        client: B2Client,
        max_small_file_size: u64,
        info: UploadInfo,
        bucket_id: String,
        file: String,
        stream: S,
    ) -> Result<(), TransferError> {
        let uploader = {
            let session = client.session().await.map_err(TransferError::TargetError)?;

            Uploader {
                recommended_part_size: session.recommended_part_size,
                maximum_small_file_size: max(
                    max_small_file_size,
                    session.absolute_minimum_part_size,
                ),
                state: UploadState::Buffering(Sha1::new()),
                done: false,
                client,
                info,
                bucket_id,
                file,
                stream: Box::pin(stream),
                buffers: Default::default(),
                len: 0,
            }
        };

        uploader.await
    }

    fn inner_poll(&mut self, cx: &mut Context) -> Option<FuturePoll<Result<(), TransferError>>> {
        // First drive any existing uploads.
        match self.state {
            UploadState::SmallFileUpload(ref mut sf) => {
                return Some(match sf.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                    Poll::Ready(Err(e)) => Poll::Ready(Err(TransferError::TargetError(e))),
                    Poll::Pending => Poll::Pending,
                });
            }
            UploadState::LargeFileUpload(ref mut lf) => match lf.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => return Some(Poll::Ready(Ok(()))),
                Poll::Ready(Err(e)) => {
                    return Some(Poll::Ready(Err(TransferError::TargetError(e))))
                }
                _ => (),
            },
            _ => {
                assert!(!self.done);
            }
        }

        if self.done {
            return Some(Poll::Pending);
        }

        // Try to pull some new data.
        let data = match self.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(d))) => Data::from_buf(d),
            Poll::Ready(Some(Err(e))) => {
                return Some(Poll::Ready(Err(TransferError::SourceError(e))));
            }
            Poll::Ready(None) => {
                // All the data has been read. Switch to pending state.
                self.done = true;

                match self.state {
                    UploadState::Buffering(ref mut hasher) => {
                        // Start a small file upload.
                        self.state = UploadState::SmallFileUpload(Box::pin(
                            OperationCompleteFuture::from_future(Uploader::<S>::small_upload(
                                self.client.clone(),
                                self.info.clone(),
                                self.bucket_id.clone(),
                                self.file.clone(),
                                self.buffers.drain(..).collect(),
                                self.len,
                                hasher.hexdigest(),
                            )),
                        ));
                    }
                    UploadState::LargeFileUpload(ref mut lf) => {
                        lf.add_part(self.buffers.drain(..).collect());
                        lf.all_parts_added();
                    }
                    _ => unreachable!(),
                }
                return None;
            }
            Poll::Pending => return Some(Poll::Pending),
        };

        self.len += data.len() as u64;
        self.buffers.push(data);

        match self.state {
            UploadState::Buffering(ref mut hasher) => {
                hasher.update(&self.buffers[self.buffers.len() - 1]);

                if self.len > self.maximum_small_file_size {
                    // Start a large file upload.
                    let target = self.maximum_small_file_size;
                    let mut lf = LargeFileUploader::new(
                        self.client.clone(),
                        self.info.clone(),
                        &self.bucket_id,
                        &self.file,
                    );
                    let (buffers, len) = self.buffers.drain(..).fold(
                        (Vec::<Data>::new(), 0 as u64),
                        |(mut buffers, mut len), data| {
                            len += data.len() as u64;
                            buffers.push(data);

                            if len >= target {
                                lf.add_part(buffers);
                                Default::default()
                            } else {
                                (buffers, len)
                            }
                        },
                    );

                    self.buffers = buffers;
                    self.len = len;
                    self.state = UploadState::LargeFileUpload(Box::pin(lf));
                }
            }
            UploadState::LargeFileUpload(ref mut lf) => {
                if self.len >= self.recommended_part_size {
                    lf.add_part(self.buffers.drain(..).collect());
                    self.len = 0;
                }
            }
            _ => unreachable!(),
        }

        None
    }
}

impl<S> Future for Uploader<S>
where
    S: Stream<Item = StorageResult<Data>> + Send + 'static,
{
    type Output = Result<(), TransferError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> FuturePoll<Result<(), TransferError>> {
        loop {
            if let Some(r) = self.inner_poll(cx) {
                return r;
            }
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
    type Item = StorageResult<FileVersions>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> ResultStreamPoll<FileVersions> {
        match self.poll_next_info(cx) {
            Poll::Ready(Some(Ok(info))) => {
                if self.current.is_empty() || self.current[0].file_name == info.file_name {
                    self.current.push(info);
                    self.poll_next(cx)
                } else {
                    let versions = FileVersions::new(self.current.drain(..).collect());
                    self.current.push(info);
                    Poll::Ready(Some(Ok(versions)))
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                if !self.current.is_empty() {
                    let versions = FileVersions::new(self.current.drain(..).collect());
                    Poll::Ready(Some(Ok(versions)))
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
    /// Creates a new [`FileStore`](../../enum.FileStore.html) instance using the
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
                max_small_file_size: DEFAULT_MAX_SMALL_FILE_SIZE,
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

    async fn expand_path(
        client: B2Client,
        prefix: ObjectPath,
        path: ObjectPath,
    ) -> StorageResult<(Bucket, String)> {
        let mut file_part = prefix.join(&path);
        let bucket_name = match file_part.unshift_part() {
            Some(b) => b,
            None => return Err(error::not_found::<StorageError>(path, None)),
        };

        if file_part.is_empty() {
            return Err(error::not_found::<StorageError>(path, None));
        }

        let request = ListBucketsRequest {
            account_id: client.account_id().await?,
            bucket_id: None,
            bucket_name: Some(bucket_name),
            bucket_types: Default::default(),
        };

        let mut buckets = client.b2_list_buckets(path.clone(), request).await?.buckets;
        if buckets.len() != 1 {
            return Err(error::not_found::<StorageError>(path, None));
        }

        Ok((buckets.remove(0), file_part.to_string()))
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

    /// Sets the cutoff between normal file uploads and large file uploads.
    ///
    /// B2 allows for two different upload mechanisms, normal files and large
    /// files. Normal files are uploaded in a single http request while
    /// large files are uploading in multiple parts which can be uploaded in
    /// parallel.
    ///
    /// This sets the desired cut-off between the different upload methods.
    /// Trying to set this larger than the maximum size of normal files will
    /// just use the maximum size of normal files. Trying to set this smaller
    /// than the minimum size of large file parts will just use the minimum
    /// size of large file parts.
    pub fn limit_small_file_size(mut self, size: u64) -> B2BackendBuilder {
        if size > TOTAL_MAX_SMALL_FILE_SIZE {
            self.settings.max_small_file_size = TOTAL_MAX_SMALL_FILE_SIZE;
        } else {
            self.settings.max_small_file_size = size;
        }
        self
    }

    /// Creates a new B2 based [`FileStore`](../../enum.FileStore.html) using
    /// this builder's settings.
    pub fn connect(self) -> ConnectFuture {
        ConnectFuture::from_future(async {
            trace!("Connecting to B2 with settings {:?}", self.settings);
            let connector = match HttpsConnector::new() {
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

            Ok(FileStore::from(backend))
        })
    }
}

async fn object_list(
    client: B2Client,
    backend_prefix: ObjectPath,
    prefix: ObjectPath,
    delimiter: Option<String>,
) -> StorageResult<ObjectStream> {
    let mut file_part = backend_prefix.join(&prefix);
    let bucket = file_part.unshift_part();

    let mut request = ListBucketsRequest {
        account_id: client.account_id().await?,
        bucket_id: None,
        bucket_name: None,
        bucket_types: Default::default(),
    };

    if let Some(ref bucket_name) = bucket {
        // Only include the bucket named `bucket`.
        request.bucket_name = Some(bucket_name.clone());
    }

    let bucket_name = bucket.unwrap_or_else(String::new);
    let path = ObjectPath::new(bucket_name.clone())?;
    let listers = client
        .b2_list_buckets(path, request)
        .await?
        .buckets
        .drain(..)
        .filter(|b| b.bucket_name.starts_with(&bucket_name))
        .map(move |b| {
            let options = ListFileVersionsRequest {
                bucket_id: b.bucket_id.clone(),
                start_file_name: None,
                start_file_id: None,
                max_file_count: None,
                prefix: Some(file_part.to_string()),
                delimiter: delimiter.clone(),
            };

            let requestor = FileVersionsRequestor::new(client.clone(), prefix.clone(), options);
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

impl StorageBackend for B2Backend {
    fn backend_type(&self) -> Backend {
        Backend::B2
    }

    fn list_objects<P>(&self, prefix: P) -> ObjectStreamFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        let prefix = match prefix.try_into() {
            Ok(p) => p,
            Err(e) => return ObjectStreamFuture::from_value(Err(e.into())),
        };

        ObjectStreamFuture::from_future(object_list(
            self.client(),
            self.settings.prefix.clone(),
            prefix,
            None,
        ))
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

        if !path.is_empty() && !path.is_dir_prefix() {
            path.push_part("");
        }

        ObjectStreamFuture::from_future(object_list(
            self.client(),
            self.settings.prefix.clone(),
            path,
            Some(String::from("/")),
        ))
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
            let (bucket, file) =
                B2Backend::expand_path(client.clone(), backend_prefix.clone(), path.clone())
                    .await?;

            let options = ListFileVersionsRequest {
                bucket_id: bucket.bucket_id.clone(),
                start_file_name: None,
                start_file_id: None,
                max_file_count: None,
                prefix: Some(file.clone()),
                delimiter: Some(String::from("/")),
            };

            let requestor = FileVersionsRequestor::new(client.clone(), path.clone(), options);
            let mut files: Vec<FileVersions> = ListStream::new(requestor)
                .try_filter(|versions| ready(versions.latest().file_name == file))
                .try_collect()
                .await?;
            if files.len() != 1 {
                return Err(error::not_found::<StorageError>(path, None));
            }

            new_object(&bucket.bucket_name, files.remove(0), &backend_prefix)
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

        let client = self.client();
        let prefix = self.settings.prefix.clone();
        ObjectFuture::from_future(get(client, prefix, path))
    }

    fn get_file_stream<P>(&self, path: P) -> DataStreamFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        let path = match path.try_into() {
            Ok(p) => p,
            Err(e) => return DataStreamFuture::from_value(Err(e.into())),
        };

        if path.is_dir_prefix() {
            return DataStreamFuture::from_value(Err(error::invalid_path(
                path,
                "Object paths cannot be empty or end with a '/' character.",
            )));
        }

        let mut file_name = self.settings.prefix.join(&path);
        let bucket = match file_name.unshift_part() {
            Some(b) => b,
            _ => {
                return DataStreamFuture::from_value(Err(error::invalid_path(
                    path,
                    "Object paths cannot be empty.",
                )));
            }
        };

        let future = self
            .client()
            .b2_download_file_by_name(path, bucket, file_name.to_string())
            .map_ok(|body| {
                DataStream::from_stream(body.map(|result| match result {
                    Ok(chunk) => Result::<Data, StorageError>::Ok(chunk.into_bytes()),
                    Err(e) => Result::<Data, StorageError>::Err(e.into()),
                }))
            });

        DataStreamFuture::from_future(future)
    }

    fn delete_object<P>(&self, path: P) -> OperationCompleteFuture
    where
        P: TryInto<ObjectPath>,
        P::Error: Into<StorageError>,
    {
        async fn delete(backend: B2Backend, path: ObjectPath) -> StorageResult<()> {
            let object: B2Object = match backend.clone().get_object(path.clone()).await?.try_into()
            {
                Ok(o) => o,
                Err(_) => {
                    return Err(error::internal_error::<StorageError>(
                        "Failed to convert retrieved object to the expected type.",
                        None,
                    ));
                }
            };

            for info in object.versions() {
                match info.file_id {
                    Some(ref id) => {
                        backend
                            .client()
                            .b2_delete_file_version(
                                path.clone(),
                                DeleteFileVersionRequest {
                                    file_name: info.file_name.clone(),
                                    file_id: id.to_owned(),
                                },
                            )
                            .await?;
                    }
                    None => {
                        return Err(error::internal_error::<StorageError>(
                            "Expected object to have a file id.",
                            None,
                        ));
                    }
                }
            }

            Ok(())
        }

        let path = match path.try_into() {
            Ok(p) => p,
            Err(e) => return OperationCompleteFuture::from_value(Err(e.into())),
        };
        OperationCompleteFuture::from_future(delete(self.clone(), path))
    }

    fn write_file_from_stream<S, I, E, P>(&self, info: P, stream: S) -> WriteCompleteFuture
    where
        S: Stream<Item = Result<I, E>> + Send + 'static,
        I: IntoBuf + 'static,
        E: Into<StorageError> + 'static,
        P: TryInto<UploadInfo>,
        P::Error: Into<StorageError>,
    {
        async fn upload<S>(
            client: B2Client,
            max_small_file_size: u64,
            prefix: ObjectPath,
            info: UploadInfo,
            stream: S,
        ) -> Result<(), TransferError>
        where
            S: Stream<Item = StorageResult<Data>> + Send + 'static,
        {
            let (bucket, file) =
                B2Backend::expand_path(client.clone(), prefix.clone(), info.path.clone())
                    .await
                    .map_err(TransferError::SourceError)?;

            Uploader::upload(
                client,
                max_small_file_size,
                info,
                bucket.bucket_id,
                file,
                stream,
            )
            .await
        }

        let info = match info.try_into() {
            Ok(i) => i,
            Err(e) => {
                return WriteCompleteFuture::from_value(Err(TransferError::TargetError(e.into())))
            }
        };

        let path = info.path.clone();
        if path.is_dir_prefix() {
            return WriteCompleteFuture::from_value(Err(TransferError::TargetError(
                error::invalid_path(
                    path,
                    "Object paths cannot be empty or end with a '/' character.",
                ),
            )));
        }

        WriteCompleteFuture::from_future(upload(
            self.client(),
            self.settings.max_small_file_size,
            self.settings.prefix.clone(),
            info,
            into_data_stream(stream),
        ))
    }
}

fn generate_error(method: &str, path: &ObjectPath, response: &str) -> StorageError {
    let error: ErrorResponse = match from_str(response) {
        Ok(r) => r,
        Err(e) => {
            trace!("Unable to parse ErrorResponse structure from {}.", response);
            return error::invalid_data(
                &format!("Unable to parse error response from {}.", method),
                Some(e),
            );
        }
    };
    trace!("Found {:?}", error);

    match (method, error.status, error.code.as_str()) {
        ("b2_authorize_account", 401, "bad_auth_token") => error::access_denied::<StorageError>(
            "The application key id or key were not recognized.",
            None,
        ),
        (_, 400, "bad_request") => error::internal_error::<StorageError>(&error.message, None),
        (_, 400, "invalid_bucket_id") => error::not_found::<StorageError>(path.to_owned(), None),
        (_, 400, "bad_bucket_id") => error::not_found::<StorageError>(path.to_owned(), None),
        (_, 400, "file_not_present") => error::not_found::<StorageError>(path.to_owned(), None),
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
