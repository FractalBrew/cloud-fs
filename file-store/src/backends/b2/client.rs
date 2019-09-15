//! The main B2 client API.
//!
//! Mainly split out to ensure that only the expected methods can be called
//! to ensure that limits are enforced correctly.
use std::fmt;
use std::future::Future;
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use base64::encode;
use futures::lock::Mutex;
use futures::stream::{iter, Stream, StreamExt};
use http::header;
use http::method::Method;
use hyper::body::Body;
use hyper::Chunk;
use hyper::{Request, Response};
use log::{error, trace, warn};
use serde::de::DeserializeOwned;
use serde_json::{from_str, to_string};

use storage_types::b2::v2::requests::*;
use storage_types::b2::v2::responses::*;
use storage_types::b2::v2::{
    percent_encode, UserFileInfo, B2_HEADER_CONTENT_SHA1, B2_HEADER_FILE_INFO_PREFIX,
    B2_HEADER_FILE_NAME, B2_HEADER_PART_NUMBER,
};

use super::{B2Settings, Client};
use crate::types::stream::AfterStream;
use crate::types::*;
use crate::utils::Limit;

const MAX_API_RETRIES: usize = 5;

fn generate_error(
    method: &str,
    client_id: usize,
    path: &ObjectPath,
    response: &str,
) -> StorageError {
    let error: ErrorResponse = match from_str(response) {
        Ok(r) => r,
        Err(e) => {
            error!(
                "Client {:04}: Unable to parse ErrorResponse structure from {}.",
                client_id, response
            );
            return error::invalid_data(
                &format!("Unable to parse error response from {}.", method),
                Some(e),
            );
        }
    };
    warn!(
        "Client {:04}: The API call {} failed with {:?}",
        client_id, method, error
    );

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

#[derive(Default, Debug)]
pub(super) struct B2ClientState {
    session: Option<AuthorizeAccountResponse>,
}

#[derive(Debug)]
pub(super) struct B2Client {
    pub id: usize,
    pub client: Client,
    pub settings: B2Settings,
    pub next_id: Arc<AtomicUsize>,
    pub state: Arc<Mutex<B2ClientState>>,
    pub limiter: Limit,
}

impl Clone for B2Client {
    fn clone(&self) -> B2Client {
        B2Client {
            id: self.next_id.fetch_add(1, Ordering::SeqCst),
            client: self.client.clone(),
            settings: self.settings.clone(),
            next_id: self.next_id.clone(),
            state: self.state.clone(),
            limiter: self.limiter.clone(),
        }
    }
}

impl B2Client {
    fn api_url(&self, host: &str, method: &str) -> String {
        format!("{}/b2api/{}/{}", host, B2_VERSION, method)
    }

    async fn request(
        &self,
        method: &str,
        path: ObjectPath,
        request: Request<Body>,
    ) -> StorageResult<Response<Body>> {
        trace!("Client {:04}: Requesting {}", self.id, request.uri());
        let response = match self.client.request(request).await {
            Ok(r) => {
                trace!("Client {:04}: {} b2 api call succeeded", self.id, method);
                r
            }
            Err(e) => {
                error!(
                    "Client {:04}: {} b2 api call failed: {}",
                    self.id, method, e
                );
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
            Err(generate_error(method, self.id, &path, &data))
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
                trace!(
                    "Client {:04}: {} api method returned {:?}",
                    self.id,
                    method,
                    r
                );
                Ok(r)
            }
            Err(e) => {
                error!("Client {:04}: {} api method failed: {}", self.id, method, e);
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

        trace!(
            "Client {:04}: Starting b2_authorize_account api call with {}",
            self.id,
            secret
        );

        let request = Request::builder()
            .method(Method::GET)
            .uri(self.api_url(&self.settings.host, "b2_authorize_account"))
            .header(header::AUTHORIZATION, secret)
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
            let _limit = self.limiter.take().await;

            let (api_url, authorization) = {
                let session = self.session().await?;
                (session.api_url.clone(), session.authorization_token.clone())
            };

            trace!(
                "Client {:04}: Starting {} api call (attempt {}) with {:?}",
                self.id,
                method,
                tries + 1,
                request
            );
            let data = to_string(&request)?;

            let request = Request::builder()
                .method(Method::POST)
                .uri(self.api_url(&api_url, method))
                .header(header::AUTHORIZATION, &authorization)
                .body(data.into())?;

            match self.basic_request(method, path.clone(), request).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    if e.kind() == error::StorageErrorKind::AccessExpired {
                        self.reset_session(&authorization).await;

                        tries += 1;
                        if tries < MAX_API_RETRIES {
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    pub async fn b2_download_file_by_name(
        self,
        path: ObjectPath,
        bucket: String,
        file: String,
    ) -> StorageResult<impl Stream<Item = Result<Chunk, hyper::Error>>> {
        let mut tries: usize = 0;
        loop {
            let mut limit = self.limiter.take().await;

            let (download_url, authorization) = {
                let session = self.session().await?;
                (
                    session.download_url.clone(),
                    session.authorization_token.clone(),
                )
            };

            trace!(
                "Client {:04}: Starting {} api call (attempt {})",
                self.id,
                "b2_download_file_by_name",
                tries + 1,
            );

            let request = Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "{}/file/{}/{}",
                    download_url,
                    percent_encode(&bucket),
                    percent_encode(&file)
                ))
                .header(header::AUTHORIZATION, &authorization)
                .body(Body::empty())?;

            match self
                .request("b2_download_file_by_name", path.clone(), request)
                .await
            {
                Ok(response) => {
                    let (_, body) = response.into_parts();
                    let stream = AfterStream::after(body, move || limit.release());

                    return Ok(stream);
                }
                Err(e) => {
                    if e.kind() == error::StorageErrorKind::AccessExpired {
                        self.reset_session(&authorization).await;

                        tries += 1;
                        if tries < MAX_API_RETRIES {
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn b2_upload_file(
        self,
        path: ObjectPath,
        url: String,
        auth: String,
        file_name: String,
        content_type: String,
        info: UserFileInfo,
        length: u64,
        hash: String,
        data: Vec<Data>,
    ) -> StorageResult<UploadFileResponse> {
        let mut tries: usize = 0;

        loop {
            let mut builder = Request::builder();
            builder
                .method(Method::POST)
                .uri(&url)
                .header(header::AUTHORIZATION, &auth)
                .header(B2_HEADER_FILE_NAME, percent_encode(&file_name))
                .header(header::CONTENT_TYPE, &content_type)
                .header(header::CONTENT_LENGTH, length)
                .header(B2_HEADER_CONTENT_SHA1, &hash);

            for (key, value) in info.iter() {
                builder.header(&format!("{}{}", B2_HEADER_FILE_INFO_PREFIX, key), value);
            }

            let request = builder.body(Body::wrap_stream(
                iter(data.clone()).map(Ok::<_, StorageError>),
            ))?;

            let _limit = self.limiter.take().await;
            match self
                .basic_request("b2_upload_file", path.clone(), request)
                .await
            {
                Ok(response) => return Ok(response),
                Err(e) => {
                    tries += 1;
                    if tries < MAX_API_RETRIES {
                        continue;
                    }

                    return Err(e);
                }
            }
        }
    }

    pub async fn b2_upload_part(
        self,
        path: ObjectPath,
        upload_url: GetUploadPartUrlResponse,
        part: usize,
        length: u64,
        hash: String,
        data: Vec<Data>,
    ) -> StorageResult<UploadPartResponse> {
        let mut tries: usize = 0;

        loop {
            let request = Request::builder()
                .method(Method::POST)
                .uri(&upload_url.upload_url)
                .header(header::AUTHORIZATION, &upload_url.authorization_token)
                .header(B2_HEADER_PART_NUMBER, part)
                .header(header::CONTENT_LENGTH, length)
                .header(B2_HEADER_CONTENT_SHA1, &hash)
                .body(Body::wrap_stream(
                    iter(data.clone()).map(Ok::<_, StorageError>),
                ))?;

            let _limit = self.limiter.take().await;
            match self
                .basic_request("b2_upload_part", path.clone(), request)
                .await
            {
                Ok(response) => return Ok(response),
                Err(e) => {
                    tries += 1;
                    if tries < MAX_API_RETRIES {
                        continue;
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
        let mut state = self.state.lock().await;
        if let Some(ref s) = state.session {
            if s.authorization_token == auth_token {
                state.session.take();
            }
        }
    }

    async fn session(&self) -> StorageResult<AuthorizeAccountResponse> {
        let mut state = self.state.lock().await;
        if let Some(ref s) = state.session {
            Ok(s.clone())
        } else {
            let new_session = self.b2_authorize_account().await?;
            state.session.replace(new_session.clone());
            Ok(new_session)
        }
    }

    pub async fn account_info(&self) -> StorageResult<AuthorizeAccountResponse> {
        let _limit = self.limiter.take().await;
        self.session().await
    }
}
