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

//! The main B2 client API.
//!
//! Mainly split out to ensure that only the expected methods can be called
//! to ensure that limits are enforced correctly.
use std::fmt;
use std::future::Future;
use std::io::Read;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use base64::encode;
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

use super::{B2Settings, Client, ClientPool};
use crate::types::stream::AfterStream;
use crate::types::*;
use crate::utils::Pool;

const MAX_API_RETRIES: usize = 5;

#[derive(Debug)]
struct B2Error {
    error: StorageError,
    needs_auth: bool,
    can_retry: bool,
}

impl From<B2Error> for StorageError {
    fn from(error: B2Error) -> StorageError {
        error.error
    }
}

type B2Result<T> = Result<T, B2Error>;

impl From<hyper::error::Error> for B2Error {
    fn from(hyper_error: hyper::error::Error) -> B2Error {
        fn error(error: StorageError, can_retry: bool) -> B2Error {
            B2Error {
                error,
                needs_auth: can_retry,
                can_retry,
            }
        }

        if hyper_error.is_parse() || hyper_error.is_user() {
            error(error::invalid_data(Some(&hyper_error.to_string())), false)
        } else if hyper_error.is_canceled() {
            error(error::cancelled(Some(&hyper_error.to_string())), true)
        } else if hyper_error.is_closed() {
            error(
                error::connection_closed(Some(&hyper_error.to_string())),
                true,
            )
        } else if hyper_error.is_connect() {
            error(
                error::connection_failed(Some(&hyper_error.to_string())),
                true,
            )
        } else if hyper_error.is_incomplete_message() {
            error(
                error::connection_closed(Some(&hyper_error.to_string())),
                true,
            )
        } else {
            error(error::invalid_data(Some(&hyper_error.to_string())), true)
        }
    }
}

impl From<hyper::error::Error> for StorageError {
    fn from(hyper_error: hyper::error::Error) -> StorageError {
        let b2_error: B2Error = hyper_error.into();
        b2_error.into()
    }
}

impl From<http::Error> for StorageError {
    fn from(error: http::Error) -> StorageError {
        error::other_error(Some(&error.to_string()))
    }
}

impl From<serde_json::error::Error> for StorageError {
    fn from(error: serde_json::error::Error) -> StorageError {
        error::internal_error(Some(&format!("Failed to encode request data: {}", error)))
    }
}

fn generate_error(method: &str, client_id: usize, path: &ObjectPath, response: &str) -> B2Error {
    fn error(error: StorageError) -> B2Error {
        B2Error {
            error,
            needs_auth: false,
            can_retry: false,
        }
    }

    let error_info: ErrorResponse = match from_str(response) {
        Ok(r) => r,
        Err(e) => {
            error!(
                "Client {:04}: Unable to parse ErrorResponse structure from {}.",
                client_id, response
            );
            return error(error::invalid_data(Some(&format!(
                "Unable to parse error response from {}: {}.",
                method, e
            ))));
        }
    };
    warn!(
        "Client {:04}: The API call {} failed with {:?}",
        client_id, method, error_info
    );

    match (error_info.status, error_info.code.as_str()) {
        (400, "bad_request") => error(error::internal_error(Some(&error_info.message))),
        (400, "bad_bucket_id") => {
            error(error::not_found(path.to_owned(), Some(&error_info.message)))
        }
        (400, "invalid_bucket_id") => {
            error(error::not_found(path.to_owned(), Some(&error_info.message)))
        }
        (400, "too_many_buckets") => error(error::over_quota(Some(&error_info.code))),
        (400, "duplicate_bucket_name") => error(error::already_exists(
            path.to_owned(),
            Some(&error_info.code),
        )),
        (400, "file_not_present") => {
            error(error::not_found(path.to_owned(), Some(&error_info.message)))
        }
        (400, "out_of_range") => error(error::internal_error(Some(&error_info.message))),
        (400, "cap_exceeded") => error(error::over_quota(Some(&error_info.code))),

        (401, "unsupported") => error(error::access_denied(Some(&error_info.message))),
        (401, "unauthorized") => error(error::access_denied(Some(&error_info.message))),
        (401, "bad_auth_token") => B2Error {
            error: error::access_expired(Some(&error_info.message)),
            needs_auth: true,
            can_retry: true,
        },
        (401, "expired_auth_token") => B2Error {
            error: error::access_expired(Some(&error_info.message)),
            needs_auth: true,
            can_retry: true,
        },

        (403, "cap_exceeded") => error(error::over_quota(Some(&error_info.code))),

        (404, "not_found") => error(error::not_found(path.clone(), Some(&error_info.message))),

        (405, "method_not_allowed") => error(error::internal_error(Some(&error_info.message))),

        (408, "request_timeout") => B2Error {
            error: error::connection_closed(Some(&error_info.message)),
            needs_auth: true,
            can_retry: true,
        },

        (416, "range_not_satisfiable") => error(error::internal_error(Some(&error_info.message))),

        (429, "too_many_requests") => B2Error {
            error: error::over_quota(Some(&error_info.message)),
            needs_auth: true,
            can_retry: true,
        },

        (500, "internal_error") => B2Error {
            error: error::service_error(Some(&error_info.message)),
            needs_auth: true,
            can_retry: true,
        },
        (503, "bad_request") => B2Error {
            error: error::service_error(Some(&error_info.message)),
            needs_auth: true,
            can_retry: true,
        },

        (status, _) => {
            if status == 400 {
                error(error::internal_error(Some(&error_info.message)))
            } else if status == 401 {
                B2Error {
                    error: error::access_expired(Some(&error_info.message)),
                    needs_auth: true,
                    can_retry: true,
                }
            } else if status >= 500 && status < 600 {
                B2Error {
                    error: error::service_error(Some(&error_info.message)),
                    needs_auth: true,
                    can_retry: true,
                }
            } else {
                B2Error {
                    error: error::other_error(Some(&error_info.message)),
                    needs_auth: true,
                    can_retry: true,
                }
            }
        }
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

#[derive(Debug, Clone)]
pub(super) struct B2Client {}

impl B2Client {
    fn api_url(host: &str, method: &str) -> String {
        format!("{}/b2api/{}/{}", host, B2_VERSION, method)
    }

    async fn request(
        id: usize,
        method: &str,
        path: ObjectPath,
        client: &Client,
        request: Request<Body>,
    ) -> B2Result<Response<Body>> {
        trace!("Client {:04}: Requesting {}", id, request.uri());
        let response = match client.request(request).await {
            Ok(r) => {
                trace!("Client {:04}: {} b2 api call succeeded", id, method);
                r
            }
            Err(e) => {
                error!("Client {:04}: {} b2 api call failed: {}", id, method, e);
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
            Err(generate_error(method, id, &path, &data))
        }
    }

    async fn basic_request<R>(
        id: usize,
        method: &str,
        path: ObjectPath,
        mut client: Client,
        request: Request<Body>,
    ) -> B2Result<R>
    where
        R: DeserializeOwned + fmt::Debug,
    {
        let response = B2Client::request(id, method, path, &client, request).await?;
        let (_, body) = response.into_parts();

        let mut data: String = String::new();
        BlockingStreamReader::from_stream(body)
            .read_to_string(&mut data)
            .unwrap();

        // Make sure that client stays alive until the request is complete.
        client.release();

        match from_str(&data) {
            Ok(r) => {
                trace!("Client {:04}: {} api method returned {:?}", id, method, r);
                Ok(r)
            }
            Err(e) => {
                error!("Client {:04}: {} api method failed: {}", id, method, e);
                Err(B2Error {
                    error: error::invalid_data(Some(&format!(
                        "Unable to parse response from {}: {}.",
                        method, e
                    ))),
                    needs_auth: false,
                    can_retry: true,
                })
            }
        }
    }

    pub async fn authorize(
        settings: B2Settings,
        clients: ClientPool,
    ) -> StorageResult<AuthorizeAccountResponse> {
        let secret = format!(
            "Basic {}",
            encode(&format!("{}:{}", settings.key_id, settings.key))
        );

        trace!(
            "Authorization: Starting b2_authorize_account api call with {}",
            secret
        );

        let request = Request::builder()
            .method(Method::GET)
            .uri(B2Client::api_url(&settings.host, "b2_authorize_account"))
            .header(header::AUTHORIZATION, secret)
            .header(header::USER_AGENT, settings.user_agent)
            .body(Body::empty())?;

        let empty = ObjectPath::empty();
        let client = clients.acquire().await;
        Ok(B2Client::basic_request(0, "b2_authorize_account", empty, client, request).await?)
    }
}

#[derive(Debug)]
pub(super) struct B2APIState {
    pub settings: B2Settings,
    pub clients: ClientPool,
    pub next_id: Arc<AtomicUsize>,
    pub auth_tokens: Pool<(B2Settings, ClientPool), AuthorizeAccountResponse, StorageError>,
}

impl Clone for B2APIState {
    fn clone(&self) -> B2APIState {
        B2APIState {
            settings: self.settings.clone(),
            clients: self.clients.clone(),
            next_id: self.next_id.clone(),
            auth_tokens: self.auth_tokens.clone(),
        }
    }
}

#[derive(Debug)]
pub(super) struct B2API {
    id: usize,
    state: B2APIState,
}

impl Clone for B2API {
    fn clone(&self) -> B2API {
        B2API::new(&self.state)
    }
}

impl B2API {
    pub fn new(state: &B2APIState) -> B2API {
        B2API {
            id: state.next_id.fetch_add(1, Ordering::SeqCst),
            state: state.clone(),
        }
    }

    async fn b2_api_call<S, Q>(self, method: &str, path: ObjectPath, request: S) -> StorageResult<Q>
    where
        S: serde::ser::Serialize + Clone + fmt::Debug,
        for<'de> Q: serde::de::Deserialize<'de> + fmt::Debug,
    {
        let mut tries: usize = 0;
        loop {
            let mut auth_info = self.state.auth_tokens.acquire().await?;

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
                .uri(B2Client::api_url(&auth_info.api_url, method))
                .header(header::AUTHORIZATION, &auth_info.authorization_token)
                .header(header::USER_AGENT, &self.state.settings.user_agent)
                .body(data.into())?;

            let client = self.state.clients.acquire().await;

            match B2Client::basic_request(self.id, method, path.clone(), client, request).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    if e.needs_auth {
                        auth_info.destroy();
                    }

                    tries += 1;

                    if !e.can_retry || tries >= MAX_API_RETRIES {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    pub async fn account_info(&self) -> StorageResult<AuthorizeAccountResponse> {
        let auth_info = self.state.auth_tokens.acquire().await?;
        let mut account_info = auth_info.deref().clone();
        account_info.authorization_token = String::new();
        Ok(account_info)
    }

    pub async fn b2_download_file_by_name(
        self,
        path: ObjectPath,
        bucket: String,
        file: String,
    ) -> StorageResult<impl Stream<Item = Result<Chunk, hyper::Error>>> {
        let mut tries: usize = 0;
        loop {
            let mut auth_info = self.state.auth_tokens.acquire().await?;

            trace!(
                "Client {:04}: Starting {} api call (attempt {})",
                self.id,
                "b2_download_file_by_name",
                tries + 1,
            );

            let request = Request::builder()
                .method(Method::GET)
                .header(header::AUTHORIZATION, &auth_info.authorization_token)
                .header(header::USER_AGENT, &self.state.settings.user_agent)
                .uri(format!(
                    "{}/file/{}/{}",
                    auth_info.download_url,
                    percent_encode(&bucket),
                    percent_encode(&file)
                ))
                .body(Body::empty())?;

            let mut client = self.state.clients.acquire().await;
            match B2Client::request(
                self.id,
                "b2_download_file_by_name",
                path.clone(),
                &client,
                request,
            )
            .await
            {
                Ok(response) => {
                    let (_, body) = response.into_parts();
                    let stream = AfterStream::after(body, move || client.release());

                    return Ok(stream);
                }
                Err(e) => {
                    client.release();
                    if e.needs_auth {
                        auth_info.destroy();
                    }

                    tries += 1;

                    if !e.can_retry || tries >= MAX_API_RETRIES {
                        return Err(e.into());
                    }
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
                .header(header::USER_AGENT, &self.state.settings.user_agent)
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

            let client = self.state.clients.acquire().await;
            match B2Client::basic_request(self.id, "b2_upload_file", path.clone(), client, request)
                .await
            {
                Ok(response) => return Ok(response),
                Err(e) => {
                    tries += 1;

                    if !e.can_retry || tries >= MAX_API_RETRIES {
                        return Err(e.into());
                    }
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
                .header(header::USER_AGENT, &self.state.settings.user_agent)
                .header(B2_HEADER_PART_NUMBER, part)
                .header(header::CONTENT_LENGTH, length)
                .header(B2_HEADER_CONTENT_SHA1, &hash)
                .body(Body::wrap_stream(
                    iter(data.clone()).map(Ok::<_, StorageError>),
                ))?;

            let client = self.state.clients.acquire().await;
            match B2Client::basic_request(self.id, "b2_upload_part", path.clone(), client, request)
                .await
            {
                Ok(response) => return Ok(response),
                Err(e) => {
                    tries += 1;

                    if !e.can_retry || tries >= MAX_API_RETRIES {
                        return Err(e.into());
                    }
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
}
