#![cfg(feature = "b2")]

use std::cmp::Ord;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fs::{metadata, read, read_dir, DirEntry};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use base64::encode;
use futures::channel::oneshot::{channel, Sender};
use futures::compat::*;
use futures::future::{ready, TryFutureExt};
use futures::lock::Mutex;
use futures::stream::{iter, TryStreamExt};
use http::header;
use http::request::Parts;
use http::StatusCode;
use hyper::server::Server;
use hyper::service::service_fn;
use hyper::{Body, Chunk, Request, Response};
use serde_json::{from_slice, to_string_pretty};
use uuid::Uuid;

use storage_types::b2::v2::requests::*;
use storage_types::b2::v2::responses::*;
use storage_types::JSInt as Int;

use crate::runner::TestResult;

const TEST_KEY_ID: &str = "foo";
const TEST_KEY: &str = "bar";
const TEST_ACCOUNT_ID: &str = "foobarbaz";

/// How many uses can an auth token see before it expires?
const AUTH_TIMEOUT: usize = 2;
const DEFAULT_FILE_COUNT: usize = 2;
const BUCKET_ID_PREFIX: &str = "bkt_";
const FILE_ID_PREFIX: &str = "id_";

type B2Result = Result<Response<Body>, B2Error>;

#[derive(Debug)]
struct B2Error {
    status: StatusCode,
    code: String,
    message: String,
}

impl B2Error {
    fn new<C, M>(status: StatusCode, code: C, message: M) -> B2Error
    where
        C: Display,
        M: Display,
    {
        B2Error {
            status,
            code: code.to_string(),
            message: message.to_string(),
        }
    }

    fn not_found<P>(path: P) -> B2Error
    where
        P: AsRef<Path>,
    {
        B2Error::new(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("{} did not exist.", path.as_ref().display()),
        )
    }

    fn path_error<P>(path: P, error: io::Error) -> B2Error
    where
        P: AsRef<Path>,
    {
        if error.kind() == io::ErrorKind::NotFound {
            B2Error::not_found(path.as_ref())
        } else {
            B2Error::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                format!("Error accessing {}: {}", path.as_ref().display(), error),
            )
        }
    }

    fn invalid_bucket_id<D>(id: D) -> B2Error
    where
        D: Display,
    {
        B2Error::new(
            StatusCode::BAD_REQUEST,
            "invalid_bucket_id",
            format!("Invalid bucketId: {}", id),
        )
    }

    fn invalid_parameters<D>(message: D) -> B2Error
    where
        D: Display,
    {
        B2Error::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }
}

impl Display for B2Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(&format!("{:?}", self))
    }
}

impl From<serde_json::Error> for B2Error {
    fn from(error: serde_json::Error) -> B2Error {
        B2Error::new(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("Failed to parse request: {}", error),
        )
    }
}

impl From<B2Error> for Response<Body> {
    fn from(error: B2Error) -> Response<Body> {
        let response = ErrorResponse {
            status: Int::from(error.status.as_u16()),
            code: error.code.clone(),
            message: error.message.clone(),
        };

        Response::builder()
            .status(error.status)
            .body(
                to_string_pretty(&response)
                    .expect("Failed to serialize error.")
                    .into(),
            )
            .expect("Failed to build error response.")
    }
}

impl From<io::Error> for B2Error {
    fn from(error: io::Error) -> B2Error {
        if error.kind() == io::ErrorKind::NotFound {
            B2Error::new(StatusCode::NOT_FOUND, "not_found", format!("{}", error))
        } else {
            B2Error::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                format!("{}", error),
            )
        }
    }
}

trait IntoPathError<R> {
    fn into_path_err<P>(self, path: P) -> Result<R, B2Error>
    where
        P: AsRef<Path>;
}

impl<R> IntoPathError<R> for Result<R, io::Error> {
    fn into_path_err<P>(self, path: P) -> Result<R, B2Error>
    where
        P: AsRef<Path>,
    {
        self.map_err(|e| B2Error::path_error(path, e))
    }
}

macro_rules! api_response {
    ($body:expr) => {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(
                to_string_pretty(&$body)
                    .expect("Failed to serialize response.")
                    .into(),
            )
            .expect("Failed to build response."))
    };
}

macro_rules! api_method {
    ($method:ident, $self:expr, $found:expr, $head:expr, $data:expr) => {
        if $found == stringify!($method) {
            return $self.$method($head, from_slice(&$data)?).await;
        }
    };
}

#[allow(clippy::large_enum_variant)]
enum ListResult {
    Item(FileInfo),
    Error(B2Error),
    Done,
    TryAgain,
}

struct FileLister {
    bucket_id: String,
    prefix: String,
    delimiter: Option<String>,
    state: Vec<(String, Vec<Result<DirEntry, B2Error>>)>,
    last_name: Option<String>,
}

impl FileLister {
    fn new(
        bucket_id: &str,
        dir: &Path,
        prefix: &str,
        delimiter: &Option<String>,
    ) -> Result<FileLister, B2Error> {
        let mut lister = FileLister {
            bucket_id: bucket_id.to_owned(),
            prefix: prefix.to_owned(),
            delimiter: delimiter.to_owned(),
            state: Default::default(),
            last_name: None,
        };

        lister.push_state("", dir)?;

        Ok(lister)
    }
}

impl FileLister {
    fn push_state(&mut self, path: &str, dir: &Path) -> Result<(), B2Error> {
        let mut list: Vec<Result<DirEntry, B2Error>> = read_dir(dir)
            .into_path_err(dir)?
            .map(|r| r.map_err(B2Error::from))
            .collect();
        list.sort_unstable_by(|a, b| match (a, b) {
            (Ok(entry_a), Ok(entry_b)) => entry_b.file_name().cmp(&entry_a.file_name()),
            (Ok(_), Err(_)) => Ordering::Greater,
            (Err(_), Ok(_)) => Ordering::Less,
            (Err(_), Err(_)) => Ordering::Equal,
        });

        self.state.push((path.to_owned(), list));

        Ok(())
    }

    fn next_info(&mut self) -> ListResult {
        match self.state.last_mut() {
            Some((path, list)) => match list.pop() {
                Some(Ok(entry)) => {
                    let file_name = entry.file_name().to_str().unwrap().to_owned();
                    let file_path = if path.is_empty() {
                        file_name.clone()
                    } else {
                        format!("{}/{}", path, &file_name)
                    };

                    let meta = match entry.metadata().into_path_err(entry.path()) {
                        Ok(m) => m,
                        Err(e) => return ListResult::Error(e),
                    };

                    if meta.is_dir() {
                        match self.push_state(&file_path, &entry.path().as_path()) {
                            Ok(()) => ListResult::TryAgain,
                            Err(e) => ListResult::Error(e),
                        }
                    } else if file_path.starts_with(&self.prefix) {
                        if let Some(delimiter) = &self.delimiter {
                            let suffix = file_path[self.prefix.len()..].to_owned();
                            if let Some(pos) = suffix.find(delimiter.as_str()) {
                                let len = self.prefix.len() + pos + delimiter.len();
                                let file_path = file_path[0..len].to_owned();

                                return ListResult::Item(FileInfo {
                                    account_id: TEST_ACCOUNT_ID.to_owned(),
                                    action: String::from("folder"),
                                    bucket_id: self.bucket_id.clone(),
                                    content_length: 0,
                                    content_sha1: None,
                                    content_type: None,
                                    file_id: None,
                                    file_info: Default::default(),
                                    file_name: file_path,
                                    upload_timestamp: 0,
                                });
                            }
                        }

                        ListResult::Item(FileInfo {
                            account_id: TEST_ACCOUNT_ID.to_owned(),
                            action: String::from("upload"),
                            bucket_id: self.bucket_id.clone(),
                            content_length: meta.len(),
                            content_sha1: None,
                            content_type: None,
                            file_id: Some(format!("{}{}", FILE_ID_PREFIX, file_path)),
                            file_info: Default::default(),
                            file_name: file_path,
                            upload_timestamp: 0,
                        })
                    } else {
                        ListResult::TryAgain
                    }
                }
                Some(Err(e)) => ListResult::Error(e),
                None => {
                    self.state.pop();
                    ListResult::TryAgain
                }
            },
            None => ListResult::Done,
        }
    }
}

impl Iterator for FileLister {
    type Item = Result<FileInfo, B2Error>;

    fn next(&mut self) -> Option<Result<FileInfo, B2Error>> {
        loop {
            match self.next_info() {
                ListResult::Item(info) => match &self.last_name {
                    Some(last_name) => {
                        if last_name != &info.file_name {
                            self.last_name = Some(info.file_name.clone());
                            return Some(Ok(info));
                        }
                    }
                    None => {
                        self.last_name = Some(info.file_name.clone());
                        return Some(Ok(info));
                    }
                },
                ListResult::Error(e) => return Some(Err(e)),
                ListResult::Done => return None,
                ListResult::TryAgain => (),
            }
        }
    }
}

#[derive(Default)]
struct B2ServerState {
    authorizations: HashMap<String, usize>,
}

impl B2ServerState {
    fn new() -> B2ServerState {
        Default::default()
    }
}

#[derive(Clone)]
struct B2Server {
    addr: SocketAddr,
    root: PathBuf,
    state: Arc<Mutex<B2ServerState>>,
}

impl B2Server {
    async fn b2_authorize_account(self, auth: &str) -> B2Result {
        let mut state = self.state.lock().await;
        let expected = format!("Basic {}", encode(&format!("{}:{}", TEST_KEY_ID, TEST_KEY)));

        let base = format!("http://{}", self.addr);
        let api_url = format!("{}/api", base);
        let download_url = format!("{}/download", base);

        if expected == auth {
            let uuid = Uuid::new_v4().to_string();
            state.authorizations.insert(uuid.clone(), 0);

            api_response!(AuthorizeAccountResponse {
                account_id: String::from(TEST_ACCOUNT_ID),
                authorization_token: uuid,
                allowed: AuthorizeAccountAllowed {
                    capabilities: vec![],
                    bucket_id: None,
                    bucket_name: None,
                    name_prefix: None,
                },
                api_url,
                download_url,
                recommended_part_size: 1000,
                absolute_minimum_part_size: 500,
            })
        } else {
            Err(B2Error::new(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Incorrect key id or key.",
            ))
        }
    }

    async fn b2_list_buckets(self, _head: Parts, body: ListBucketsRequest) -> B2Result {
        if body.account_id != TEST_ACCOUNT_ID {
            return Err(B2Error::new(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "This key cannot access buckets from the requested account.",
            ));
        }

        if !body.bucket_types.includes(BucketType::Public) {
            return api_response!(ListBucketsResponse {
                buckets: Vec::new()
            });
        }

        let name = match (body.bucket_id, body.bucket_name) {
            (Some(id), None) => {
                if !id.starts_with(BUCKET_ID_PREFIX) {
                    return Err(B2Error::invalid_bucket_id(&id));
                }
                Some(id[BUCKET_ID_PREFIX.len()..].to_owned())
            }
            (None, Some(name)) => Some(name),
            (None, None) => None,
            _ => {
                return Err(B2Error::invalid_parameters(
                    "Requested both bucketId and bucketName.",
                ));
            }
        };

        if let Some(name) = name {
            let mut path = self.root.clone();
            path.push(&name);

            let meta = metadata(&path).into_path_err(&path)?;

            if !meta.is_dir() {
                return Err(B2Error::not_found(&path));
            }

            return api_response!(ListBucketsResponse {
                buckets: vec![Bucket {
                    account_id: String::from(TEST_ACCOUNT_ID),
                    bucket_id: format!("{}{}", BUCKET_ID_PREFIX, &name),
                    bucket_name: name.to_owned(),
                    bucket_type: BucketType::Public,
                    bucket_info: Default::default(),
                    cors_rules: Default::default(),
                    lifecycle_rules: Default::default(),
                    revision: 0,
                }]
            });
        }

        let buckets: Vec<Bucket> = read_dir(&self.root)
            .into_path_err(&self.root)?
            .filter_map(|result| {
                let entry = match result {
                    Ok(e) => e,
                    _ => return None,
                };

                match entry.metadata() {
                    Ok(m) => {
                        if !m.is_dir() {
                            return None;
                        }
                    }
                    _ => return None,
                }

                let name = match entry.file_name().into_string() {
                    Ok(s) => s,
                    _ => panic!("Path at {} uses an invalid name.", entry.path().display()),
                };

                Some(Bucket {
                    account_id: String::from(TEST_ACCOUNT_ID),
                    bucket_id: format!("{}{}", BUCKET_ID_PREFIX, name),
                    bucket_name: name.to_owned(),
                    bucket_type: BucketType::Public,
                    bucket_info: Default::default(),
                    cors_rules: Default::default(),
                    lifecycle_rules: Default::default(),
                    revision: 0,
                })
            })
            .collect();

        api_response!(ListBucketsResponse { buckets })
    }

    async fn b2_list_file_names(self, _head: Parts, body: ListFileNamesRequest) -> B2Result {
        if !body.bucket_id.starts_with(BUCKET_ID_PREFIX) {
            return Err(B2Error::invalid_bucket_id(&body.bucket_id));
        }

        let mut dir = self.root.clone();
        dir.push(&body.bucket_id[BUCKET_ID_PREFIX.len()..]);
        let start = body.start_file_name.unwrap_or_else(String::new);

        let lister = FileLister::new(
            &body.bucket_id,
            dir.as_path(),
            &body.prefix.unwrap_or_else(String::new),
            &body.delimiter,
        )?
        .filter(|result| match result {
            Ok(info) => info.file_name >= start,
            Err(_) => true,
        });

        let mut response = ListFileNamesResponse {
            files: Default::default(),
            next_file_name: None,
        };

        for result in lister {
            let info = result?;

            if response.files.len() < DEFAULT_FILE_COUNT {
                response.files.push(info);
            } else if response.files.len() == DEFAULT_FILE_COUNT {
                response.next_file_name = Some(info.file_name);
                break;
            }
        }

        api_response!(response)
    }

    async fn check_auth(&self, auth: &str) -> Result<(), B2Error> {
        let mut state = self.state.lock().await;
        match state.authorizations.remove(auth) {
            Some(c) => {
                if c < AUTH_TIMEOUT {
                    state.authorizations.insert(auth.to_owned(), c + 1);
                    Ok(())
                } else {
                    Err(B2Error::new(
                        StatusCode::UNAUTHORIZED,
                        "expired_auth_token",
                        "Auth token has expired.",
                    ))
                }
            }
            None => Err(B2Error::new(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Unknown auth token.",
            )),
        }
    }

    async fn serve(self, request: Request<Body>) -> B2Result {
        let (head, body) = request.into_parts();

        let path = match head.uri.path_and_query() {
            Some(p) => p.path(),
            None => {
                return Err(B2Error::invalid_parameters("Request contained no path."));
            }
        };

        let auth = match head.headers.get(header::AUTHORIZATION) {
            Some(a) => a.to_str().map_err(|e| {
                B2Error::new(
                    StatusCode::UNAUTHORIZED,
                    "unauthorized",
                    format!("Request contained an invalid authorization: {}", e),
                )
            })?,
            None => {
                return Err(B2Error::new(
                    StatusCode::UNAUTHORIZED,
                    "unauthorized",
                    "Request contained no authorization.",
                ));
            }
        };

        if path.starts_with("/b2api/v2/b2_authorize_account") {
            self.b2_authorize_account(&auth).await
        } else if path.starts_with("/api/b2api/v2/") {
            self.check_auth(&auth).await?;

            let method = &path[14..];

            let data = body.compat().try_concat().await.map_err(|e| {
                B2Error::invalid_parameters(format!("Failed to receive entire body: {}", e))
            })?;

            api_method!(b2_list_buckets, self, method, head, data);
            api_method!(b2_list_file_names, self, method, head, data);

            Err(B2Error::invalid_parameters("Invalid API method requested."))
        } else if path.starts_with("/download/file/") {
            self.check_auth(&auth).await?;

            let path = &path[15..];
            let mut file = self.root.clone();
            file.push(path);

            let meta = metadata(&file).into_path_err(&file)?;
            if !meta.is_file() {
                return Err(B2Error::not_found(&file));
            }

            let source = read(&file).into_path_err(file)?;
            let mut len = source.len() / 5;
            if len == 0 {
                len = 1;
            }

            let blocks: Vec<io::Result<Chunk>> = source
                .chunks(len)
                .map(|s| {
                    let mut result: Vec<u8> = Default::default();
                    result.extend_from_slice(s);
                    io::Result::<Chunk>::Ok(result.into())
                })
                .collect();

            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Body::wrap_stream(Compat::new(iter(blocks))))
                .expect("Failed to build response."))
        } else {
            Err(B2Error::invalid_parameters("Invalid path requested."))
        }
    }
}

pub fn build_server(
    root: PathBuf,
) -> TestResult<(SocketAddr, impl Future<Output = Result<(), ()>>, Sender<()>)> {
    let (sender, receiver) = channel::<()>();
    let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
    let listener = TcpListener::bind(addr).expect("Failed to bind to server socket.");
    let addr = listener
        .local_addr()
        .expect("Failed to bind to server socket.");

    let b2_server = B2Server {
        addr,
        state: Arc::new(Mutex::new(B2ServerState::new())),
        root,
    };

    let http_server = Server::from_tcp(listener)
        .expect("Failed to attach to tcp stream.")
        .serve(move || {
            let server = b2_server.clone();
            service_fn(move |req| {
                let response = server
                    .clone()
                    .serve(req)
                    .or_else(|e| ready(Ok(e.into()) as Result<Response<Body>, io::Error>));
                Compat::new(Box::pin(response))
            })
        });

    let server_future = http_server
        .with_graceful_shutdown(receiver.compat())
        .compat()
        .map_err(|e| panic!(e.to_string()));

    Ok((addr, server_future, sender))
}
