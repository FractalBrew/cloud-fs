#![cfg(feature = "b2")]

use std::cmp::Ord;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Display;
use std::fs::{metadata, read, read_dir, remove_file, DirEntry, File};
use std::io;
use std::io::{BufWriter, Write};
use std::net::SocketAddr;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use base64::encode;
use filetime::{set_file_mtime, FileTime};
use futures::channel::oneshot::{channel, Sender};
use futures::future::FutureExt;
use futures::lock::Mutex;
use futures::stream::{iter, TryStreamExt};
use http::header;
use http::header::{AsHeaderName, HeaderMap};
use http::request::Parts;
use http::StatusCode;
use hyper::server::Server;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Chunk, Request, Response};
use serde_json::{from_slice, to_string_pretty};
use sha1::Sha1;
use tokio::spawn;
use uuid::Uuid;

use storage_types::b2::v2::requests::*;
use storage_types::b2::v2::responses::*;
use storage_types::b2::v2::{
    percent_decode, BucketType, FileAction, Int, UserFileInfo, B2_HEADER_CONTENT_SHA1,
    B2_HEADER_FILE_INFO_PREFIX, B2_HEADER_FILE_NAME, B2_HEADER_PART_NUMBER, LAST_MODIFIED_KEY,
};

use crate::runner::TestResult;

const TEST_KEY_ID: &str = "foo";
const TEST_KEY: &str = "bar";
const TEST_ACCOUNT_ID: &str = "foobarbaz";

/// How many uses can an auth token see before it expires?
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

    fn request_timeout<D>(message: D) -> B2Error
    where
        D: Display,
    {
        B2Error::new(StatusCode::REQUEST_TIMEOUT, "request_timeout", message)
    }

    fn method_not_allowed<D>(message: D) -> B2Error
    where
        D: Display,
    {
        B2Error::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            message,
        )
    }

    fn server_error<D>(message: D) -> B2Error
    where
        D: Display,
    {
        B2Error::new(StatusCode::INTERNAL_SERVER_ERROR, "server_error", message)
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

fn header_or_error<I>(headers: &HeaderMap, name: I) -> Result<String, B2Error>
where
    I: AsHeaderName + Display + Clone,
{
    match headers.get(name.clone()) {
        Some(val) => match val.to_str() {
            Ok(s) => Ok(s.to_owned()),
            Err(_) => Err(B2Error::invalid_parameters(format!(
                "Header {} was invalid.",
                name
            ))),
        },
        None => Err(B2Error::invalid_parameters(format!(
            "No header {} provided.",
            name
        ))),
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
                                    action: FileAction::Folder,
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

                        let mut info = UserFileInfo::new();
                        if let Ok(time) = meta.modified() {
                            if let Ok(dur) = time.duration_since(UNIX_EPOCH) {
                                info.insert(
                                    LAST_MODIFIED_KEY.to_owned(),
                                    dur.as_millis().to_string(),
                                );
                            }
                        }

                        ListResult::Item(FileInfo {
                            account_id: TEST_ACCOUNT_ID.to_owned(),
                            action: FileAction::Upload,
                            bucket_id: self.bucket_id.clone(),
                            content_length: meta.len(),
                            content_sha1: None,
                            content_type: None,
                            file_id: Some(format!("{}{}", FILE_ID_PREFIX, entry.path().display())),
                            file_info: info,
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

struct LargeUpload {
    file_name: String,
    bucket_id: String,
    auth: HashSet<String>,
    parts: HashMap<usize, (Vec<Chunk>, String)>,
}

impl LargeUpload {
    fn new(file_name: &str, bucket_id: &str) -> LargeUpload {
        LargeUpload {
            file_name: file_name.to_owned(),
            bucket_id: bucket_id.to_owned(),
            auth: Default::default(),
            parts: Default::default(),
        }
    }
}

#[derive(Default)]
struct B2ServerState {
    authorizations: HashMap<String, usize>,
    upload_authorizations: HashMap<String, String>,
    large_uploads: HashMap<String, LargeUpload>,
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
    auth_timeout: usize,
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

    async fn b2_list_file_versions(self, _head: Parts, body: ListFileVersionsRequest) -> B2Result {
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

        let mut response = ListFileVersionsResponse {
            files: Default::default(),
            next_file_name: None,
            next_file_id: None,
        };

        for result in lister {
            let info = result?;

            if response.files.len() < DEFAULT_FILE_COUNT {
                response.files.push(info);
            } else if response.files.len() == DEFAULT_FILE_COUNT {
                response.next_file_name = Some(info.file_name);
                response.next_file_id = info.file_id;
                break;
            }
        }

        api_response!(response)
    }

    async fn b2_delete_file_version(
        self,
        _head: Parts,
        body: DeleteFileVersionRequest,
    ) -> B2Result {
        if !body.file_id.starts_with(FILE_ID_PREFIX) {
            return Err(B2Error::new(
                StatusCode::BAD_REQUEST,
                "file_not_present",
                format!("File not present: {} {}", body.file_name, body.file_id),
            ));
        }

        let path = &body.file_id[FILE_ID_PREFIX.len()..];

        match metadata(path) {
            Ok(meta) => {
                if !meta.is_file() {
                    return Err(B2Error::new(
                        StatusCode::BAD_REQUEST,
                        "file_not_present",
                        format!("File not present: {} {}", body.file_name, body.file_id),
                    ));
                }

                remove_file(path)?;

                api_response!(DeleteFileVersionResponse {
                    file_id: body.file_id,
                    file_name: body.file_name,
                })
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    Err(B2Error::new(
                        StatusCode::BAD_REQUEST,
                        "file_not_present",
                        format!("File not present: {} {}", body.file_name, body.file_id),
                    ))
                } else {
                    Err(e.into())
                }
            }
        }
    }

    async fn b2_download_file(self, path: &str) -> B2Result {
        let path = match percent_decode(path) {
            Ok(s) => s,
            Err(_) => return Err(B2Error::invalid_parameters("File path was invalid utf-8.")),
        };

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
            .body(Body::wrap_stream(iter(blocks)))
            .expect("Failed to build response."))
    }

    async fn b2_get_upload_url(self, _head: Parts, body: GetUploadUrlRequest) -> B2Result {
        if !&body.bucket_id.starts_with(BUCKET_ID_PREFIX) {
            return Err(B2Error::new(
                StatusCode::BAD_REQUEST,
                "bad_request",
                format!("Invalid bucket id: {}", body.bucket_id),
            ));
        }

        let mut bucket = self.root.clone();
        bucket.push(&body.bucket_id[BUCKET_ID_PREFIX.len()..]);

        match metadata(bucket) {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(B2Error::new(
                        StatusCode::BAD_REQUEST,
                        "bad_request",
                        format!("Invalid bucket id: {}", body.bucket_id),
                    ));
                }
            }
            Err(_) => {
                return Err(B2Error::new(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    format!("Invalid bucket id: {}", body.bucket_id),
                ));
            }
        }

        let auth = Uuid::new_v4().to_string();
        let mut state = self.state.lock().await;
        state
            .upload_authorizations
            .insert(auth.clone(), body.bucket_id.clone());

        api_response!(GetUploadUrlResponse {
            upload_url: format!("http://{}/upload/file/{}", self.addr, body.bucket_id),
            bucket_id: body.bucket_id,
            authorization_token: auth,
        })
    }

    async fn b2_upload_file(self, bucket_id: &str, head: Parts, mut body: Body) -> B2Result {
        let file = match percent_decode(&header_or_error(&head.headers, B2_HEADER_FILE_NAME)?) {
            Ok(s) => s,
            Err(_) => return Err(B2Error::invalid_parameters("Filename was not valid utf-8.")),
        };
        let expected_sha1 = header_or_error(&head.headers, B2_HEADER_CONTENT_SHA1)?;
        let expected_length: u64 =
            match header_or_error(&head.headers, header::CONTENT_LENGTH)?.parse() {
                Ok(len) => len,
                Err(_) => {
                    return Err(B2Error::invalid_parameters(
                        "Content-Length header could not be parsed.",
                    ))
                }
            };

        let last_modified = head
            .headers
            .get(&format!(
                "{}{}",
                B2_HEADER_FILE_INFO_PREFIX, LAST_MODIFIED_KEY
            ))
            .and_then(|t| t.to_str().ok())
            .and_then(|t| t.parse::<u64>().ok())
            .map(|d| UNIX_EPOCH + Duration::from_millis(d));

        let mut path = self.root.clone();
        path.push(&bucket_id[BUCKET_ID_PREFIX.len()..]);
        path.push(&file);
        let mut writer = File::create(&path)?;

        let mut length: Int = 0;
        let mut hasher = Sha1::new();

        loop {
            match body.next().await {
                Some(Ok(chunk)) => {
                    writer.write_all(&chunk)?;
                    hasher.update(&chunk);
                    length += chunk.len() as Int;
                }
                Some(Err(e)) => {
                    return Err(B2Error::request_timeout(format!(
                        "Failed to upload data: {}",
                        e
                    )))
                }
                None => break,
            }
        }

        if length != expected_length {
            return Err(B2Error::invalid_parameters(
                "Content-Length header was not set correctly.",
            ));
        }

        if expected_sha1 != hasher.hexdigest() {
            return Err(B2Error::invalid_parameters(
                "Expected hash did not match data.",
            ));
        }

        if let Some(time) = last_modified {
            if let Err(e) = set_file_mtime(&path, FileTime::from_system_time(time)) {
                return Err(B2Error::server_error(format!(
                    "Failed to set file modification time: {}.",
                    e
                )));
            }
        }

        api_response!(UploadFileResponse {
            account_id: TEST_ACCOUNT_ID.to_owned(),
            action: FileAction::Upload,
            bucket_id: bucket_id.to_owned(),
            content_length: length,
            content_sha1: Some(expected_sha1.to_owned()),
            content_type: Some(String::from("application/octet-stream")),
            file_id: Some(format!("{}", path.display())),
            file_info: Default::default(),
            file_name: file.to_owned(),
            upload_timestamp: 0,
        })
    }

    async fn b2_start_large_file(self, _head: Parts, body: StartLargeFileRequest) -> B2Result {
        if !body.bucket_id.starts_with(BUCKET_ID_PREFIX) {
            return Err(B2Error::invalid_bucket_id(&body.bucket_id));
        }

        let mut path = self.root.clone();
        path.push(&body.bucket_id[BUCKET_ID_PREFIX.len()..]);
        path.push(&body.file_name);

        let file_id = format!("{}{}", FILE_ID_PREFIX, path.display());

        let mut state = self.state.lock().await;
        if state.large_uploads.contains_key(&file_id) {
            return Err(B2Error::invalid_parameters(format!(
                "File {} is already being uploaded.",
                &body.file_name
            )));
        }

        state.large_uploads.insert(
            file_id.clone(),
            LargeUpload::new(&body.file_name, &body.bucket_id),
        );

        api_response!(StartLargeFileResponse {
            account_id: TEST_ACCOUNT_ID.to_owned(),
            action: FileAction::Start,
            bucket_id: body.bucket_id,
            content_length: 0,
            content_sha1: None,
            content_type: Some(body.content_type),
            file_id: Some(file_id),
            file_info: Default::default(),
            file_name: body.file_name,
            upload_timestamp: 0,
        })
    }

    async fn b2_get_upload_part_url(self, _head: Parts, body: GetUploadPartUrlRequest) -> B2Result {
        let mut state = self.state.lock().await;
        match state.large_uploads.get_mut(&body.file_id) {
            Some(upload) => {
                let auth = Uuid::new_v4().to_string();
                upload.auth.insert(auth.clone());

                api_response!(GetUploadPartUrlResponse {
                    authorization_token: auth,
                    file_id: body.file_id.clone(),
                    upload_url: format!("http://{}/upload/part/{}", self.addr, body.file_id),
                })
            }
            None => Err(B2Error::invalid_parameters("Unknown file id.")),
        }
    }

    async fn b2_upload_part(self, file_id: String, head: Parts, mut body: Body) -> B2Result {
        let expected_sha1 = header_or_error(&head.headers, B2_HEADER_CONTENT_SHA1)?;
        let expected_length: u64 =
            match header_or_error(&head.headers, header::CONTENT_LENGTH)?.parse() {
                Ok(len) => len,
                Err(_) => {
                    return Err(B2Error::invalid_parameters(
                        "Content-Length header could not be parsed.",
                    ))
                }
            };
        let part_number: usize =
            match header_or_error(&head.headers, B2_HEADER_PART_NUMBER)?.parse() {
                Ok(len) => len,
                Err(_) => {
                    return Err(B2Error::invalid_parameters(
                        "X-Bz-Part-Number header could not be parsed.",
                    ))
                }
            };

        if part_number < 1 {
            return Err(B2Error::invalid_parameters(
                "X-Bz-Part-Number header contained an invalid part number.",
            ));
        }

        let mut length: Int = 0;
        let mut hasher = Sha1::new();
        let mut data: Vec<Chunk> = Default::default();

        loop {
            match body.next().await {
                Some(Ok(chunk)) => {
                    hasher.update(&chunk);
                    length += chunk.len() as Int;
                    data.push(chunk);
                }
                Some(Err(e)) => {
                    return Err(B2Error::request_timeout(format!(
                        "Failed to upload data: {}",
                        e
                    )))
                }
                None => break,
            }
        }

        if length != expected_length {
            return Err(B2Error::invalid_parameters(
                "Content-Length header was not set correctly.",
            ));
        }

        if expected_sha1 != hasher.hexdigest() {
            return Err(B2Error::invalid_parameters(
                "Expected hash did not match data.",
            ));
        }

        let mut state = self.state.lock().await;
        let upload = match state.large_uploads.get_mut(&file_id) {
            Some(u) => u,
            None => {
                return Err(B2Error::invalid_parameters(
                    "Large upload already completed?",
                ))
            }
        };

        upload
            .parts
            .insert(part_number - 1, (data, expected_sha1.clone()));

        api_response!(UploadPartResponse {
            file_id,
            part_number,
            content_length: expected_length,
            content_sha1: expected_sha1,
            upload_timestamp: 0,
        })
    }

    async fn b2_finish_large_file(self, _head: Parts, body: FinishLargeFileRequest) -> B2Result {
        let mut upload = {
            let mut state = self.state.lock().await;
            match state.large_uploads.remove(&body.file_id) {
                Some(upload) => upload,
                None => return Err(B2Error::invalid_parameters("Unknown file id.")),
            }
        };

        if upload.parts.len() != body.part_sha1_array.len() {
            return Err(B2Error::invalid_parameters("Incorrect number of parts."));
        }

        let path = Path::new(&body.file_id[FILE_ID_PREFIX.len()..]);
        let file = match File::create(path) {
            Ok(f) => f,
            Err(_) => return Err(B2Error::server_error("Failed to create file to write.")),
        };
        let mut writer = BufWriter::new(file);

        let length = 0;
        for i in 0..upload.parts.len() {
            let (data, hash) = match upload.parts.remove(&i) {
                Some(d) => d,
                None => return Err(B2Error::invalid_parameters("Missing part.")),
            };

            if hash != body.part_sha1_array[i] {
                return Err(B2Error::invalid_parameters("Invalid hash."));
            }

            for chunk in data {
                writer.write_all(&chunk)?;
            }
        }

        api_response!(FinishLargeFileResponse {
            account_id: String::from(TEST_ACCOUNT_ID),
            action: FileAction::Upload,
            bucket_id: upload.bucket_id,
            content_length: length,
            content_sha1: None,
            content_type: None,
            file_id: Some(body.file_id),
            file_info: Default::default(),
            file_name: upload.file_name,
            upload_timestamp: 0,
        })
    }

    async fn check_auth(&self, auth: &str) -> Result<(), B2Error> {
        let mut state = self.state.lock().await;
        let count = match state.authorizations.get(auth) {
            Some(c) => c.to_owned(),
            _ => {
                return Err(B2Error::new(
                    StatusCode::UNAUTHORIZED,
                    "unauthorized",
                    "Unknown auth token.",
                ))
            }
        };

        if count < self.auth_timeout {
            state.authorizations.insert(auth.to_owned(), count + 1);
            Ok(())
        } else {
            Err(B2Error::new(
                StatusCode::UNAUTHORIZED,
                "expired_auth_token",
                "Auth token has expired.",
            ))
        }
    }

    async fn call_api(self, method: &str, head: Parts, data: Chunk) -> B2Result {
        api_method!(b2_list_buckets, self, method, head, data);
        api_method!(b2_list_file_names, self, method, head, data);
        api_method!(b2_list_file_versions, self, method, head, data);
        api_method!(b2_delete_file_version, self, method, head, data);
        api_method!(b2_get_upload_url, self, method, head, data);
        api_method!(b2_start_large_file, self, method, head, data);
        api_method!(b2_get_upload_part_url, self, method, head, data);
        api_method!(b2_finish_large_file, self, method, head, data);

        Err(B2Error::invalid_parameters("Invalid API method requested."))
    }

    async fn serve(self, request: Request<Body>) -> B2Result {
        let (head, body) = request.into_parts();

        let path = match head.uri.path_and_query() {
            Some(p) => p.path().to_owned(),
            None => {
                return Err(B2Error::invalid_parameters("Request contained no path."));
            }
        };

        let auth = match head.headers.get(header::AUTHORIZATION) {
            Some(a) => a
                .to_str()
                .map_err(|e| {
                    B2Error::new(
                        StatusCode::UNAUTHORIZED,
                        "unauthorized",
                        format!("Request contained an invalid authorization: {}", e),
                    )
                })?
                .to_owned(),
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

            let data = body.try_concat().await.map_err(|e| {
                B2Error::invalid_parameters(format!("Failed to receive entire body: {}", e))
            })?;
            self.call_api(method, head, data).await
        } else if path.starts_with("/download/file/") {
            let target = &path[15..];
            self.check_auth(&auth).await?;
            self.b2_download_file(target).await
        } else if path.starts_with("/upload/file/") {
            if head.method != "POST" {
                return Err(B2Error::method_not_allowed(
                    "Only POST methods can upload files.",
                ));
            }

            let bucket_id = path[13..].to_owned();
            let mutex = self.state.clone();

            {
                let mut state = mutex.lock().await;
                // Remove the authorization so it cannot be used while this
                // transfer takes place.
                match state.upload_authorizations.remove(&auth) {
                    Some(valid_id) => {
                        if valid_id != bucket_id {
                            return Err(B2Error::new(
                                StatusCode::UNAUTHORIZED,
                                "unauthorized",
                                "Unknown upload auth token for bucket.",
                            ));
                        }
                    }
                    None => {
                        return Err(B2Error::new(
                            StatusCode::UNAUTHORIZED,
                            "unauthorized",
                            "Unknown upload auth token.",
                        ));
                    }
                }
            }

            let result = self.b2_upload_file(&bucket_id, head, body).await;

            {
                // Re-add the authorization for later use.
                let mut state = mutex.lock().await;
                state.upload_authorizations.insert(auth, bucket_id);
            }

            result
        } else if path.starts_with("/upload/part/") {
            if head.method != "POST" {
                return Err(B2Error::method_not_allowed(
                    "Only POST methods can upload parts.",
                ));
            }

            let file_id = path[13..].to_owned();
            let mutex = self.state.clone();
            {
                let mut state = mutex.lock().await;
                let upload = match state.large_uploads.get_mut(&file_id) {
                    Some(upload) => upload,
                    None => {
                        return Err(B2Error::invalid_parameters("Unknown large file ID."));
                    }
                };

                // Remove the authorization so it cannot be used while this
                // transfer takes place.
                if !upload.auth.remove(&auth) {
                    return Err(B2Error::new(
                        StatusCode::UNAUTHORIZED,
                        "unauthorized",
                        "Unknown part auth token.",
                    ));
                }
            }

            let result = self.b2_upload_part(file_id.clone(), head, body).await;

            {
                // Re-add the authorization for later use.
                let mut state = mutex.lock().await;
                if let Some(upload) = state.large_uploads.get_mut(&file_id) {
                    upload.auth.insert(auth);
                }
            }

            result
        } else {
            Err(B2Error::invalid_parameters("Invalid path requested."))
        }
    }
}

pub fn start_server(root: PathBuf, auth_timeout: usize) -> TestResult<(SocketAddr, Sender<()>)> {
    let (shutdown_sender, shutdown_receiver) = channel::<()>();

    let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
    let listener = TcpListener::bind(addr).expect("Failed to bind to server socket.");
    let addr = listener
        .local_addr()
        .expect("Failed to bind to server socket.");

    let b2_server = B2Server {
        addr,
        auth_timeout,
        state: Arc::new(Mutex::new(B2ServerState::new())),
        root,
    };

    let http_server = Server::from_tcp(listener)
        .expect("Failed to attach to tcp stream.")
        .serve(make_service_fn(move |_| {
            let server = b2_server.clone();
            async {
                Ok::<_, io::Error>(service_fn(move |request: Request<Body>| {
                    server
                        .clone()
                        .serve(request)
                        .map(|r| r.or_else(|e| Ok::<Response<Body>, io::Error>(e.into())))
                }))
            }
        }));

    let server_future = http_server
        .with_graceful_shutdown(shutdown_receiver.map(|_| ()))
        .map(|r| match r {
            Ok(()) => (),
            Err(e) => panic!(e.to_string()),
        });

    spawn(server_future);

    Ok((addr, shutdown_sender))
}
