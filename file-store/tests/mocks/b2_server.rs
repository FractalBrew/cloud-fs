#![cfg(feature = "b2")]

use std::io;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;

use base64::encode;
use futures::channel::oneshot::{channel, Sender};
use futures::compat::*;
use futures::future::{Future, TryFutureExt};
use futures::lock::Mutex;
use http::header;
use http::StatusCode;
use hyper::server::Server;
use hyper::service::service_fn;
use hyper::{Body, Request, Response};
use serde_json::to_string_pretty;
use uuid::Uuid;

use storage_types::b2::responses::*;

use crate::runner::TestResult;

const TEST_KEY_ID: &str = "foo";
const TEST_KEY: &str = "bar";

fn error_response(status: StatusCode, code: &str, message: &str) -> Response<Body> {
    let error = ErrorResponse {
        status: status.as_u16(),
        code: code.to_owned(),
        message: message.to_owned(),
    };

    Response::builder()
        .status(status)
        .body(
            to_string_pretty(&error)
                .expect("Failed to serialize error.")
                .into(),
        )
        .expect("Failed to build error response.")
}

#[derive(Default)]
struct B2ServerState {
    authorizations: Vec<String>,
}

impl B2ServerState {
    fn new() -> B2ServerState {
        Default::default()
    }
}

struct B2Server {
    addr: SocketAddr,
    state: Arc<Mutex<B2ServerState>>,
}

impl B2Server {
    async fn handle(
        addr: SocketAddr,
        state: Arc<Mutex<B2ServerState>>,
        request: Request<Body>,
    ) -> io::Result<Response<Body>> {
        let server = B2Server { addr, state };
        Ok(server.serve(request).await)
    }

    async fn b2_authorize_account(&self, request: Request<Body>, auth: &str) -> Response<Body> {
        let mut state = self.state.lock().await;
        let expected = format!("Basic {}", encode(&format!("{}:{}", TEST_KEY_ID, TEST_KEY)));

        let uri = request.uri();
        println!("request uri: {}", uri);
        let base = format!("http://{}", self.addr);

        let api_url = format!("{}/api", base);

        let download_url = format!("{}/download", base);

        if expected == auth {
            let uuid = Uuid::new_v4().to_string();
            state.authorizations.push(uuid.clone());

            let response = AuthorizeAccountResponse {
                account_id: String::from("foo"),
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
            };

            Response::builder()
                .status(StatusCode::OK)
                .body(
                    to_string_pretty(&response)
                        .expect("Failed to serialize response.")
                        .into(),
                )
                .expect("Failed to build response.")
        } else {
            error_response(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Incorrect key id or key.",
            )
        }
    }

    async fn check_auth(&self, auth: &str) -> Result<(), Response<Body>> {
        let state = self.state.lock().await;
        for known in state.authorizations.iter() {
            if auth == known {
                return Ok(());
            }
        }

        Err(error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Unknown auth token.",
        ))
    }

    async fn serve(self, request: Request<Body>) -> Response<Body> {
        let uri = request.uri();
        let path = match uri.path_and_query() {
            Some(p) => p.path(),
            None => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "Request contained no path.",
                );
            }
        };

        let auth = match request.headers().get(header::AUTHORIZATION) {
            Some(a) => match a.to_str() {
                Ok(s) => s.to_owned(),
                _ => {
                    return error_response(
                        StatusCode::UNAUTHORIZED,
                        "unauthorized",
                        "Request contained an invalid authorization.",
                    );
                }
            },
            None => {
                return error_response(
                    StatusCode::UNAUTHORIZED,
                    "unauthorized",
                    "Request contained no authorization.",
                );
            }
        };

        if path.starts_with("/b2api/v2/b2_authorize_account") {
            self.b2_authorize_account(request, &auth).await
        } else if path.starts_with("/api/b2api/v2/") {
            let method = &path[14..];

            if let Err(r) = self.check_auth(&auth).await {
                return r;
            }

            match method {
                _ => error_response(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "Invalid API method requested.",
                ),
            }
        } else {
            return error_response(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "Invalid path requested.",
            );
        }
    }
}

pub fn build_server(
    _path: PathBuf,
) -> TestResult<(SocketAddr, impl Future<Output = Result<(), ()>>, Sender<()>)> {
    let state = Arc::new(Mutex::new(B2ServerState::new()));

    let (sender, receiver) = channel::<()>();
    let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
    let listener = TcpListener::bind(addr).expect("Failed to bind to server socket.");
    let addr = listener
        .local_addr()
        .expect("Failed to bind to server socket.");

    let server = Server::from_tcp(listener)
        .expect("Failed to attach to tcp stream.")
        .serve(move || {
            let this_state = state.clone();
            service_fn(move |req| {
                Compat::new(Box::pin(B2Server::handle(
                    addr.clone(),
                    this_state.clone(),
                    req,
                )))
            })
        });

    let server_future = server
        .with_graceful_shutdown(receiver.compat())
        .compat()
        .map_err(|e| panic!("{}", e));

    Ok((addr, server_future, sender))
}
