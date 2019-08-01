#![cfg(feature = "b2")]

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use futures::channel::oneshot::{channel, Sender};
use futures::compat::*;
use futures::future::{Future, TryFutureExt};
use futures::lock::Mutex;

use hyper::server::Server;
use hyper::service::service_fn;
use hyper::{Body, Request, Response};

use crate::runner::TestResult;

struct B2ServerState {}

impl B2ServerState {
    fn new() -> B2ServerState {
        B2ServerState {}
    }
}

struct B2Server {
    state: Arc<Mutex<B2ServerState>>,
}

impl B2Server {
    async fn handle(
        state: Arc<Mutex<B2ServerState>>,
        request: Request<Body>,
    ) -> io::Result<Response<Body>> {
        let server = B2Server { state };

        server.serve(request).await
    }

    async fn serve(self, request: Request<Body>) -> io::Result<Response<Body>> {
        unimplemented!();
    }
}

pub fn build_server(
    _path: PathBuf,
) -> TestResult<(SocketAddr, impl Future<Output = Result<(), ()>>, Sender<()>)> {
    let state = Arc::new(Mutex::new(B2ServerState::new()));

    let (sender, receiver) = channel::<()>();
    let addr = ([127, 0, 0, 1], 0).into();

    let server = Server::bind(&addr).serve(move || {
        let this_state = state.clone();
        service_fn(move |req| Compat::new(Box::pin(B2Server::handle(this_state.clone(), req))))
    });

    let addr = server.local_addr();
    println!("Listening on {}", addr);

    let server_future = server
        .with_graceful_shutdown(receiver.compat())
        .compat()
        .map_err(|e| panic!("{}", e));

    Ok((addr, server_future, sender))
}
