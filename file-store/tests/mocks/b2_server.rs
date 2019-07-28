#![cfg(feature = "b2")]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::io;

use futures::compat::*;
use futures::future::{Future, TryFutureExt};
use futures::channel::oneshot::{Sender, channel};

use hyper::server::Server;
use hyper::service::service_fn_ok;
use hyper::{Body, Response};

pub fn build_server(
    _path: PathBuf,
) -> io::Result<(SocketAddr, impl Future<Output = Result<(), ()>>, Sender<()>)> {
    let (sender, receiver) = channel::<()>();
    let addr = ([127, 0, 0, 1], 0).into();

    let make_service = || {
        service_fn_ok(|_req| {
            println!("Saw request");
            Response::new(Body::from("Hello World"))
        })
    };

    let server = Server::bind(&addr).serve(make_service);

    let addr = server.local_addr();
    println!("Listening on {}", addr);

    let server_future = server
        .with_graceful_shutdown(receiver.compat())
        .compat()
        .map_err(|e| panic!("{}", e));

    Ok((addr, server_future, sender))
}
