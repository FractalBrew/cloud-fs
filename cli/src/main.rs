#![feature(async_await)]
use std::io;
use std::path::Path;

use clap::{App, Arg, SubCommand};
use futures::future::{ready, TryFutureExt};
use futures::stream::TryStreamExt;

use file_store::backends::FileBackend;
use file_store::executor::run;
use file_store::{FileStore, StoragePath};

async fn ls(filestore: FileStore, path: StoragePath) -> io::Result<()> {
    let stream = filestore.list_objects(path).await?;
    stream
        .try_for_each(|object| {
            println!(
                "{:8}{:70}{}",
                object.object_type(),
                object.path(),
                object.size()
            );
            ready(Ok(()))
        })
        .await
}

fn main() {
    let matches = App::new("fs")
        .about("Access file storage systems")
        .arg(
            Arg::with_name("storage")
                .display_order(0)
                .long("storage")
                .help("Selects the storage system to use.")
                .takes_value(true)
                .required(true)
                .possible_values(&["file", "b2"]),
        )
        .arg(
            Arg::with_name("root")
                .long("root")
                .help("The root path for the storage system.")
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("ls").about("Lists files.").arg(
                Arg::with_name("path")
                    .help("The path to list.")
                    .takes_value(true)
                    .required(true),
            ),
        )
        .get_matches();

    let fsfuture = match matches.value_of("storage") {
        Some("file") => {
            let root = match matches.value_of("root") {
                Some(l) => l,
                None => {
                    println!(
                        "File storage requires you to supply the --root option.\n\n{}",
                        matches.usage()
                    );
                    return;
                }
            };

            FileBackend::connect(Path::new(root))
        }
        _ => {
            println!("Unknown storage system.\n\n{}", matches.usage());
            return;
        }
    };

    let future = match matches.subcommand() {
        ("ls", Some(params)) => {
            let path = match params.value_of("path") {
                Some(p) => match StoragePath::new(p) {
                    Ok(pth) => pth,
                    Err(e) => {
                        println!("{}\n\n{}", e, matches.usage());
                        return;
                    }
                },
                None => {
                    println!("No local path specified.\n\n{}", matches.usage());
                    return;
                }
            };

            fsfuture.and_then(|fs| ls(fs, path))
        }
        _ => {
            println!("Unknown command.\n\n{}", matches.usage());
            return;
        }
    };

    run(future).unwrap().unwrap();
}
