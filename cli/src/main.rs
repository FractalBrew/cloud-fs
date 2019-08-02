#![feature(async_await)]
use std::path::Path;

use clap::{App, Arg, SubCommand};
use futures::future::{ready, TryFutureExt};
use futures::stream::TryStreamExt;

use file_store::backends::{B2Backend, FileBackend};
use file_store::executor::run;
use file_store::{FileStore, ObjectPath, StorageResult};

async fn ls(filestore: FileStore, path: ObjectPath) -> StorageResult<()> {
    let stream = filestore.list_objects(path).await?;
    stream
        .try_for_each(|object| {
            println!(
                "{:8}{:5} {}",
                object.object_type(),
                object.size(),
                object.path()
            );
            ready(Ok(()))
        })
        .await
}

fn main() {
    let matches = App::new("fs")
        .about("Access storage systems")
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
        .arg(
            Arg::with_name("id")
                .long("id")
                .help("The user identifier (username, etc.) for the storage system.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("secret")
                .long("secret")
                .help("The secret (password, etc.) for the storage system.")
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("ls").about("Lists files.").arg(
                Arg::with_name("path")
                    .help("The path to list.")
                    .takes_value(true),
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
        Some("b2") => {
            let key_id = match matches.value_of("id") {
                Some(k) => k,
                None => {
                    println!(
                        "B2 storage requires you to supply the --id option, it should be the application key identifier.\n\n{}",
                        matches.usage()
                    );
                    return;
                }
            };

            let key = match matches.value_of("secret") {
                Some(k) => k,
                None => {
                    println!(
                        "B2 storage requires you to supply the --secret option, it should be the application key.\n\n{}",
                        matches.usage()
                    );
                    return;
                }
            };

            B2Backend::connect(key_id, key)
        }
        _ => {
            println!("Unknown storage system.\n\n{}", matches.usage());
            return;
        }
    };

    let future = match matches.subcommand() {
        ("ls", Some(params)) => {
            let path = match params.value_of("path") {
                Some(p) => match ObjectPath::new(p) {
                    Ok(pth) => pth,
                    Err(e) => {
                        println!("{}\n\n{}", e, matches.usage());
                        return;
                    }
                },
                None => ObjectPath::new("").unwrap(),
            };

            fsfuture.and_then(|fs| ls(fs, path))
        }
        _ => {
            println!("Unknown command.\n\n{}", matches.usage());
            return;
        }
    };

    match run(future).unwrap() {
        Ok(()) => (),
        Err(e) => println!("{}", e),
    }
}
