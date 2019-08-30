mod commands;

use std::path::Path;

use clap::App;
use tokio::runtime::Runtime;
use yaml_rust::{Yaml, YamlLoader};

use file_store::backends::b2::B2Backend;
use file_store::backends::file::FileBackend;
use file_store::ObjectPath;

use commands::*;

fn build_yaml() -> Yaml {
    let mut yaml = YamlLoader::load_from_str(include_str!("main.yaml"))
        .unwrap()
        .remove(0);
    if let Yaml::Hash(ref mut app_hash) = yaml {
        let mut backends = app_hash
            .remove(&Yaml::String(String::from("backends")))
            .unwrap();
        let commands = app_hash
            .remove(&Yaml::String(String::from("commands")))
            .unwrap();

        if let Yaml::Array(ref mut ba) = backends {
            for item in ba {
                if let Yaml::Hash(ref mut hash) = item {
                    if hash.len() != 1 {
                        panic!("Command line argument parsing failed.");
                    }

                    let cmd = hash.values_mut().nth(0).unwrap();
                    if let Yaml::Hash(ref mut cmd_hash) = cmd {
                        cmd_hash
                            .insert(Yaml::String(String::from("subcommands")), commands.clone());
                    } else {
                        panic!("Command line argument parsing failed.");
                    }
                } else {
                    panic!("Command line argument parsing failed.");
                }
            }
        } else {
            panic!("Command line argument parsing failed.");
        }

        app_hash.insert(Yaml::String(String::from("subcommands")), backends);
    } else {
        panic!("Command line argument parsing failed.");
    }

    yaml
}

fn main() {
    env_logger::init();

    let yaml = build_yaml();
    let app_args = App::from_yaml(&yaml).get_matches();

    let (fsfuture, backend_args) = match app_args.subcommand() {
        ("file", Some(backend_args)) => {
            let root = backend_args.value_of("root").unwrap();
            (FileBackend::connect(Path::new(root)), backend_args)
        }
        ("b2", Some(backend_args)) => {
            let key_id = backend_args.value_of("key-id").unwrap();
            let key = backend_args.value_of("key").unwrap();
            let mut builder = B2Backend::builder(key_id, key);
            if let Some(prefix) = backend_args.value_of("prefix") {
                let path = ObjectPath::new(prefix).unwrap();
                builder = builder.prefix(path);
            }
            (builder.connect(), backend_args)
        }
        _ => {
            println!("You must choose a storage backend.\n{}", app_args.usage());
            return;
        }
    };

    let future = match backend_args.subcommand() {
        ("ls", Some(args)) => ls(fsfuture, args),
        ("put", Some(args)) => put(fsfuture, args),
        _ => {
            println!("You must choose a command.\n{}", app_args.usage());
            return;
        }
    };

    let runtime = Runtime::new().unwrap();
    match runtime.block_on(future) {
        Ok(()) => (),
        Err(e) => println!("{}", e),
    }

    runtime.shutdown_on_idle();
}
