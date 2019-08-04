#![feature(async_await)]
mod commands;

use std::path::Path;

use clap::App;
use yaml_rust::{Yaml, YamlLoader};

use file_store::backends::{B2Backend, FileBackend};
use file_store::executor::run;

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
            (B2Backend::connect(key_id, key), backend_args)
        }
        _ => {
            panic!("Command line parsing failed.");
        }
    };

    let future = match backend_args.subcommand() {
        ("ls", Some(args)) => ls(fsfuture, args),
        _ => {
            panic!("Command line parsing failed.");
        }
    };

    match run(future).unwrap() {
        Ok(()) => (),
        Err(e) => println!("{}", e),
    }
}
