use std::fmt;
use std::io;

use clap::ArgMatches;
use futures::future::{ready, BoxFuture};
use futures::stream::{StreamExt, TryStreamExt};
use tokio::io::{stdin, stdout, AsyncWriteExt, Stdin};

use file_store::utils::ReaderStream;
use file_store::{ConnectFuture, ObjectPath, StorageError, TransferError};

#[derive(Debug)]
pub struct ErrorResult {
    message: String,
}

impl fmt::Display for ErrorResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(&self.message)
    }
}

impl From<StorageError> for ErrorResult {
    fn from(error: StorageError) -> ErrorResult {
        ErrorResult {
            message: error.to_string(),
        }
    }
}

impl From<io::Error> for ErrorResult {
    fn from(error: io::Error) -> ErrorResult {
        ErrorResult {
            message: error.to_string(),
        }
    }
}

impl From<TransferError> for ErrorResult {
    fn from(error: TransferError) -> ErrorResult {
        match error {
            TransferError::SourceError(e) => ErrorResult {
                message: e.to_string(),
            },
            TransferError::TargetError(e) => ErrorResult {
                message: e.to_string(),
            },
        }
    }
}

pub fn ls(
    connect: ConnectFuture,
    args: &ArgMatches<'_>,
) -> BoxFuture<'static, Result<(), ErrorResult>> {
    let prefix_arg = args.value_of("prefix").map(String::from);

    Box::pin(async move {
        let fs = connect.await?;
        let prefix = match prefix_arg {
            Some(p) => ObjectPath::new(p)?,
            None => ObjectPath::empty(),
        };

        let stream = fs.list_objects(prefix).await?;
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
            .await?;
        Ok(())
    })
}

pub fn put(
    connect: ConnectFuture,
    args: &ArgMatches<'_>,
) -> BoxFuture<'static, Result<(), ErrorResult>> {
    let path = args.value_of("PATH").map(String::from).unwrap();

    Box::pin(async move {
        let fs = connect.await?;
        let path = ObjectPath::new(path)?;
        let stream = ReaderStream::<Stdin>::stream(stdin(), 1000000, 500000);
        fs.write_file_from_stream(path, stream).await?;
        Ok(())
    })
}

pub fn cat(
    connect: ConnectFuture,
    args: &ArgMatches<'_>,
) -> BoxFuture<'static, Result<(), ErrorResult>> {
    let path = args.value_of("PATH").map(String::from).unwrap();

    Box::pin(async move {
        let fs = connect.await?;
        let path = ObjectPath::new(path)?;

        let mut stream = fs.get_file_stream(path).await?;
        let mut stdout = stdout();
        loop {
            match stream.next().await {
                Some(Ok(data)) => {
                    stdout.write_all(&data).await?;
                }
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(()),
            }
        }
    })
}

pub fn rm(
    connect: ConnectFuture,
    args: &ArgMatches<'_>,
) -> BoxFuture<'static, Result<(), ErrorResult>> {
    let path = args.value_of("PATH").map(String::from).unwrap();

    Box::pin(async move {
        let fs = connect.await?;
        let path = ObjectPath::new(path)?;

        Ok(fs.delete_object(path).await?)
    })
}
