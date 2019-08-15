use clap::ArgMatches;
use futures::future::{ready, Future, TryFutureExt};
use futures::stream::TryStreamExt;

use file_store::{ConnectFuture, ObjectPath, StorageResult};

pub fn ls(
    connect: ConnectFuture,
    args: &ArgMatches<'_>,
) -> impl Future<Output = StorageResult<()>> {
    let prefix_arg = args.value_of("prefix").map(String::from);

    connect.and_then(move |fs| {
        async move {
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
                .await
        }
    })
}
