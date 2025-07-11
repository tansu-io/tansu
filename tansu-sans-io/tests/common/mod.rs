#![allow(dead_code)]

use tansu_sans_io::{Error, Result};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

pub(crate) fn init_tracing() -> Result<DefaultGuard> {
    use std::{fs::File, sync::Arc, thread};

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_target(true)
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive("tansu_model=debug".parse()?)
                    .add_directive(
                        format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
                    ),
            )
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!(
                            "../logs/{}/{}::{name}.log",
                            env!("CARGO_PKG_NAME"),
                            env!("CARGO_CRATE_NAME")
                        ))
                        .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}
