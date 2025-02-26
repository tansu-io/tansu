// Copyright ⓒ 2025 Peter Morgan <peter.james.morgan@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use crate::{Error, Result};
use http_body_util::Full;
use hyper::{
    Method, Request, Response,
    body::{Bytes, Incoming},
    header::CONTENT_TYPE,
    service::service_fn,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use opentelemetry::global;
use opentelemetry_prometheus::exporter;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{Encoder, Registry, TextEncoder};
use tokio::net::TcpListener;
use tracing::debug;
use url::Url;

async fn serve_req(request: Request<Incoming>, state: Registry) -> Result<Response<Full<Bytes>>> {
    debug!(method = ?request.method(), path = request.uri().path());

    match (request.method(), request.uri().path()) {
        (&Method::GET, "/metrics") => {
            let mut buffer = vec![];
            let encoder = TextEncoder::new();
            let metric_families = state.gather();

            encoder
                .encode(&metric_families, &mut buffer)
                .map_err(Error::from)
                .and(
                    Response::builder()
                        .status(200)
                        .header(CONTENT_TYPE, encoder.format_type())
                        .body(Full::new(Bytes::from(buffer)))
                        .map_err(Into::into),
                )
        }

        _ => Response::builder()
            .status(404)
            .body(Full::new("Page not found".into()))
            .map_err(Into::into),
    }
}

pub async fn init(listener: Url) -> Result<()> {
    debug!(%listener);

    let registry = Registry::new();

    let exporter = exporter().with_registry(registry.clone()).build()?;

    let provider = SdkMeterProvider::builder().with_reader(exporter).build();

    global::set_meter_provider(provider);

    let listener = TcpListener::bind(format!(
        "{}:{}",
        listener.host_str().unwrap_or("0.0.0.0"),
        listener.port().unwrap_or(3000)
    ))
    .await?;

    while let Ok((stream, _addr)) = listener.accept().await {
        if let Err(err) = Builder::new(TokioExecutor::new())
            .serve_connection(
                TokioIo::new(stream),
                service_fn(|req| serve_req(req, registry.clone())),
            )
            .await
        {
            eprintln!("{err}");
        }
    }

    Ok(())
}
