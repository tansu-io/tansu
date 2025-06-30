use std::fmt::{self, Display, Formatter};

use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{ExporterBuildError, Protocol, WithExportConfig as _};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tracing::debug;
use url::{ParseError, Url};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    ExporterBuild(#[from] ExporterBuildError),
    Parse(#[from] ParseError),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn meter_provider(
    otlp_endpoint_url: Url,
    service_name: impl Into<String>,
) -> Result<SdkMeterProvider> {
    otlp_endpoint_url
        .join("v1/metrics")
        .inspect(|endpoint| debug!(%endpoint))
        .map_err(Into::into)
        .and_then(|endpoint| {
            opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(endpoint.to_string())
                .build()
                .map_err(Into::into)
        })
        .map(|exporter| {
            let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
                .with_periodic_exporter(exporter)
                .with_resource(
                    Resource::builder_empty()
                        .with_attributes([KeyValue::new(SERVICE_NAME, service_name.into())])
                        .build(),
                )
                .build();

            global::set_meter_provider(meter_provider.clone());

            meter_provider
        })
}
