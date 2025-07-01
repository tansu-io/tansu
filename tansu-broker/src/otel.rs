// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use ::tracing::debug;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{Protocol, WithExportConfig as _};
use opentelemetry_sdk::Resource;
use url::Url;

use crate::{Result, TracingFormat};

mod tracing;

#[derive(Debug)]
pub struct Guard {
    #[allow(dead_code)]
    tracer: tracing::Guard,
}

pub fn init(tracing_format: TracingFormat) -> Result<Guard> {
    tracing::init_tracing_subscriber(tracing_format).map(|tracer| Guard { tracer })
}

pub fn metric_exporter(endpoint: Url) -> Result<()> {
    let endpoint = endpoint
        .join("v1/metrics")
        .inspect(|endpoint| debug!(%endpoint))?;

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(endpoint.to_string())
        .build()?;

    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(
            Resource::builder_empty()
                .with_attributes([KeyValue::new("service.name", env!("CARGO_PKG_NAME"))])
                .build(),
        )
        .build();

    global::set_meter_provider(meter_provider);

    Ok(())
}
