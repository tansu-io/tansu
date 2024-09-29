// Copyright ⓒ 2024 Peter Morgan <peter.james.morgan@gmail.com>
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

#[cfg(test)]
mod tests {

    use crate::Result;
    use futures::StreamExt;
    use object_store::{aws::AmazonS3Builder, path::Path, ObjectStore};
    use tracing::debug;
    use tracing_subscriber::{fmt::format::FmtSpan, prelude::*};

    #[tokio::test]
    async fn minio() -> Result<()> {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_level(true)
                    .with_line_number(true)
                    .with_thread_ids(true)
                    .with_span_events(FmtSpan::ACTIVE),
            )
            .init();

        let store = AmazonS3Builder::new()
            .with_access_key_id("HAFgdT6j3X3Lek04nvDJ")
            .with_secret_access_key("qu5XLUSyS2U6xw12mboUYpOCmIH8zaRReDjxqWby")
            .with_endpoint("http://localhost:9000")
            .with_bucket_name("tansu")
            .with_allow_http(true)
            .build()?;

        debug!(?store);

        let prefix = Path::from("a");

        let mut list_stream = store.list(Some(&prefix));

        while let Some(meta) = list_stream.next().await.transpose().unwrap() {
            let location = meta.location;
            let size = meta.size;

            debug!(?location, ?size)
        }

        Ok(())
    }
}
