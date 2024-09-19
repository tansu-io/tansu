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
    use futures::stream::BoxStream;
    use futures_core::stream::BoxStream;
    use object_store::parse_url_opts;
    use tracing::debug;
    use url::Url;

    #[tokio::test]
    async fn minio() -> Result<()> {
        let options = [
            ("accessKey", "HAFgdT6j3X3Lek04nvDJ"),
            ("secretKey", "qu5XLUSyS2U6xw12mboUYpOCmIH8zaRReDjxqWby"),
        ];

        let url = Url::parse("http::/localhost:9000/bucket/tansu")?;

        let (store, path) = parse_url_opts(&url, options.into_iter())?;
        debug!(?store, ?path);

        let mut list_stream = store.list(None);

        while let Some(meta) = list_stream.next().await.transpose().unwrap() {
            let location = meta.location;
            let size = meta.size;

            debug!(?location, ?size)
        }

        Ok(())
    }
}
