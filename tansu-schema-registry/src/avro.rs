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

use bytes::{Buf, Bytes};
use tansu_kafka_sans_io::{ErrorCode, record::inflated::Batch};
use tracing::debug;

use crate::{Error, Result, Validator};

pub(crate) struct Schema {
    schema: apache_avro::Schema,
}

impl TryFrom<Bytes> for Schema {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        apache_avro::Schema::parse_reader(&mut bytes.reader())
            .map_err(Into::into)
            .map(|schema| Schema { schema })
    }
}

impl Validator for Schema {
    fn validate(&self, batch: &Batch) -> Result<()> {
        debug!(?batch);

        for record in &batch.records {
            debug!(?record);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
