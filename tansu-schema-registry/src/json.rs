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

use crate::{Error, Result};
use bytes::Bytes;
use tansu_kafka_sans_io::{record::inflated::Batch, ErrorCode};
use tracing::{debug, error};

pub(crate) struct Schema {
    key: Option<jsonschema::Validator>,
    value: Option<jsonschema::Validator>,
}

fn validator_for(schema: Option<Bytes>) -> Result<Option<jsonschema::Validator>> {
    debug!(?schema);

    schema
        .map(|encoded| {
            serde_json::from_reader(&encoded[..])
                .map_err(Error::from)
                .inspect(|schema| debug!(?schema))
                .and_then(|schema| jsonschema::validator_for(&schema).map_err(Error::from))
        })
        .transpose()
}

fn validate(validator: Option<&jsonschema::Validator>, encoded: Option<Bytes>) -> Result<()> {
    debug!(validator = ?validator, ?encoded);

    validator
        .map_or(Ok(()), |validator| {
            encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
                serde_json::from_reader(&encoded[..])
                    .map_err(Error::from)
                    .inspect(|instance| debug!(?instance))
                    .and_then(|instance| {
                        validator
                            .validate(&instance)
                            .inspect_err(|err| error!(?err))
                            .map_err(|_err| Error::Api(ErrorCode::InvalidRecord))
                    })
            })
        })
        .inspect(|r| debug!(?r))
        .inspect_err(|err| error!(?err))
}

impl TryFrom<super::Schema> for Schema {
    type Error = Error;

    fn try_from(schema: super::Schema) -> Result<Self, Self::Error> {
        debug!(?schema);

        validator_for(schema.key.clone())
            .and_then(|key| validator_for(schema.value.clone()).map(|value| Self { key, value }))
    }
}

impl super::Validator for Schema {
    fn validate(&self, batch: &Batch) -> Result<()> {
        debug!(?batch);

        for record in &batch.records {
            debug!(?record);

            validate(self.key.as_ref(), record.key.clone())
                .and(validate(self.value.as_ref(), record.value.clone()))?
        }

        Ok(())
    }
}
