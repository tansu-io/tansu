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

use tansu_kafka_sans_io::{Body, ErrorCode};

pub(crate) struct InitProducerIdRequest;

impl InitProducerIdRequest {
    pub(crate) fn response(
        &self,
        transactional_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Body {
        let _ = transactional_id;
        let _ = transaction_timeout_ms;
        let _ = producer_id;
        let _ = producer_epoch;

        Body::InitProducerIdResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None.into(),
            producer_id: 1,
            producer_epoch: 0,
        }
    }
}
