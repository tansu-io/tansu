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

use core::fmt::Debug;
use std::ops::RangeFrom;

use bytes::Bytes;

use crate::{Index, Result};

pub trait ProvideKvStore {
    fn provide_kv_store(&self) -> Result<Box<dyn KvStore>>;
}

pub trait KvStore: Debug + Send + Sync {
    fn get(&self, key: Bytes) -> Result<Option<Bytes>>;
    fn put(&mut self, key: Bytes, value: Bytes) -> Result<()>;

    fn remove_range_from(&mut self, range: RangeFrom<Bytes>) -> Result<()>;
}

pub fn log_key_for_index(index: Index) -> Result<Bytes> {
    Ok(Bytes::from(format!("raft/logs/{:0>20}", index)))
}

pub fn state_key() -> Bytes {
    Bytes::from("raft/state")
}
