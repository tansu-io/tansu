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

pub mod offset;
pub mod time;

use crate::{Result, TopitionOffset};
use std::fmt::Debug;

pub trait OffsetProvider: Debug + Send {
    fn provide_offset(&self, tpo: &TopitionOffset) -> Result<Box<dyn Offset>>;
}

impl<T: OffsetProvider + ?Sized> OffsetProvider for Box<T> {
    fn provide_offset(&self, tpo: &TopitionOffset) -> Result<Box<dyn Offset>> {
        (**self).provide_offset(tpo)
    }
}

pub trait Offset: Debug + Send {
    fn append(&mut self, offset: i64, position: u64) -> Result<()>;
    fn position_for_offset(&mut self, offset: i64) -> Result<u64>;
}

impl<T: Offset + ?Sized> Offset for Box<T> {
    fn append(&mut self, offset: i64, position: u64) -> Result<()> {
        (**self).append(offset, position)
    }

    fn position_for_offset(&mut self, offset: i64) -> Result<u64> {
        (**self).position_for_offset(offset)
    }
}

pub trait TimeProvider: Debug + Send {
    fn provide_time(&self, tpo: &TopitionOffset) -> Result<Box<dyn Time>>;
}

impl<T: TimeProvider + ?Sized> TimeProvider for Box<T> {
    fn provide_time(&self, tpo: &TopitionOffset) -> Result<Box<dyn Time>> {
        (**self).provide_time(tpo)
    }
}

pub trait Time: Debug + Send {
    fn append(&mut self, time: i64, offset: i64) -> Result<()>;
    fn offset_for_time(&mut self, time: i64) -> Result<i64>;
}

impl<T: Time + ?Sized> Time for Box<T> {
    fn append(&mut self, time: i64, offset: i64) -> Result<()> {
        (**self).append(time, offset)
    }

    fn offset_for_time(&mut self, time: i64) -> Result<i64> {
        (**self).offset_for_time(time)
    }
}
