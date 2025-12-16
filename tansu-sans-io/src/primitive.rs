// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::iter::once;

use bytes::Bytes;

use crate::Result;

pub trait ByteSize {
    fn size_in_bytes(&self) -> Result<usize>;
}

pub trait WithCapacity {
    fn capacity_in_bytes(&self) -> Result<usize>;
}

impl WithCapacity for bool {
    fn capacity_in_bytes(&self) -> Result<usize> {
        Ok(size_of::<i8>())
    }
}

impl<const N: usize> WithCapacity for [u8; N] {
    fn capacity_in_bytes(&self) -> Result<usize> {
        Ok(N)
    }
}

impl WithCapacity for f64 {
    fn capacity_in_bytes(&self) -> Result<usize> {
        Ok(size_of::<f64>())
    }
}

impl WithCapacity for i8 {
    fn capacity_in_bytes(&self) -> Result<usize> {
        Ok(size_of::<i8>())
    }
}

impl WithCapacity for i16 {
    fn capacity_in_bytes(&self) -> Result<usize> {
        Ok(size_of::<i16>())
    }
}

impl WithCapacity for i32 {
    fn capacity_in_bytes(&self) -> Result<usize> {
        Ok(size_of::<i32>())
    }
}

impl WithCapacity for i64 {
    fn capacity_in_bytes(&self) -> Result<usize> {
        Ok(size_of::<i64>())
    }
}

impl WithCapacity for u16 {
    fn capacity_in_bytes(&self) -> Result<usize> {
        Ok(size_of::<u16>())
    }
}

impl WithCapacity for Bytes {
    fn capacity_in_bytes(&self) -> Result<usize> {
        Ok(size_of::<i32>() + self.len())
    }
}

impl WithCapacity for String {
    fn capacity_in_bytes(&self) -> Result<usize> {
        Ok(size_of::<i16>() + self.len())
    }
}

impl<T> WithCapacity for Option<T>
where
    T: WithCapacity,
{
    fn capacity_in_bytes(&self) -> Result<usize> {
        self.as_ref().map_or(Ok(0), WithCapacity::capacity_in_bytes)
    }
}

impl<T> WithCapacity for Vec<T>
where
    T: WithCapacity,
{
    fn capacity_in_bytes(&self) -> Result<usize> {
        self.iter()
            .map(WithCapacity::capacity_in_bytes)
            // length of the vector
            .chain(once(Ok(size_of::<i32>())))
            // empty tag buffer
            .chain(once(Ok(size_of::<i8>())))
            .collect::<Result<Vec<_>>>()
            .map(|items| items.iter().sum())
    }
}

pub mod tagged;
pub mod uuid;
pub mod varint;
