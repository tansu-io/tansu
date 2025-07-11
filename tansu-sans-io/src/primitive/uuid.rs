use serde::{Deserialize, Serialize};

use super::ByteSize;
use crate::Result;

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct Uuid(pub [u8; 16]);

impl From<[u8; 16]> for Uuid {
    fn from(value: [u8; 16]) -> Self {
        Self(value)
    }
}

impl ByteSize for Uuid {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(size_of_val(&self.0))
    }
}
