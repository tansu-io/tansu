use crate::Result;

pub trait ByteSize {
    fn size_in_bytes(&self) -> Result<usize>;
}

pub mod tagged;
pub mod uuid;
pub mod varint;
