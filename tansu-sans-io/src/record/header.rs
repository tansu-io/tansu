use crate::{Result, primitive::ByteSize, record::codec::Octets};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Header {
    #[serde(serialize_with = "Octets::serialize")]
    #[serde(deserialize_with = "Octets::deserialize")]
    pub key: Option<Bytes>,

    #[serde(serialize_with = "Octets::serialize")]
    #[serde(deserialize_with = "Octets::deserialize")]
    pub value: Option<Bytes>,
}

impl Header {
    pub fn builder() -> Builder {
        Builder::default()
    }
}

impl From<Builder> for Header {
    fn from(value: Builder) -> Self {
        Self {
            key: value.key.0,
            value: value.value.0,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Builder {
    key: Octets,
    value: Octets,
}

impl Builder {
    #[must_use]
    pub fn key(mut self, key: Bytes) -> Self {
        self.key = Octets::from(Some(key));
        self
    }

    #[must_use]
    pub fn value(mut self, value: Bytes) -> Self {
        self.value = Octets::from(Some(value));
        self
    }

    #[must_use]
    pub fn build(self) -> Header {
        Header {
            key: self.key.0,
            value: self.value.0,
        }
    }
}

impl From<Header> for Builder {
    fn from(value: Header) -> Self {
        Self {
            key: value.key.into(),
            value: value.value.into(),
        }
    }
}

impl ByteSize for Builder {
    fn size_in_bytes(&self) -> Result<usize> {
        self.key
            .size_in_bytes()
            .and_then(|ksz| self.value.size_in_bytes().map(|vsz| ksz + vsz))
    }
}
