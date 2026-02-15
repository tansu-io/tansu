#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use tansu_sans_io::Decode;
use tansu_sans_io::primitive::varint::{LongVarInt, VarInt};

fuzz_target!(|data: &[u8]| {
    let mut buf = Bytes::copy_from_slice(data);
    let _ = VarInt::decode(&mut buf);

    let mut buf = Bytes::copy_from_slice(data);
    let _ = LongVarInt::decode(&mut buf);
});
