#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use tansu_sans_io::Frame;

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }
    let api_key = i16::from_be_bytes([data[0], data[1]]);
    let api_version = i16::from_be_bytes([data[2], data[3]]);
    let payload = Bytes::copy_from_slice(&data[4..]);
    let _ = Frame::response_from_bytes(payload, api_key, api_version);
});
