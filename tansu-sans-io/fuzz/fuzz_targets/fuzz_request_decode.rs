#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use tansu_sans_io::Frame;

fuzz_target!(|data: &[u8]| {
    let _ = Frame::request_from_bytes(Bytes::copy_from_slice(data));
});
