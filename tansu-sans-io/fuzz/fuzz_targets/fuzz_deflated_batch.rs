#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use tansu_sans_io::record::deflated;

fuzz_target!(|data: &[u8]| {
    let _ = deflated::Batch::try_from(Bytes::copy_from_slice(data));
});
