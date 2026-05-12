use crc::{CRC_32_ISO_HDLC, Crc};

pub fn compute_crc(
    timestamp: &[u8; 8],
    key_size: &[u8; 8],
    value_size: &[u8; 8],
    key: &[u8],
    value: &[u8],
) -> u32 {
    let crc32 = Crc::<u32>::new(&CRC_32_ISO_HDLC);
    let mut digest = crc32.digest();
    digest.update(timestamp);
    digest.update(key_size);
    digest.update(value_size);
    digest.update(key);
    digest.update(value);
    digest.finalize()
}

use std::time::{SystemTime, UNIX_EPOCH};

pub fn new_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
