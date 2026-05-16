use crc::{CRC_32_ISO_HDLC, Crc};
use xxhash_rust::xxh3::xxh3_128;
pub const NUM_HASHES: usize = 7;
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

pub fn compute_crc_data_block(data: &[u8]) -> u32 {
    let crc32 = Crc::<u32>::new(&CRC_32_ISO_HDLC);
    let mut digest = crc32.digest();
    digest.update(data);
    digest.finalize()
}

use std::time::{SystemTime, UNIX_EPOCH};

pub fn new_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

pub fn get_hashed_key_positions(key: &[u8], bloom_filter_size: usize) -> [usize; NUM_HASHES] {
    let h_key = xxh3_128(key);
    let h1 = (h_key >> 64) as u64;
    let h2 = h_key as u64;

    let mut arr: [usize; NUM_HASHES] = [0; NUM_HASHES];
    for i in 0..NUM_HASHES {
        arr[i] = h1.wrapping_add(i as u64).wrapping_mul(h2) as usize % bloom_filter_size;
    }

    arr
}
