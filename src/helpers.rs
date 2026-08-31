use crate::errors::Result;
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
    // TODO: Do not call the new function below for every crc
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

#[cfg(target_os = "macos")]
use std::io::Error;
use std::{
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::errors::{DataCorruptedErr, DbError};

pub fn new_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// PROBLEM: Not cache friendly
// split array into 64 byte blocks, then have one position from the hash function choose the block in the array and the rest to choose
// the positions within that block, that way you dont jump around
pub fn get_hashed_key_positions(key: &[u8], bloom_filter_size: usize) -> [usize; NUM_HASHES] {
    get_positions_from_hashed_key(hash_key(key), bloom_filter_size)
}

pub fn hash_key(key: &[u8]) -> u128 {
    xxh3_128(key)
}
pub fn read_range(b: &[u8], start: usize, end: usize) -> Result<&[u8]> {
    b.get(start..end).ok_or(DbError::OutOfBoundsRead {
        start: start as u64,
        end: end as u64,
        len: b.len() as u64,
    })
}

pub fn get_positions_from_hashed_key(
    hashed_key: u128,
    bloom_filter_size: usize,
) -> [usize; NUM_HASHES] {
    let h1 = (hashed_key >> 64) as u64;
    let h2 = hashed_key as u64;

    let mut arr: [usize; NUM_HASHES] = [0; NUM_HASHES];
    for i in 0..NUM_HASHES {
        arr[i] = (h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize) % bloom_filter_size;
    }

    arr
}

// helper for key and value record check only
pub fn check_key_value_record_does_not_exceed_max(
    size: u64,
    max_size: u64,
    offset: u64,
    file_path: &PathBuf,
) -> Result<()> {
    if size > max_size {
        Err(DbError::DataCorrupted(DataCorruptedErr {
            offset,
            file_path: file_path.to_path_buf(),
            reason: crate::errors::CorruptionType::KeyValueRecordExceedsMaxLength {
                max: max_size,
                found: size,
            },
        }))
    } else {
        Ok(())
    }
}

pub fn create_new_data_file(dir: &Path) -> io::Result<(File, PathBuf, PathBuf)> {
    let tstamp = new_timestamp();
    let data_file_path_final = dir.join(format!("{}.sst", tstamp));
    let data_file_path_tmp = dir.join(format!("{}.sst.tmp", tstamp));
    let data_file = OpenOptions::new()
        .read(true)
        .append(true)
        .create_new(true)
        .open(&data_file_path_tmp)?;
    Ok((data_file, data_file_path_tmp, data_file_path_final))
}
