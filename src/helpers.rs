use crate::{
    errors::{
        CorruptionType::{self, TruncatedRecord},
        CrcType, Result,
    },
    lsm::Hlc,
};
use crc::{CRC_32_ISO_HDLC, Crc, Digest};
use xxhash_rust::xxh3::xxh3_128;
pub const NUM_HASHES: usize = 7;

pub struct Crc32 {
    crc32: Crc<u32>,
}

impl Crc32 {
    pub const fn new() -> Self {
        Self {
            crc32: Crc::<u32>::new(&CRC_32_ISO_HDLC),
        }
    }

    pub fn compute_crc_data_block(&self, data: &[u8]) -> u32 {
        self.crc32.checksum(data)
    }
    pub fn digest(&self) -> Digest<'_, u32> {
        self.crc32.digest()
    }
}
pub static CRC32: Crc32 = Crc32::new();

// pub fn compute_crc_data_block(data: &[u8]) -> u32 {
//     let crc32 = Crc::<u32>::new(&CRC_32_ISO_HDLC);
//     let mut digest = crc32.digest();
//     digest.update(data);
//     digest.finalize()
// }

#[cfg(target_os = "macos")]
use std::io::Error;
use std::{
    fs::{File, OpenOptions},
    io::{self, ErrorKind::UnexpectedEof, Read},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use crate::errors::{DataCorruptedErr, DbError};

pub fn new_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
    // use 52 of the bits of the value above to be the timestamp
    // then 12 of the bits serve as a logical counter so we get 2^12
    //[timestampbits52...logicalcounterbits12]
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
    // TODO: make sure bloom_filter is never 0, or just throw err
    let h1 = (hashed_key >> 64) as u64;
    let h2 = hashed_key as u64;

    let mut arr: [usize; NUM_HASHES] = [0; NUM_HASHES];
    for i in 0..NUM_HASHES {
        arr[i] = (h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize) % bloom_filter_size;
    }

    arr
}

pub fn read_exact_or_corrupt(
    reader: &mut impl Read,
    buf: &mut [u8],
    offset: u64,
    file_path: &Path,
) -> Result<()> {
    // if we get a truncated record, we have corrupted data
    reader.read_exact(buf).map_err(|e| {
        if e.kind() == UnexpectedEof {
            DbError::DataCorrupted(DataCorruptedErr {
                offset,
                file_path: file_path.to_path_buf(),
                reason: TruncatedRecord,
            })
        } else {
            DbError::Io(e)
        }
    })
}
// helper for key and value record check only

pub fn check_crc(
    crc_to_check: u32,
    crc_in_file: u32,
    offset: u64,
    file_path: &Path,
    crc_type: CrcType,
) -> Result<()> {
    if crc_to_check != crc_in_file {
        Err(DbError::DataCorrupted(DataCorruptedErr {
            offset,
            file_path: file_path.to_path_buf(),
            reason: CorruptionType::CrcMismatch {
                expected: crc_in_file,
                found: crc_to_check,
                mismatch_type: crc_type,
            },
        }))
    } else {
        Ok(())
    }
}

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

pub fn get_hlc_from_valid_pathbuf(path: &Path) -> Result<u64> {
    let stem = path
        .file_stem()
        .and_then(|x| x.to_str())
        .ok_or_else(|| DbError::InvalidSstableFileName(path.to_path_buf()))?;

    stem.parse::<u64>()
        .map_err(|e| DbError::PathFailedToParseToInt(path.to_path_buf(), e))
}

pub fn find_max_hlc_between_files(path_bufs: &[PathBuf]) -> Option<u64> {
    path_bufs
        .iter()
        .filter_map(|x| get_hlc_from_valid_pathbuf(x).ok())
        .max()
}

// dir should be a constant not passed since Im using it for ssts only or make it so it takes a type of file to create
pub fn create_new_data_file(dir: &Path, hlc: u64) -> io::Result<(File, PathBuf)> {
    let data_file_path = dir.join(format!("{}.sst", hlc));

    let data_file = OpenOptions::new()
        .read(true)
        .append(true)
        .create_new(true)
        .open(&data_file_path)?;
    Ok((data_file, data_file_path))
}
