use crc::{CRC_32_ISO_HDLC, Crc};
use std::ops::Deref;
use std::os::unix::fs::FileExt;

use core::num;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions, remove_file};
use std::io::{self, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};

use std::path::{Path, PathBuf};
use std::ptr::null;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};
use std::sync::{Weak, mpsc};
use std::thread::spawn;
use std::{todo, unimplemented, unreachable};

use crate::errors::{
    CorruptionType, CrcType, DataCorruptedErr, DbError, InvalidMemtableInput, Result,
};
use crate::helpers::{
    NUM_HASHES, check_crc, compute_crc_data_block, create_new_data_file,
    find_max_hlc_between_files, get_hashed_key_positions, new_timestamp, read_exact_or_corrupt,
    read_range,
};
use crate::lsm::Lookup::{Absent, Deleted, Found};

use std::cmp::{Ordering as CmpOrdering, Reverse, max};

// const MAX_FILE_SIZE: u64 = 4 * 1024 * 1024; // SUBJECT TO CHANGE
const MEMTABLE_THRESHOLD: u64 = 8 * 1024 * 1024; // SUBJECT TO CHANGE
// this means L0 sstables are 8 mb, so we usually compact all L0 sstablse with all L1 sstables to a single L1 sstable.
const DATA_BLOCK: u16 = 8 * 1024; // Data block in SSTable
pub const DATA_BLOCK_MAX_BYTES_SIZE: u64 = 155673; // 8192(max db_size) + KEY_MAX_BYTES_SIZE + VALUE_MAX_BYTES_SIZE + 25 bytes for metadata(timestamp, ksz,vsz,tmbstone); // if we had a db_size of 8191, we could end up with adding a max val and max key
// const MAX_BLOCK_SIZE: u64 = 1024 * 1024;
pub const MAX_SST_SIZE: u64 = 1024 * 1024 * 160;
const TAG_DELETION: u8 = 2;
const TAG_INSERTION: u8 = 4;
pub const SST_LEVEL_COUNT: usize = 4;
pub const KEY_MAX_BYTES_SIZE: u64 = 16384;
pub const VALUE_MAX_BYTES_SIZE: u64 = 131072;
pub const NUM_OF_BITS_FOR_TSTAMP: u8 = 52;
pub const NUM_OF_BITS_FOR_COUNTER: u8 = 12;
pub const MASK_FOR_COUNTER: u64 = (u64::MAX) >> NUM_OF_BITS_FOR_TSTAMP; // 4096
pub const MASK_FOR_TSTAMP: u64 = (u64::MAX) << NUM_OF_BITS_FOR_COUNTER;
pub const NUM_OF_L0_FILES_TO_TRIGGER_COMPACTION: usize = 10;
pub const NUM_OF_BYTES_NEEDED_TO_TRIGGER_L1_COMPACTION: u64 = MAX_SST_SIZE * 10;
pub const NUM_OF_BYTES_NEEDED_TO_TRIGGER_L2_COMPACTION: u64 = MAX_SST_SIZE * 100;
pub const NUM_OF_BYTES_NEEDED_TO_TRIGGER_L3_COMPACTION: u64 = MAX_SST_SIZE * 1000;
pub const NUM_OF_BYTES_NEEDED_TO_TRIGGER_L4_COMPACTION: u64 = MAX_SST_SIZE * 10000;

// WAL config for flush

#[derive(Copy, Clone)]
enum SyncConfig {
    None,       // fast, data can be lost
    Every(u64), // in ms
    Always,     // Ddurable
}

enum Lookup {
    Found(Vec<u8>),
    Deleted,
    Absent,
}

pub struct BloomFilter {
    pub bits: Vec<u64>,
    pub num_bits: u64,
}

enum WalRecordType<'a> {
    Deletion(&'a [u8]),            // ( key )
    Insertion(&'a [u8], &'a [u8]), // (key, value)
}

pub struct SparseIndex {
    pub index_entries: Vec<u8>,
    pub size: u64,
}

impl SparseIndex {
    pub fn new() -> Self {
        Self {
            index_entries: Vec::new(),
            size: 0,
        }
    }
    pub fn add_entry(&mut self, starting_key: &[u8], data_len: u64, offset: u64) {
        // data_len = block length WITHOUT 4 Byte CRC
        let first_keysz = (starting_key.len() as u64).to_le_bytes();
        let data_block_sz = data_len.to_le_bytes();

        // sparse index: sizeof(k), k, offset, datablock_size);
        self.index_entries.extend_from_slice(&first_keysz);
        self.index_entries.extend_from_slice(starting_key);
        self.index_entries.extend_from_slice(&offset.to_le_bytes());
        self.index_entries.extend_from_slice(&data_block_sz);
        self.size += 1;
    }

    fn parse_sparse_index(b: &[u8], path: PathBuf) -> Result<Vec<(Vec<u8>, u64, u64)>> {
        let mut out = Vec::new();
        // I am parsing this layout: ksz(8) | key(of size: ksz) | offset(8) | datablock_sz(8)
        // to essentially => key | offset | datablock_sz (this lives in memory, the sparseIndex needs key to binary search. meanwhile the sparseIndex in the metadatafooter does need the key size)
        // TODO:
        // Have caller
        let mut current = 0;
        while current < b.len() {
            // let ksz = u64::from_le_bytes(b[current..(current + 8)].try_into().unwrap());
            // put in a function and reuse
            let ksz = u64::from_le_bytes(read_range(b, current, current + 8)?.try_into().unwrap());
            if ksz > KEY_MAX_BYTES_SIZE {
                return Err(DbError::DataCorrupted(DataCorruptedErr {
                    offset: current as u64,
                    file_path: path,
                    reason: CorruptionType::KeyValueRecordExceedsMaxLength {
                        max: KEY_MAX_BYTES_SIZE,
                        found: ksz,
                    },
                }));
            }
            current += 8;
            let key = read_range(b, current, current + (ksz as usize))?.to_vec();

            current += ksz as usize;

            let offset =
                u64::from_le_bytes(read_range(b, current, current + 8)?.try_into().unwrap());

            current += 8;

            let data_block_size =
                u64::from_le_bytes(read_range(b, current, current + 8)?.try_into().unwrap());
            if data_block_size > DATA_BLOCK_MAX_BYTES_SIZE {
                return Err(DbError::DataCorrupted(DataCorruptedErr {
                    offset: current as u64,
                    file_path: path,
                    reason: CorruptionType::BufferExceedsMaxLength {
                        size: data_block_size,
                        max_size: DATA_BLOCK_MAX_BYTES_SIZE,
                    },
                }));
            }
            current += 8;
            out.push((key, offset, data_block_size));
        }
        Ok(out)
    }
}

impl BloomFilter {
    pub fn new(num_bits: usize) -> Self {
        let words_for_bits = num_bits.div_ceil(64);

        Self {
            bits: vec![0u64; words_for_bits],
            num_bits: (words_for_bits * 64) as u64,
        }
    }

    pub fn set_bits(&mut self, positons: [usize; NUM_HASHES]) {
        for position in positons {
            let word_idx = position / 64;
            let bit_idx = position % 64;

            self.bits[word_idx] |= 1u64 << bit_idx; // shift the bit to the left by bit_idx positions and thats our mask. mask OR curr_u64 = done
        }
    }

    pub fn check_bits(&self, positons: [usize; NUM_HASHES]) -> bool {
        for position in positons {
            let word_idx = position / 64;
            let bit_idx = position % 64;

            if ((self.bits[word_idx] >> bit_idx) & 1u64) == 0 {
                return false;
            }
        }

        true
    }
}

struct WAL {
    wal_writer: Option<BufWriter<File>>,
    sync_c: SyncConfig,
    record_buffer: Vec<u8>,
    threshold: u64,
    path: PathBuf,
}

impl WAL {
    fn new(
        threshold: u64,
        sync_c: SyncConfig,
        parent_dir: &Path,
        curr_hlc: u64,
    ) -> io::Result<WAL> {
        let wal_path = parent_dir.join(format!("{}.wal", curr_hlc));

        let wal_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&wal_path)?;
        Ok(Self {
            wal_writer: Some(BufWriter::new(wal_file)),
            threshold,
            record_buffer: Vec::new(),
            sync_c,
            path: wal_path,
        })
    }
    fn destruct(mut self) -> Result<()> {
        self.wal_writer = None;
        let _ = remove_file(&self.path);
        Ok(())
    }

    // PROBLEM: Right now we sync_all for every single record, make sure you use SyncConfig later on for deciding
    fn record_to_wal<'a>(&mut self, record: WalRecordType<'a>, timestamp: u64) -> Result<()> {
        let record_buffer = &mut self.record_buffer;
        record_buffer.clear();

        match record {
            WalRecordType::Deletion(k) => {
                record_buffer.extend_from_slice(&TAG_DELETION.to_le_bytes());
                record_buffer.extend_from_slice(&timestamp.to_le_bytes());
                record_buffer.extend_from_slice(&(k.len() as u64).to_le_bytes());
                record_buffer.extend_from_slice(k);
            }
            WalRecordType::Insertion(k, v) => {
                record_buffer.extend_from_slice(&TAG_INSERTION.to_le_bytes());
                record_buffer.extend_from_slice(&timestamp.to_le_bytes());
                record_buffer.extend_from_slice(&(k.len() as u64).to_le_bytes());
                record_buffer.extend_from_slice(&(v.len() as u64).to_le_bytes());
                record_buffer.extend_from_slice(k);
                record_buffer.extend_from_slice(v);
            }
        }

        let crc = compute_crc_data_block(record_buffer);
        record_buffer.extend_from_slice(&crc.to_le_bytes());

        match self.wal_writer.as_mut() {
            Some(writer) => {
                writer.write_all(record_buffer)?;
                writer.flush()?;
                writer.get_ref().sync_all()?;
                Ok(())
            }
            None => Err(DbError::WalNotFound),
        }
    }
}

pub struct SsTableDataBlock {
    pub bytes: Cursor<Vec<u8>>, //[ tstamp(8) | ksz(8) | value_sz(8) | tombstone | key | value |  ] ... crc(4) (crc for the entire datablock);
    pub size: usize,
    pub starting_key: Vec<u8>,
}

impl SsTableDataBlock {
    pub fn new(s_key: &[u8]) -> Self {
        // creates SsTableDataBlock

        Self {
            bytes: Cursor::new(Vec::new()),
            size: 0,
            starting_key: s_key.to_vec(),
        }
    }
    pub fn append_to_block(&mut self, entry: &[u8]) {
        self.bytes.get_mut().extend_from_slice(entry);
        // self.bytes.extend_from_slice(entry);
        self.size += entry.len();
    }

    pub fn is_finished(&self) -> bool {
        self.size > DATA_BLOCK as usize
    }

    pub fn full_data_block(mut self) -> Self {
        let crc = compute_crc_data_block(self.bytes.get_ref());
        self.bytes.get_mut().extend_from_slice(&crc.to_le_bytes());
        self
    }

    pub fn grab_min_key_from_data_block(&mut self) -> Result<Vec<u8>> {
        let mut tstamp = [0u8; 8];
        let mut ksz = [0u8; 8];
        let mut vsz = [0u8; 8];
        let mut tombstone: [u8; 1] = [0u8; 1];

        self.bytes.read_exact(&mut tstamp)?;
        self.bytes.read_exact(&mut ksz)?;
        self.bytes.read_exact(&mut vsz)?;
        self.bytes.read_exact(&mut tombstone)?;
        let k_size = u64::from_le_bytes(ksz);
        let v_size = u64::from_le_bytes(vsz);
        let mut key = vec![0u8; k_size as usize];
        let mut value = vec![0u8; v_size as usize];
        self.bytes.read_exact(&mut key)?;
        self.bytes.read_exact(&mut value)?;
        self.bytes.set_position(0);
        Ok(key.to_vec())
    }
    pub fn grab_max_key_from_data_block(&mut self) -> Result<Vec<u8>> {
        let mut tstamp = [0u8; 8];
        let mut ksz = [0u8; 8];
        let mut vsz = [0u8; 8];
        let mut tombstone: [u8; 1] = [0u8; 1];
        let mut curr_max: Vec<u8> = vec![]; // function only gets called when there is a data block so it should never return this

        while self.bytes.read_exact(&mut tstamp).is_ok() {
            // when it throws eof, we have reached the end
            self.bytes.read_exact(&mut ksz)?;
            self.bytes.read_exact(&mut vsz)?;
            self.bytes.read_exact(&mut tombstone)?;
            let k_size = u64::from_le_bytes(ksz);
            let v_size = u64::from_le_bytes(vsz);
            let mut key = vec![0u8; k_size as usize];
            let mut value = vec![0u8; v_size as usize];
            self.bytes.read_exact(&mut key)?;
            self.bytes.read_exact(&mut value)?;
            curr_max = key.to_vec();
        }

        self.bytes.set_position(0);
        Ok(curr_max)
    }
}

// put the cold data into a SStable cold data vector(sparse index, etc)* //
pub struct SSTable {
    id: u64,
    file: File,
    file_path: PathBuf,
    file_size: u64,
    min_max_keys: Option<(Vec<u8>, Vec<u8>)>, // min_key is index 0, max_key is index 1
    sparse_index: Arc<Vec<(Vec<u8>, u64, u64)>>, // key | offset | datablock block length ( before CRC, which means you need to read the next 4 bytes and compute the crc)
    bloom_filter: Option<BloomFilter>,
    corrupted: bool,
    level: u8,
    currently_picked_for_compaction: bool,
}

impl SSTable {
    pub fn load(path: &Path) -> Result<Self> {
        let mut f = File::open(path)?;
        let stem = path
            .file_stem()
            .and_then(|x| x.to_str())
            .ok_or_else(|| DbError::NonNumericFileIdOnSstable(path.to_path_buf()))?; // skip if this happens

        let id = stem
            .parse::<u64>()
            .ok()
            .ok_or_else(|| DbError::InvalidSstableFileName(path.to_path_buf()))?; // Have the caller skip file if this happens

        f.seek(SeekFrom::End(-57))?;
        let mut footer = [0u8; 41];
        f.read_exact(&mut footer)?;

        let mut sparse_index_crc = [0u8; 4];
        let mut bloom_filter_crc = [0u8; 4];
        let mut metadata_crc = [0u8; 4];
        let mut min_max_crc = [0u8; 4];

        f.read_exact(&mut sparse_index_crc)?;
        f.read_exact(&mut bloom_filter_crc)?;
        f.read_exact(&mut min_max_crc)?;
        f.read_exact(&mut metadata_crc)?;
        let min_max_crc_in_file = u32::from_le_bytes(min_max_crc);
        let metadata_crc_in_file = u32::from_le_bytes(metadata_crc);
        let sparse_index_crc_in_file = u32::from_le_bytes(sparse_index_crc);
        let bloom_filter_crc_in_file = u32::from_le_bytes(bloom_filter_crc);

        let footer_metadata_crc_check = compute_crc_data_block(&footer);

        check_crc(
            footer_metadata_crc_check,
            metadata_crc_in_file,
            f.stream_position()?,
            path,
            CrcType::SstFooterMetadata,
        )?;

        let level = u8::from_le_bytes(read_range(&footer, 40, 41)?.try_into().unwrap());
        let file_length = f.metadata()?.len();

        let sparse_index_offset =
            u64::from_le_bytes(read_range(&footer, 0, 8)?.try_into().unwrap());
        let size_of_sparse_index =
            u64::from_le_bytes(read_range(&footer, 8, 16)?.try_into().unwrap());
        let size_of_bloom_filter =
            u64::from_le_bytes(read_range(&footer, 16, 24)?.try_into().unwrap()); // byte count of vector
        let size_of_min_key = u64::from_le_bytes(read_range(&footer, 24, 32)?.try_into().unwrap());
        let size_of_max_key = u64::from_le_bytes(read_range(&footer, 32, 40)?.try_into().unwrap());

        let full_data_length = size_of_sparse_index
            .checked_add(size_of_bloom_filter)
            .and_then(|x| x.checked_add(size_of_min_key))
            .and_then(|x| x.checked_add(size_of_max_key))
            .ok_or({
                DbError::DataCorrupted(DataCorruptedErr {
                    offset: sparse_index_offset,
                    file_path: path.to_path_buf(),
                    reason: CorruptionType::MetadataSizeOverflow {
                        sizes: [
                            size_of_sparse_index,
                            size_of_bloom_filter,
                            size_of_min_key,
                            size_of_max_key,
                        ],
                    },
                })
            })?;

        // check_key_value_record_does_not_exceed_max(size, max_size, offset, file_path)
        // TODO: have the helper function above work with different kinds of data_corruption // not just k/v record check
        if full_data_length > file_length {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: sparse_index_offset,
                file_path: path.to_path_buf(),
                reason: CorruptionType::MetaDataSizeExceedsFileSize {
                    file_size: file_length,
                    metadata_size: full_data_length,
                },
            }));
        }
        let full_data_length = full_data_length as usize;

        f.seek(SeekFrom::Start(sparse_index_offset))?;
        let mut full_sst_data = vec![0u8; full_data_length];
        f.read_exact(&mut full_sst_data)?;
        let bloom_filter_start = size_of_sparse_index;
        let bloom_filter_end = bloom_filter_start + size_of_bloom_filter;
        let min_k_start = bloom_filter_end;
        let min_k_end = min_k_start + size_of_min_key;
        let max_k_start = min_k_end;
        let max_k_end = max_k_start + size_of_max_key;

        // let sparse_index: &[u8] = &full_sst_data[0..(size_of_sparse_index as usize)];
        let sparse_index: &[u8] = read_range(&full_sst_data, 0, size_of_sparse_index as usize)?;
        let sparse_index_crc_check = compute_crc_data_block(sparse_index);

        check_crc(
            sparse_index_crc_check,
            sparse_index_crc_in_file,
            sparse_index_offset,
            path,
            CrcType::SparseIndex,
        )?;

        let bloom_filter: &[u8] = read_range(
            &full_sst_data,
            bloom_filter_start as usize,
            bloom_filter_end as usize as usize,
        )?;

        let bloom_filter_crc_check = compute_crc_data_block(bloom_filter);

        // &full_sst_data[(bloom_filter_start as usize)..(bloom_filter_end as usize)];
        let min_key = read_range(
            &full_sst_data,
            min_k_start as usize,
            min_k_end as usize as usize,
        )?;
        // let min_key = &full_sst_data[(min_k_start as usize)..(min_k_end as usize)];
        // let max_k = &full_sst_data[(max_k_start as usize)..(max_k_end as usize)];
        let max_k = read_range(&full_sst_data, max_k_start as usize, max_k_end as usize)?;

        let min_max_key_crc_to_check = compute_crc_data_block(read_range(
            &full_sst_data,
            min_k_start as usize,
            max_k_end as usize,
        )?);

        let bloomf_filter_64: Vec<u64> = bloom_filter
            .chunks_exact(8)
            .map(|chunk| {
                u64::from_le_bytes(
                    chunk
                        .try_into()
                        .expect("bloom_filter not divided in 64 bit chunks, data corrupted"),
                )
            })
            .collect();

        let num_bits = (bloomf_filter_64.len() * 64) as u64;

        // IF THE BLOOM_FILTER BITS ARE CORRUPTED, WE JUST DON'T USE IT. NO ERR
        let bloom_filter = if bloom_filter_crc_check == bloom_filter_crc_in_file {
            Some(BloomFilter {
                bits: bloomf_filter_64,
                num_bits,
            })
        } else {
            None
        };

        let min_max = if min_max_key_crc_to_check == min_max_crc_in_file {
            Some((min_key.to_vec(), max_k.to_vec()))
        } else {
            None
        };

        let parsed_sparse_index =
            SparseIndex::parse_sparse_index(sparse_index, path.to_path_buf())?; // catch err from caller
        Ok(SSTable {
            id,
            file: f,
            file_path: path.to_path_buf(),
            file_size: file_length,
            min_max_keys: min_max,
            sparse_index: Arc::new(parsed_sparse_index),
            bloom_filter,
            corrupted: false,
            level,
            currently_picked_for_compaction: false,
        })
    }

    fn binary_search_sparse_index(&self, key: &[u8]) -> Option<(u64, u64)> {
        // first u64 is the offset, the second is the datablock size
        if self.sparse_index.is_empty() {
            return None;
        }

        let mut lo: i64 = 0;
        let mut hi: i64 = (self.sparse_index.len() - 1) as i64;

        let mut best_candidate: Option<(u64, u64)> = None;
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            match self.sparse_index.get(mid as usize) {
                Some(entry) => {
                    let key_in_index = entry.0.as_slice();
                    if key_in_index < key {
                        best_candidate = Some((entry.1, entry.2));
                        lo = mid + 1;
                    } else if key_in_index > key {
                        hi = mid - 1;
                    } else {
                        return Some((entry.1, entry.2));
                    }
                }
                None => unreachable!(),
            }
        }

        best_candidate
    }
}
pub struct AVL {
    root: Option<Box<Node>>,
    threshold: u64,
    size: u64,
    size_in_bytes: u64,
}
#[derive(PartialEq, Clone, Debug)]
struct AvlEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    deleted: bool,
    timestamp: u64,
}
#[derive(PartialEq, Clone, Debug)]
struct Node {
    // (Done): Node should actually carry timestamp, exactly at the time a Node is created
    // Right now we get the timestamp when we serialize the kv which is basically called for everynode as we are flushing
    entry: AvlEntry,
    height: u64,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
}

impl Node {
    fn serialize_kv(&self) -> Vec<u8> {
        // return [ tstamp(8) | ksz(8) | value_sz(8) | tombstone | key | value |  ]
        let tstamp = self.entry.timestamp.to_le_bytes();
        let ksz = (self.entry.key.len() as u64).to_le_bytes();
        let vsz = (self.entry.value.len() as u64).to_le_bytes();
        let tombstone_in_byte: [u8; 1] = [if self.entry.deleted { 0xFF } else { 0x00 }];

        [
            &tstamp,
            &ksz,
            &vsz,
            tombstone_in_byte.as_slice(),
            self.entry.key.as_slice(),
            &self.entry.value,
        ]
        .concat()
    }
}

impl AVL {
    fn new(threshold: u64) -> Self {
        Self {
            root: None,
            threshold,
            size: 0,
            size_in_bytes: 0,
        }
    }

    fn get(&self, key: &[u8]) -> Lookup {
        let mut current = self.root.as_ref();
        while let Some(curr) = current {
            if curr.entry.key == key {
                if !curr.entry.deleted {
                    return Found(curr.entry.value.to_vec());
                } else {
                    return Deleted;
                }
            }
            if curr.entry.key.as_slice() > key {
                current = curr.left.as_ref();
            } else {
                current = curr.right.as_ref();
            }
        }
        Absent
    }

    fn update_height(node: &mut Box<Node>) {
        let left_height = if let Some(x) = node.left.as_ref() {
            x.height as i64
        } else {
            -1
        };

        let right_height = if let Some(x) = node.right.as_ref() {
            x.height as i64
        } else {
            -1
        };
        node.height = (1 + max(left_height, right_height)) as u64;
    }
    fn insert(&mut self, curr: Option<Box<Node>>, n: Node) -> Option<Box<Node>> {
        if let Some(mut node) = curr {
            if n.entry.key == node.entry.key {
                let old_len = node.entry.value.len() as u64;
                node.entry.value = n.entry.value;
                // We do this here because we when we delete something, we dont delete the node, we just replace the value with an empty vector
                // and we mark it as deleted so when it gets flushed to memory, the deleted flag maps to a tombstone
                node.entry.deleted = n.entry.deleted;
                node.entry.timestamp = n.entry.timestamp; // most recent of deletion
                self.size_in_bytes = self.size_in_bytes - old_len + node.entry.value.len() as u64;

                return Some(node);
            }
            if n.entry.key < node.entry.key {
                node.left = self.insert(node.left.take(), n);
            } else {
                node.right = self.insert(node.right.take(), n);
            }

            node = Self::balance(node);
            Some(node)
        } else {
            self.size_in_bytes += n.entry.value.len() as u64 + n.entry.key.len() as u64 + 25; // 25 account for record metadata// TODO: find all usge of numbers and make it a const
            self.size += 1;
            Some(Box::new(n))
        }
    }
    /*

    */
    fn exceeds_max(
        &self,
        key_size: u64,
        value_size: u64,
    ) -> std::result::Result<(), InvalidMemtableInput> {
        if key_size > KEY_MAX_BYTES_SIZE {
            return Err(InvalidMemtableInput::KeySizeTooLarge {
                max: KEY_MAX_BYTES_SIZE,
                found: key_size,
            });
        }
        if value_size > VALUE_MAX_BYTES_SIZE {
            return Err(InvalidMemtableInput::ValueSizeTooLarge {
                max: VALUE_MAX_BYTES_SIZE,
                found: value_size,
            });
        }
        Ok(())
    }
    fn put(&mut self, key: &[u8], value: &[u8], timestamp: u64) {
        let n = Node {
            entry: AvlEntry {
                key: key.to_vec(),
                value: value.to_vec(),
                deleted: false,
                timestamp,
            },
            height: 0,
            left: None,
            right: None,
        };
        let root = self.root.take();
        self.root = self.insert(root, n);
    }

    fn balance(mut node: Box<Node>) -> Box<Node> {
        Self::update_height(&mut node);
        let bf = Self::compute_balance_factor_of_node(&node);

        if bf > 1 {
            // left heavy

            let left_node = node.left.as_mut().unwrap();
            match Self::compute_balance_factor_of_node(left_node) {
                bf if bf >= 0 => {
                    let left = node.left.take().unwrap();

                    node = Self::right_rotation(node, left);
                }
                _ => {
                    let mut left_child = node.left.take().unwrap();
                    let right_of_left = left_child.right.take().unwrap();
                    left_child = Self::left_rotation(left_child, right_of_left);
                    node = Self::right_rotation(node, left_child)
                }
            }
        } else if bf < -1 {
            // right heavy

            let right_node = node.right.as_mut().unwrap();
            match Self::compute_balance_factor_of_node(right_node) {
                bf if bf <= 0 => {
                    let right = node.right.take().unwrap();
                    node = Self::left_rotation(node, right);
                }
                _ => {
                    let mut right_child = node.right.take().unwrap();
                    let left_of_right = right_child.left.take().unwrap();
                    right_child = Self::right_rotation(right_child, left_of_right);
                    node = Self::left_rotation(node, right_child);
                }
            }
        }

        node
    }

    fn left_rotation(mut parent: Box<Node>, mut child: Box<Node>) -> Box<Node> {
        // parent and child.right
        parent.right = child.left.take();

        child.left = Some(parent);

        if let Some(left) = child.left.as_mut() {
            Self::update_height(left);
        }
        Self::update_height(&mut child);
        child
    }
    fn right_rotation(mut parent: Box<Node>, mut child: Box<Node>) -> Box<Node> {
        // parent and child.left
        parent.left = child.right.take();
        child.right = Some(parent);
        if let Some(right) = child.right.as_mut() {
            Self::update_height(right);
        }
        Self::update_height(&mut child);
        child
    }

    fn compute_balance_factor_of_node(node: &Node) -> i32 {
        let bf_l = if let Some(x) = node.left.as_ref() {
            x.height as i32
        } else {
            -1
        };
        let bf_r = if let Some(x) = node.right.as_ref() {
            x.height as i32
        } else {
            -1
        };
        bf_l - bf_r
    }
    fn take_min(mut curr: Box<Node>) -> (Option<Box<Node>>, Option<Box<Node>>) {
        // in order successor.
        // we have passed the right child here
        // go left till the end

        // None
        if curr.left.is_none() {
            let right = curr.right.take();
            return (Some(curr), right);
        }

        let (min_node, left_node) = Self::take_min(curr.left.take().unwrap());
        curr.left = left_node;
        (min_node, Some(Self::balance(curr)))
    }

    fn delete(&mut self, key: &[u8], timestamp: u64) {
        let node = Node {
            entry: AvlEntry {
                key: key.to_vec(),
                value: Vec::new(),
                deleted: true,
                timestamp,
            },
            height: 0,
            left: None,
            right: None,
        };

        let root = self.root.take();
        self.root = self.insert(root, node);
    }

    fn get_min_node(node: &Option<Box<Node>>) -> Option<&Node> {
        let mut curr = node.as_ref()?;
        while let Some(n) = curr.left.as_ref() {
            curr = n
        }

        Some(curr.as_ref())
    }
    fn get_max_node(node: &Option<Box<Node>>) -> Option<&Node> {
        let mut curr = node.as_ref()?;

        while let Some(n) = curr.right.as_ref() {
            curr = n
        }

        Some(curr.as_ref()) // 
    }

    pub fn serialize_sstable_footer(
        offset: u64,
        min_key: &[u8],
        max_key: &[u8],
        sizeof_si: u64,
        sizeof_bf: u64,
        level: u8,
    ) -> Vec<u8> {
        let mut footer: Vec<u8> = Vec::new();

        footer.extend_from_slice(min_key);
        footer.extend_from_slice(max_key);

        footer.extend_from_slice(&offset.to_le_bytes());
        footer.extend_from_slice(&sizeof_si.to_le_bytes());
        footer.extend_from_slice(&sizeof_bf.to_le_bytes());
        footer.extend_from_slice(&(min_key.len() as u64).to_le_bytes());
        footer.extend_from_slice(&(max_key.len() as u64).to_le_bytes());
        footer.extend_from_slice(&level.to_le_bytes()); // Level of sstable, starts at L0
        // entire footer: | sparse_index | bloom_filter | min key | max key |  sparse_index_offset| sizeof(sparse_index) | sizeof(bloom_filter) | sizeof(minkey) |
        // | sizeof(maxkey) | level | sparse_crc(4 bytes) | bloom_crc(4 bytes) | min_max_key_crc | metadata_crc(4 bytes) |

        footer
    }

    fn build_sstable_recursive(
        &self,
        writer: &mut BufWriter<File>,
        n: &Option<Box<Node>>,
        bf: &mut BloomFilter,
        data_block: &mut Option<SsTableDataBlock>,
        sparse_index: &mut SparseIndex,
        offset: &mut u64,
    ) -> Result<()> {
        if let Some(x) = n {
            self.build_sstable_recursive(writer, &x.left, bf, data_block, sparse_index, offset)?;
            if let Some(ss_data_block) = data_block {
                match ss_data_block.is_finished() {
                    true => {
                        let owned_ss_data_block =
                            data_block.take().expect("Expected a SsTableDataBlock");
                        let data_len = owned_ss_data_block.bytes.get_ref().len() as u64; // before 4 byte crc
                        let full = owned_ss_data_block.full_data_block();

                        writer.write_all(full.bytes.get_ref())?; // including 4 byte crc

                        sparse_index.add_entry(&full.starting_key, data_len, *offset);
                        *offset += full.bytes.get_ref().len() as u64;

                        let mut new_ss_db = SsTableDataBlock::new(&x.entry.key);
                        new_ss_db.append_to_block(&x.serialize_kv());
                        *data_block = Some(new_ss_db);
                    }
                    false => {
                        ss_data_block.append_to_block(&x.serialize_kv());
                    }
                }
            } else {
                let mut new_ss_db = SsTableDataBlock::new(&x.entry.key);
                new_ss_db.append_to_block(&x.serialize_kv());
                *data_block = Some(new_ss_db);
            }
            let positions = get_hashed_key_positions(&x.entry.key, bf.num_bits as usize);
            bf.set_bits(positions);
            self.build_sstable_recursive(writer, &x.right, bf, data_block, sparse_index, offset)?;
        }
        Ok(())
    }

    fn sync_avl(&self, dir: &Path, hlc: u64) -> Result<Option<(File, PathBuf, PathBuf)>> {
        let min_k = match Self::get_min_node(&self.root) {
            Some(k) => &k.entry.key,
            None => return Ok(None),
        };

        let max_k = match Self::get_max_node(&self.root) {
            Some(k) => &k.entry.key,
            None => return Ok(None),
        };

        let (file, ss_path_tmp, ss_path_final) = create_new_data_file(dir, hlc)?;
        let tmp_path_for_err_case = ss_path_tmp.clone();

        // TODO LATER: Have a Manifest file that just keeps track of what files are active and if a file isnt in the Manifest it gets deleted.
        (|| -> Result<Option<(File, PathBuf, PathBuf)>> {
            // TODO: Can also put in a function
            let mut writer = BufWriter::new(file);
            //
            let mut data_block: Option<SsTableDataBlock> = None;

            // Also TODO: see if you can use SstFinalizer here

            // sizeof(key) | key | offset | datablock block length ( before CRC )
            let mut sparse_index = SparseIndex::new();
            let mut bloom_filter = BloomFilter::new(self.size as usize * 10);

            let mut file_offset: u64 = 0;
            self.build_sstable_recursive(
                &mut writer,
                &self.root,
                &mut bloom_filter,
                &mut data_block,
                &mut sparse_index,
                &mut file_offset,
            )?;

            if let Some(last_db) = data_block {
                let len = last_db.bytes.get_ref().len() as u64;

                let full = last_db.full_data_block();
                writer.write_all(full.bytes.get_ref())?;

                sparse_index.add_entry(&full.starting_key, len, file_offset);

                file_offset += full.bytes.get_ref().len() as u64; // length here is the start of sparse_index // 
            }
            let footer = Self::serialize_sstable_footer(
                file_offset,
                min_k,
                max_k,
                sparse_index.index_entries.len() as u64,
                (bloom_filter.bits.len() * 8) as u64, // multiply by 8, needed for reading the u8s during load
                0_u8,
            );

            let footer_len = footer.len();

            let footer_crc = compute_crc_data_block(&footer[footer_len - 41..footer_len]);
            let min_max_crc = compute_crc_data_block(&footer[..footer_len - 41]);
            let sparse_crc = compute_crc_data_block(&sparse_index.index_entries);

            writer.write_all(&sparse_index.index_entries)?;

            let crc32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);
            let mut bloom_digest = crc32.digest();

            for word in &bloom_filter.bits {
                bloom_digest.update(&word.to_le_bytes());
                writer.write_all(&word.to_le_bytes())?;
            }
            let bloom_crc = bloom_digest.finalize();

            writer.write_all(&footer)?;
            writer.write_all(&sparse_crc.to_le_bytes())?;
            writer.write_all(&bloom_crc.to_le_bytes())?;
            writer.write_all(&min_max_crc.to_le_bytes())?;

            writer.write_all(&footer_crc.to_le_bytes())?;

            let f = writer.into_inner().map_err(|e| {
                DbError::FileError(
                    format!("Failed to extract File from BufWriter: {}", e.error()),
                    ss_path_tmp.to_path_buf(),
                )
            })?;
            f.sync_all()?;

            fs::rename(&ss_path_tmp, &ss_path_final)?;

            Ok(Some((f, ss_path_tmp, ss_path_final)))
        })()
        .map_err(|err| DbError::SyncFail(Box::new(err), tmp_path_for_err_case))
    }
}

pub enum FlushingThreadResponse {
    Success { id: u64, sstable: SSTable },
    Error { id: u64, error: DbError },
}

struct CompactionJob {
    // what do I need for a compaction job?
    files: Vec<PathBuf>,
}
struct FlushingManager {
    tx: Sender<FlushingThreadResponse>,
    rx: Receiver<FlushingThreadResponse>,
    compaction_jobs_queue: VecDeque<CompactionJob>, // TODO, when we are checking the score per level for compaction, we might get multiple scores >= 1
                                                    // in that case queue the compaction jobs by priority(highest first)
}

pub enum WalReplayState {
    Clean,                   // replayed everything to mem
    PartialTruncated,        // field is offset where we stopped // JUST DELETE FILE HERE
    PartialCorrupt(DbError), // offset, err // HERE YOU TELL THE CALLER THAT FILE IS CORRUPT
}
pub struct WalToMemtableReplay {
    memtable: AVL,
    records_recovered: u64,
    valid_bytes: u64,
    most_recent_hlc: Option<u64>,
    replay_state: WalReplayState,
}

impl FlushingManager {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel::<FlushingThreadResponse>();
        Self {
            tx,
            rx,
            compaction_jobs_queue: VecDeque::new(),
        }
    }

    // main will poll and on success, will add the SST to active memory and delete old_wal from directory
    fn background_flush_memtable(
        &mut self,
        frozen_instance: FrozenMemtableInstance,
        dir: PathBuf,
        hlc: u64,
    ) -> Result<()> {
        let tx: Sender<FlushingThreadResponse> = self.tx.clone();

        spawn(move || -> Result<()> {
            let (f, ss_path_final) = match frozen_instance
                .memtable
                .sync_avl(&dir, frozen_instance.id)
            {
                Ok(Some((f, _, ss_path_final))) => {
                    if let Some(dir) = ss_path_final.parent() {
                        // always should have parent
                        File::open(dir)?.sync_all()?;
                    }

                    (f, ss_path_final)
                }
                Err(DbError::SyncFail(err, path)) => {
                    // delete the path since sync failed
                    let _ = fs::remove_file(&path);
                    let _ = tx.send(FlushingThreadResponse::Error {
                        id: frozen_instance.id,
                        error: DbError::SyncFail(Box::new(*err), path.to_path_buf()),
                    });
                    return Err(DbError::ReportedViaChannel);
                }
                Err(e) => {
                    let _ = tx.send(FlushingThreadResponse::Error {
                        id: frozen_instance.id,
                        error: e,
                    });
                    return Err(DbError::ReportedViaChannel);
                }
                Ok(None) => {
                    // channel should know
                    let _ = tx.send(FlushingThreadResponse::Error {
                        id: frozen_instance.id,
                        error: DbError::MemTableSyncError("The memtable returned None".to_string()),
                    });
                    return Err(DbError::ReportedViaChannel); // empty AVL, do nothing
                }
            };

            let sstable = SSTable::load(&ss_path_final);
            // TODO HERE: now that I am returning an error on the crc check fail, we need to rebuild the SStable in the case of
            // the spars index being corrupted, use crcmismatch type
            // if other err like: NonNumericFileIdOnSstable just send the error to main
            match sstable {
                Ok(sst) => {
                    // PROBLEM: WE INITIATE THE SEND TO MAIN HERE, HOWEVER FROZEN GETS DROPPED AT THE END OF THIS FUNCTION WHICH IN TURN
                    // MEANS THAT THE WEAK MIGHT ALSO DROP THE MEMTABLE, SO WE CAN HAVE LOSS OF DATA IN THE SPAN OF THIS SEND
                    // TO ITS RECEIVAL, SO JUST USE AN ARC FOR FLUSHING MEMTABLE AS WELL AND JUST DORP IT EXPLICITLY
                    // DONE: JUST REMEMBER TO DROP THE ARC WHEN MAIN RECEIVES THIS
                    let _ = tx.send(FlushingThreadResponse::Success {
                        id: frozen_instance.id,
                        sstable: sst,
                    });
                }
                Err(DbError::DataCorrupted(DataCorruptedErr {
                    reason:
                        CorruptionType::CrcMismatch {
                            mismatch_type: CrcType::SparseIndex,
                            ..
                        }
                        | CorruptionType::MetaDataSizeExceedsFileSize { .. }
                        | CorruptionType::MetadataSizeOverflow { .. },
                    ..
                })) => {
                    //TODO READ COMMENT(WHERE WE CALL SSTable::load on KVE::open())
                }
                // SHOULD NOT GET ANY OF THE CRCMISMATCH ERRORS OR FAILURES HERE SINCE WE JUST SYNCED THIS TO FILE CORRECTLY
                Err(dberr) => {
                    let _ = tx.send(FlushingThreadResponse::Error {
                        id: frozen_instance.id,
                        error: dberr,
                    });
                    return Err(DbError::ReportedViaChannel);
                }
            }

            Ok(())
        });

        Ok(())
    }

    fn build_avl_from_wal(&self, path: &PathBuf) -> Result<WalToMemtableReplay> {
        let mut memtable = AVL::new(MEMTABLE_THRESHOLD);
        let mut curr_offset: u64 = 0;
        let mut records_recovered = 0;
        let mut most_recent_hlc: Option<u64> = None;

        let wal_f = File::open(path)?;

        let file_len = wal_f.metadata()?.len();

        let mut reader = BufReader::new(&wal_f);

        let mut type_of_record: [u8; 1] = [0u8; 1];
        let mut valid_bytes: u64 = 0;
        let mut ksz = [0u8; 8];
        let mut tstamp = [0u8; 8];
        let mut vsz = [0u8; 8];
        let mut crc = [0u8; 4];
        let mut pos: u64 = 0;
        let outcome = (|| -> Result<()> {
            while pos < file_len {
                // We should read records up until a truncated record or a corrupted record, then we stop
                read_exact_or_corrupt(&mut reader, &mut type_of_record, curr_offset, path)?;
                curr_offset += 1;
                let type_tag = type_of_record[0];

                match type_tag {
                    TAG_DELETION => {
                        //  TAG_DELETION handle  [ tstamp(8) | ksz(8) | key(sizeof ksz ) |crc (4 bytes) ]
                        read_exact_or_corrupt(&mut reader, &mut tstamp, curr_offset, path)?;
                        curr_offset += 8;
                        read_exact_or_corrupt(&mut reader, &mut ksz, curr_offset, path)?;
                        curr_offset += 8;

                        let key_size = u64::from_le_bytes(ksz);

                        if key_size > KEY_MAX_BYTES_SIZE {
                            return Err(DbError::DataCorrupted(DataCorruptedErr {
                                offset: pos,
                                file_path: path.to_path_buf(),
                                reason: CorruptionType::Other(format!(
                                    "record size overflow: ksz={key_size}"
                                )),
                            }));
                        }
                        let mut key_buffer = vec![0u8; key_size as usize];

                        read_exact_or_corrupt(&mut reader, &mut key_buffer, curr_offset, path)?;
                        curr_offset += key_size;

                        let crc_data_block =
                            [type_of_record.as_slice(), &tstamp, &ksz, &key_buffer].concat();
                        let crc_to_check = compute_crc_data_block(&crc_data_block);

                        read_exact_or_corrupt(&mut reader, &mut crc, curr_offset, path)?;
                        curr_offset += 4;

                        let crc_from_buff = u32::from_le_bytes(crc);

                        check_crc(crc_to_check, crc_from_buff, pos, path, CrcType::WalRecord)?;
                        let ts = u64::from_le_bytes(tstamp);
                        most_recent_hlc = Some(most_recent_hlc.unwrap_or(0).max(ts));

                        valid_bytes += key_size + 21; // 4 for crc
                        pos = reader.stream_position()?;
                        memtable.delete(&key_buffer, ts);
                        records_recovered += 1;
                    }
                    TAG_INSERTION => {
                        read_exact_or_corrupt(&mut reader, &mut tstamp, curr_offset, path)?;
                        curr_offset += 8;
                        read_exact_or_corrupt(&mut reader, &mut ksz, curr_offset, path)?;
                        curr_offset += 8;
                        read_exact_or_corrupt(&mut reader, &mut vsz, curr_offset, path)?;
                        curr_offset += 8;

                        let key_size = u64::from_le_bytes(ksz);
                        let val_size = u64::from_le_bytes(vsz);

                        if key_size > KEY_MAX_BYTES_SIZE || val_size > VALUE_MAX_BYTES_SIZE {
                            return Err(DbError::DataCorrupted(DataCorruptedErr {
                                offset: pos,
                                file_path: path.to_path_buf(),
                                reason: CorruptionType::Other(format!(
                                    "record size overflow: ksz={key_size} vsz={val_size}"
                                )),
                            }));
                        }
                        let mut key_buffer = vec![0u8; key_size as usize];
                        let mut val_buffer = vec![0u8; val_size as usize];

                        read_exact_or_corrupt(&mut reader, &mut key_buffer, curr_offset, path)?;
                        curr_offset += key_size;

                        read_exact_or_corrupt(&mut reader, &mut val_buffer, curr_offset, path)?;
                        curr_offset += val_size;

                        let crc_data_block = [
                            type_of_record.as_slice(),
                            &tstamp,
                            &ksz,
                            &vsz,
                            &key_buffer,
                            &val_buffer,
                        ]
                        .concat();
                        let crc_to_check = compute_crc_data_block(&crc_data_block);

                        read_exact_or_corrupt(&mut reader, &mut crc, curr_offset, path)?;
                        curr_offset += 4;

                        let crc_from_buff = u32::from_le_bytes(crc);

                        check_crc(crc_to_check, crc_from_buff, pos, path, CrcType::WalRecord)?;

                        let ts = u64::from_le_bytes(tstamp);
                        most_recent_hlc = Some(most_recent_hlc.unwrap_or(0).max(ts));

                        valid_bytes += key_size + val_size + 29;
                        pos = reader.stream_position()?;

                        memtable.put(&key_buffer, &val_buffer, ts);
                        records_recovered += 1;

                        // TAG_INSERTION handle tstamp | ksz | vsz | key | value |crc (4 bytes)
                    }
                    _ => {
                        // corrupt
                        return Err(DbError::DataCorrupted(DataCorruptedErr {
                            reason: CorruptionType::RecordTypeCorrupted { found: type_tag },
                            offset: curr_offset - 1,
                            file_path: path.to_path_buf(),
                        }));
                    }
                }
            }
            Ok(())
        })();

        let replay_state = match outcome {
            Ok(()) => WalReplayState::Clean,
            Err(DbError::DataCorrupted(DataCorruptedErr {
                reason: CorruptionType::TruncatedRecord,
                offset,
                ..
            })) => WalReplayState::PartialTruncated,
            Err(e) => WalReplayState::PartialCorrupt(e),
        };
        match replay_state {
            // repetitive but eventually PartialCorrupt might do extra stuff
            replay_state @ WalReplayState::Clean => Ok(WalToMemtableReplay {
                memtable,
                records_recovered,
                valid_bytes,
                replay_state,
                most_recent_hlc,
            }),
            replay_state @ WalReplayState::PartialTruncated => Ok(WalToMemtableReplay {
                memtable,
                records_recovered,
                valid_bytes,
                replay_state,
                most_recent_hlc,
            }),
            replay_state @ WalReplayState::PartialCorrupt(_) => Ok(WalToMemtableReplay {
                memtable,
                records_recovered,
                valid_bytes,
                replay_state,
                most_recent_hlc,
            }),
        }
    }

    fn retrieve_wal_records(
        &mut self,
        path: &PathBuf,
        dir: &PathBuf,
        hlc: &Hlc,
    ) -> Result<Option<SSTable>> {
        let memtable = match self.build_avl_from_wal(path) {
            Ok(replay) => {
                // WE ENSURE OUR CURR HLC IS MORE RECENT THAN THE HIGHEST HLC IN THE WAL
                if let Some(most_rec_hlc) = replay.most_recent_hlc {
                    hlc.recover_to(most_rec_hlc);
                    // if hlc is way ahead of most_rec_hlc, it doesnt recover to it, but this could be an issue because
                    // then we are assigning the sst below a clock that is way ahead of its most recent record
                    // which could be an issue if we have multiple WALs with different records, then we lose the correct order because they all get
                    // a ordering of greater than the current clock, regardless of the records within them
                    // so it would be better if the wal->sst files get a hlc directly from its most recent record
                }

                match replay.replay_state {
                    WalReplayState::Clean | WalReplayState::PartialTruncated => replay.memtable,
                    WalReplayState::PartialCorrupt(_) => {
                        // TODO: caller might want to know that we worked on a corrupt file in the future
                        // FOR LOGGING PURPOSES ^^
                        replay.memtable
                    }
                }
            }

            Err(e) => {
                // Didnt retrieve anything
                return Err(e);
            }
        };

        let (f, _, ss_final_path) = match memtable.sync_avl(dir, hlc.tick()) {
            Ok(Some((f, tmp_file, ss_final_path))) => {
                if let Some(dir) = ss_final_path.parent() {
                    // always should have parent
                    File::open(dir)?.sync_all()?;
                }
                let _ = fs::remove_file(path); // wal data has been put into sst, remove wal
                (f, tmp_file, ss_final_path)
            }
            Err(DbError::SyncFail(err, path)) => {
                let _ = fs::remove_file(&path);

                return Err(DbError::SyncFail(Box::new(*err), path.to_path_buf()));
            }

            Err(err) => {
                return Err(err);
            }
            Ok(None) => {
                return {
                    let _ = fs::remove_file(path);
                    Ok(None)
                };
            }
        };
        let sstable = SSTable::load(&ss_final_path)?;

        Ok(Some(sstable))
    }
}

struct FrozenMemtableInstance {
    memtable: Arc<AVL>,
    finished: bool,
    id: u64, // hlc
}
// TODO:
struct KVEngine {
    // node_id: have a unique ID here
    data_directory: PathBuf, // data_directory now holds all .sst and .wal files
    sstables: Option<Arc<RwLock<[Vec<SSTable>; SST_LEVEL_COUNT]>>>, // [vec0(l0), vec1(l1)] .. etc/
    sync_config: SyncConfig,
    wal: WAL,
    frozen_wal: Option<WAL>, // TODO: eventually there can be multiple of these
    memtable: AVL,
    frozen_memtables: Option<BTreeMap<u64, FrozenMemtableInstance>>, // ordered. id(hlc) -> mem
    // frozen_memtable: Option<Arc<AVL>>,                               // and here
    corrupted_files: HashSet<PathBuf>,
    flushing_manager: FlushingManager,
    hlc: Arc<Hlc>, // first 52 bits are the time stamp, 12 last bits are the counter
}

pub struct Hlc {
    hlc: AtomicU64,
}

impl Hlc {
    fn new() -> Self {
        // we always create a new one for now, but in reality, we only create a new one on first boot, then we check the last most recent
        // timestmap in the database and we check if we need a new timestamp or we use the last one + counter goes up
        Self {
            hlc: AtomicU64::new(new_timestamp() << NUM_OF_BITS_FOR_COUNTER),
        } // counter starts at 0 here
    }

    fn curr_timestamp(&self) -> u64 {
        new_timestamp() << NUM_OF_BITS_FOR_COUNTER
    }

    pub fn sync_with_remote(&self, remote_hlc: u64) -> u64 {
        self.advance(remote_hlc)
    }

    pub fn recover_to(&self, hlc_from_disk: u64) -> u64 {
        self.advance(hlc_from_disk)
    }

    pub fn tick(&self) -> u64 {
        self.advance(0)
    }
    fn advance(&self, floor: u64) -> u64 {
        let curr_tstamp = self.curr_timestamp();
        let mut prev = self.hlc.load(Relaxed);
        loop {
            let max = prev.max(floor);
            let hlc_tstamp = max & MASK_FOR_TSTAMP;

            let new = if hlc_tstamp >= curr_tstamp {
                let new_counter = (max & MASK_FOR_COUNTER) + 1;
                if new_counter > MASK_FOR_COUNTER {
                    // if counter overflows, we add 1 to the timestamp, so we are advancing the physical clock and resetting counter to 0
                    // physical clock starts 12 bits to the left so thats why we add the operation below
                    hlc_tstamp + (MASK_FOR_COUNTER + 1) // Mask is 12 ones, add 1 to get 4096
                } else {
                    hlc_tstamp | new_counter // new counter here is at most 4095 no need to use mask
                }
            } else {
                curr_tstamp
            };

            match self
                .hlc
                .compare_exchange_weak(prev, new, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return new,
                Err(x) => prev = x,
            }
        }
    }

    pub fn deserialize_hlc(hlc: u64) -> (u64, u64) {
        // returns (timestmap, counter)
        ((hlc >> NUM_OF_BITS_FOR_COUNTER), (hlc & MASK_FOR_COUNTER)) // timestamp okay to move down to lower bits, counter cant move up
    }
}

impl KVEngine {
    // threshold and sync_config can be part of one config struct later.
    fn open(dir_name: &Path, sync_config: SyncConfig) -> Result<KVEngine> {
        // TODO: Put the actual directory somewhere specific not in the working dir
        let path = PathBuf::from(dir_name);

        let mut sstables: [Vec<SSTable>; SST_LEVEL_COUNT] = [const { Vec::new() }; SST_LEVEL_COUNT];

        let memtable = AVL::new(MEMTABLE_THRESHOLD);

        let mut sst_vec: Vec<PathBuf> = Vec::new();
        let mut wal_vec: Vec<PathBuf> = Vec::new();

        // sort by
        for entry in fs::read_dir(dir_name)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            match path.extension().and_then(|x| x.to_str()) {
                Some(e) => match e {
                    "sst" => {
                        sst_vec.push(path);
                    }
                    "wal" => {
                        wal_vec.push(path);
                    }
                    "tmp" => {
                        let _ = remove_file(path); // unfinished sync
                    }
                    _ => {}
                },
                _ => continue,
            };
        }

        let max_hlc_from_ssts = find_max_hlc_between_files(sst_vec.as_slice()).unwrap_or(0);

        // I have wal_paths, with data that needs to be synced as ssts.
        //

        let hlc = Hlc::new();

        hlc.recover_to(max_hlc_from_ssts);
        let wal = WAL::new(MEMTABLE_THRESHOLD, sync_config, &path, hlc.tick())?;
        // IMPORTANT: The new wal is created after we check the actual directory for wal files.
        // This is important because we do not want to call retrieve_wal_records() on the new empty wal

        let mut self_instance = Self {
            data_directory: path,
            sync_config,
            memtable,
            sstables: None,
            wal,
            frozen_memtables: None,
            frozen_wal: None,
            flushing_manager: FlushingManager::new(),
            corrupted_files: HashSet::new(),
            hlc: Arc::new(hlc),
        };

        for path in sst_vec {
            match SSTable::load(&path) {
                Ok(sst) => {
                    if sst.level < SST_LEVEL_COUNT as u8 {
                        sstables[sst.level as usize].push(sst);
                    } else {
                        // probs corrupted
                        self_instance.corrupted_files.insert(sst.file_path); // Should I store just the path or the entire SST? 
                        // put in a corrupted vec
                    }
                }
                Err(DbError::DataCorrupted(DataCorruptedErr {
                    reason:
                        CorruptionType::CrcMismatch {
                            mismatch_type: CrcType::SparseIndex,
                            ..
                        }
                        | CorruptionType::MetaDataSizeExceedsFileSize { .. }
                        | CorruptionType::MetadataSizeOverflow { .. },
                    ..
                })) => {
                    // rebuild all the metadata(sparse, bloom, min max etc)
                    // what needs to be done here? we have a sstable with presumably some correct data in there but
                    // the metadata is corrupt, do I read the sstable front to back and create the metadata as we go?
                    // Read the sstable in order(needs the change below), write a new sstable with all the valid data_records
                    // while also building the bloom_filter, sparse_index etc, attach all metadata at the end
                    // then name the rebuilt sstable the same as the sstable with the corrupt metadata so we preserve key recency order

                    // Note: For now I'm just going to treat the sstable as corrupt and the data 3lost because I need to change
                    // the sparse_index format to be firstkey: offset and have the offset point to the data_block_length in the front of a
                    // datea_block, so we jump to that offset, the first 8 bytes tell us the data_block_length, then we read the data_block
                    // compared to what I have now: firstkey: (offset, data_block_length) which means I need the sparse_index to know where
                    // and how long the data_block_length is, meaning if there is a corrupt sparse_index, I cannot rebuild it because
                    // I dont know where data_blocks start or end
                    // LEAVING THIS HERE BECAUSE I WILL IMPLEMENT THIS LATER
                }
                Err(
                    DbError::InvalidSstableFileName(_p) | DbError::NonNumericFileIdOnSstable(_p),
                ) => {
                    continue;
                }
                Err(dberr) => {
                    continue;
                    // we can reach here if read_exact fails for example or seeking fails
                    // what to do? skip for now
                }
            }
        }

        let mut wal_vec = wal_vec
            .into_iter()
            .filter(|x| {
                x.file_stem()
                    .and_then(|x| x.to_str())
                    .and_then(|x| x.parse::<u64>().ok())
                    .is_some()
            })
            .collect::<Vec<PathBuf>>();

        wal_vec.sort_by_key(|x| {
            x.file_stem()
                .and_then(|x| x.to_str())
                .map(|x| x.parse::<u64>().ok())
        });
        for path in wal_vec {
            // wal populates this and we flush it to disk as an .sst

            match self_instance.flushing_manager.retrieve_wal_records(
                &path,
                &self_instance.data_directory,
                &self_instance.hlc,
            ) {
                // TIHS SHOULD ANTICIPATE THE SPARSE_INDEX_CRC_MISMATCH FAILURE IN THE FUTURE
                // WHERE WE WILL REBUILD THE SSTABLE
                // FOR NOW, IF IT FAILS WE DONT PUSH SST
                Ok(Some(ss)) => {
                    if ss.level < SST_LEVEL_COUNT as u8 {
                        sstables[ss.level as usize].push(ss);
                    } else {
                        // probs corrupted
                        self_instance.corrupted_files.insert(ss.file_path);
                        // put in a corrupted vec
                    }
                }
                Err(e) => continue,
                Ok(None) => continue,
            }
        }
        // TODO MAYBE: Other than L0, all the other levels have no overlapping keys in the sstables
        // meaning that they could be ordered by min_k, that way a binary search can be done on them instead of linearly checking every sst for the record
        // good enough for now
        sstables.iter_mut().for_each(|ss_vec| {
            ss_vec.sort_by_key(|p| Reverse(p.id)); // Descending order
        });

        self_instance.sstables = Some(Arc::new(RwLock::new(sstables)));
        Ok(self_instance)
    }

    fn should_search_sstable_file(key: &[u8], sstable: &SSTable) -> bool {
        if let Some((min, max)) = &sstable.min_max_keys
            && (key > max.as_slice() || key < min.as_slice())
        {
            return false;
        }

        if let Some(bloom_filter) = &sstable.bloom_filter {
            let bf_bit_positions = get_hashed_key_positions(key, bloom_filter.num_bits as usize);
            bloom_filter.check_bits(bf_bit_positions)
        } else {
            true // If we do not have a bloom filter, we just search the file without bloom filter optimization
        }
    }

    fn search_kv_in_sstable(sstable: &SSTable, key: &[u8]) -> Result<Lookup> {
        let Some((offset, data_len)) = sstable.binary_search_sparse_index(key) else {
            return Ok(Absent);
        };

        if data_len > DATA_BLOCK_MAX_BYTES_SIZE {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset,
                file_path: sstable.file_path.clone(),
                reason: CorruptionType::BufferExceedsMaxLength {
                    size: data_len,
                    max_size: DATA_BLOCK_MAX_BYTES_SIZE,
                },
            }));
        }
        let mut data_buffer = vec![0u8; data_len as usize];

        let mut crc = [0u8; 4];

        let mut reader = BufReader::new(&sstable.file);

        reader.seek(SeekFrom::Start(offset))?;

        reader.read_exact(&mut data_buffer)?;

        //
        // we read CRC here because data_len above doesnt take into account the 4 bytes for crc
        reader.read_exact(&mut crc)?;
        let crc_from_buff = u32::from_le_bytes(crc);

        let fresh_crc = compute_crc_data_block(&data_buffer);

        check_crc(
            fresh_crc,
            crc_from_buff,
            offset,
            &sstable.file_path,
            CrcType::DataBlock,
        )?;

        let mut pos = 0;
        while pos < data_buffer.len() {
            if pos + 25 > data_buffer.len() {
                return Err(DbError::DataCorrupted(DataCorruptedErr {
                    offset: offset + pos as u64,
                    file_path: sstable.file_path.clone(),
                    reason: CorruptionType::Other(format!(
                        "truncated record header at buffer position {} (buffer len {})",
                        pos,
                        data_buffer.len(),
                    )),
                }));
            }

            // its actually: [ tstamp(8) | ksz(8) | value_sz(8) |tombstone| key | value |  ]
            let ksz = u64::from_le_bytes(
                read_range(&data_buffer, pos + 8, pos + 16)?
                    .try_into()
                    .unwrap(),
            ) as usize;
            // u64::from_le_bytes(data_buffer[pos + 8..pos + 16].try_into().unwrap()) as usize;
            let vsz = u64::from_le_bytes(
                read_range(&data_buffer, pos + 16, pos + 24)?
                    .try_into()
                    .unwrap(),
            ) as usize;
            // u64::from_le_bytes(data_buffer[pos + 16..pos + 24].try_into().unwrap()) as usize;
            // let deleted = &data_buffer[pos + 24..pos + 25][0];
            let deleted = read_range(&data_buffer, pos + 24, pos + 25)?[0];
            // [ tstamp(8) | ksz(8) | value_sz(8) | deletedflag(1) | key | value ]
            // check ksz and vsz doesnt overflow
            let key_start = pos + 25;

            let val_end = key_start
                .checked_add(ksz)
                .and_then(|v| v.checked_add(vsz))
                .ok_or_else(|| {
                    DbError::DataCorrupted(DataCorruptedErr {
                        offset: offset + pos as u64,
                        file_path: sstable.file_path.clone(),
                        reason: CorruptionType::Other(format!(
                            "record size overflow: ksz={ksz}, vsz={vsz}"
                        )),
                    })
                })?;

            if val_end > data_buffer.len() {
                return Err(DbError::DataCorrupted(DataCorruptedErr {
                    offset: offset + pos as u64,
                    file_path: sstable.file_path.clone(),
                    reason: CorruptionType::LengthMismatch {
                        expected: val_end,
                        found: data_buffer.len(),
                    },
                }));
            }

            let val_start = key_start + ksz; // if val_end is safe then this is safe(no overflow)
            let curr_key = read_range(&data_buffer, key_start, val_start)?;
            // let curr_key = &data_buffer[key_start..val_start];
            let value = read_range(&data_buffer, val_start, val_end)?;
            // let value: &[u8] = &data_buffer[val_start..val_end];

            match curr_key.cmp(key) {
                CmpOrdering::Less => {
                    pos = val_end;
                    continue;
                }
                CmpOrdering::Equal => {
                    if deleted == 0xFF {
                        return Ok(Deleted);
                    }
                    return Ok(Found(value.to_vec()));
                }
                CmpOrdering::Greater => break,
            }
        }
        Ok(Absent)
    }
    fn search_for_kv_in_sstables(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(sstables) = &self.sstables {
            // Lock here is held for the entirety of the loop. Ok for now, mostly reads, rare writes
            for level in sstables.read().unwrap().iter() {
                for element in level.iter() {
                    match Self::should_search_sstable_file(key, element) {
                        true => match Self::search_kv_in_sstable(element, key)? {
                            Found(k) => return Ok(Some(k)),
                            Deleted => return Ok(None),
                            Absent => {
                                continue;
                            }
                        },
                        false => continue,
                    }
                }
            }
        }
        Ok(None)
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.memtable.get(key) {
            Found(bytes) => return Ok(Some(bytes.to_vec())),
            Deleted => return Ok(None),
            Absent => {} // fall through
        }

        if let Some(frozen_memtable_collection) = self.frozen_memtables.as_ref() {
            for (id, mem_table_instance) in frozen_memtable_collection.iter().rev() {
                // rev() because we search newer memtables first which have a higher id(hlc)

                match mem_table_instance.memtable.get(key) {
                    Found(bytes) => return Ok(Some(bytes.to_vec())),
                    Deleted => return Ok(None),
                    Absent => {}
                }
            }
        }

        self.search_for_kv_in_sstables(key) // if we get here, 
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.memtable
            .exceeds_max(key.len() as u64, value.len() as u64)?;

        if (key.len() as u64 + value.len() as u64 + self.memtable.size_in_bytes)
            > self.memtable.threshold
        {
            self.rotate_memtable_and_wal()?;
        }
        // let tstamp = new_timestamp();
        let hlc = self.hlc.tick();
        self.wal
            .record_to_wal(WalRecordType::Insertion(key, value), hlc)?;

        self.memtable.put(key, value, hlc);

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        let k_len = key.len() as u64;
        self.memtable.exceeds_max(k_len, 0)?;
        if (k_len + self.memtable.size_in_bytes) > self.memtable.threshold {
            self.rotate_memtable_and_wal()?;
        }
        // let tstamp = new_timestamp();
        let hlc = self.hlc.tick();
        self.wal.record_to_wal(WalRecordType::Deletion(key), hlc)?;
        self.memtable.delete(key, hlc);

        Ok(())
    }

    fn rotate_memtable_and_wal(&mut self) -> Result<()> {
        let old_wal = std::mem::replace(
            &mut self.wal,
            WAL::new(
                MEMTABLE_THRESHOLD,
                self.sync_config,
                &self.data_directory,
                self.hlc.tick(),
            )?,
        );
        // WHEN MAIN(whoever polls it) RECEIVES A SUCCESSFUL FLUSH, REMOVE THE OLD WAL ASSOCIATED WITH THAT FLUSH
        let frozen = Arc::new(std::mem::replace(
            &mut self.memtable,
            AVL::new(MEMTABLE_THRESHOLD),
        ));

        // PROBLEM: rotation1 starts, we create run this code here, we have flushing_mem and frozen_wal saved
        // but before rotation1 sync finishes another one starts, replacing our flushing_mem and frozen_wal with the curr ones
        // meaning we lose the rotation1 flushing mem data for lookup until rotation1 syncing is done.
        // so use a structure that can hold multiple ordered ones

        self.frozen_wal = Some(old_wal);
        let tick = self.hlc.tick();

        // WHEN MAIN RECEIVES MESSAGE ABOUT A SUCCESSFUL SST SYNCED, USE SST ID TO REMOVE IT FROM FROZEN COLLECTION
        // GET METHOD SHOULD FIRST CHECK ACTIVE MEMTABLE, THEN THE COLLECTION OF FROZEN FROM MOST RECENT TO LAST
        // order: frozen1 -> tick123, frozen2->tick240, frozen3->tick500
        // most recent is tick500 so the get method should go through them in reverse

        // PROBLEM ^:
        // if a flush that contains more recent data finished before older flushes ->
        // it sits as a sst whereas the get() method checks frozen wals first but the newer data sits behind as a sst, serving stale data
        // figure out a way to not let this happen
        // If we keep every frozen memtable in memory, thats a lot of space amplification
        // also depending on load, we could always have frozen AVLS in memory
        // only remove frozen mem if its older than every other frozen mem ? so older memtables wait for more recent ones
        // Node(id: 145, AVL, state: unifinished), Node(id: 241, AVL, state: unfinished), Node(id: 13,AVL, state: unfinished), Node(id: 600, AVL, state: finished(is already an sst on disk))
        // if Node.600 gets removed from memory, we search the other memtables before the SSTS and serve STALE data
        // fix: on poll, when a Node.id is returned, mark it finished, then have another function check if there is any OLDER data on memtable memory
        // if yes, we have to wait for all of those to finish to retire Node.600
        // on poll, run a while loop that gets the first element(dont pop yet), checks if its finished, if yes retire(first element means OLDEST by id) so its okay to retire
        // also make sure to mark the mem returned as finished before this
        if let Some(frozen_mems) = self.frozen_memtables.as_mut() {
            frozen_mems.insert(
                tick,
                FrozenMemtableInstance {
                    finished: false,
                    memtable: Arc::clone(&frozen),
                    id: tick,
                },
            );
        }

        self.flushing_manager.background_flush_memtable(
            FrozenMemtableInstance {
                finished: false,
                memtable: Arc::clone(&frozen),
                id: tick,
            },
            self.data_directory.clone(),
            tick,
        )?;

        Ok(())
    }

    fn does_overlap(sstable: &SSTable, min_k: &[u8], max_k: &[u8]) -> bool {
        if let Some((other_ss_min_k, other_ss_max_k)) = sstable.min_max_keys.as_ref() {
            return (other_ss_min_k.as_slice() <= max_k && min_k <= other_ss_max_k.as_slice());
        }
        false
    }

    fn get_min_max_key_range_of_entire_level(
        &self,
        level: u8,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let Some(levels) = &self.sstables else {
            return Ok(None);
        };
        let levels = levels.read().unwrap();
        let Some(sstables) = levels.get(level as usize) else {
            return Ok(None);
        };
        let mut curr_min_max: Option<(&[u8], &[u8])> = None;

        for sstable in sstables {
            let Some((curr_ss_min, curr_ss_max)) = sstable.min_max_keys.as_ref() else {
                // jf ss has no min_max_keys(which we should always make sure it does since even if they are corrupted, we can rebuild by consulting the sparse_i)
                // but in case theres nothing here just return an Error for now
                return Err(DbError::MissingKey(
                    "Min-Max keys missing from SStables metadata".to_string(),
                )); // TODO: fix this error return, its just here so it compiles, either return a correct error or rebuild min max
            };

            curr_min_max = match curr_min_max {
                Some((curr_min, curr_max)) => {
                    Some((curr_min.min(curr_ss_min), curr_max.max(curr_ss_max)))
                }
                None => Some((curr_ss_min, curr_ss_max)),
            }
        }

        Ok(curr_min_max.map(|(min, max)| (min.to_vec(), max.to_vec())))
    }

    fn select_files_for_l0_compaction(&self) -> Result<Option<Vec<PathBuf>>> {
        let mut vec_of_overlapping_pathbufs: Vec<PathBuf> = Vec::new();
        if let Some((min_k, max_k)) = self.get_min_max_key_range_of_entire_level(0)? {
            let Some(levels) = &self.sstables else {
                return Ok(None);
            };
            let levels = levels.read().unwrap();
            let _ = &levels[0]
                .iter()
                .for_each(|ss| vec_of_overlapping_pathbufs.push(ss.file_path.clone()));
            // what if L1 is empty?
            if let Some(vector_of_sstables_one_level_up) = levels.get((1) as usize) {
                vector_of_sstables_one_level_up.iter().for_each(|sst| {
                    KVEngine::does_overlap(sst, min_k.as_slice(), max_k.as_slice()).then(|| {
                        vec_of_overlapping_pathbufs.push(sst.file_path.clone());
                    });
                });
            }
        }

        Ok(Some(vec_of_overlapping_pathbufs))
    }

    // TODO: pass the guard to these functions, you do not need to take a RwLockReadGuard 3 times to do one thing and could change
    // grab levels from a driver function, pass it on to all the functions that need it to determine CompactionJob
    // function doesnt take L0 into consideration, for that we compact when we pass file count threshold
    // when should this function be called? every once in a while? when a threshold of any level is reached?
    fn select_level_for_compaction(&self) -> Result<Option<(f64, u8)>> {
        let Some(levels) = &self.sstables else {
            return Ok(None);
        };

        let mut best_ratio_candidate: Option<(f64, u8)> = None; // first number is ratio, second is what level

        let levels = levels.read().unwrap();

        let l0 = &levels[0];
        let ratio = l0.len() as f64 / NUM_OF_L0_FILES_TO_TRIGGER_COMPACTION as f64;
        if ratio >= 1_f64 {
            best_ratio_candidate = Some((ratio, 0))
        }

        for level in 1..levels.len() - 1 {
            let sstables_in_level = &levels[level];

            let bytes_in_entire_level: u64 =
                sstables_in_level.iter().map(|sst| sst.file_size).sum();

            let target_to_trigger: u64 = MAX_SST_SIZE * 10_u64.pow(level as u32); // corresponds to NUM_OF_BYTES_NEEDED_TO_TRIGGER_L1_COMPACTION et al

            if bytes_in_entire_level < target_to_trigger {
                continue;
            };

            let ratio_for_level = bytes_in_entire_level as f64 / target_to_trigger as f64;

            best_ratio_candidate = match best_ratio_candidate {
                None => {
                    if ratio_for_level >= 1_f64 {
                        Some((ratio_for_level, level as u8))
                    } else {
                        None
                    }
                }
                curr @ Some((curr_ratio, _)) => {
                    if curr_ratio < ratio_for_level {
                        Some((ratio_for_level, level as u8))
                    } else {
                        curr
                    }
                }
            }
        }

        Ok(best_ratio_candidate)
    }

    fn select_files_for_compaction(&self, level: u8) -> Result<Option<Vec<PathBuf>>> {
        if level == 0 {
            return self.select_files_for_l0_compaction();
        }

        let mut best_candidate: Option<(u64, &SSTable)> = None; // will be made into a struct later
        let mut files_to_compact: Option<Vec<PathBuf>> = None;
        // first  value is the size of the sum of bytes of all files(a level up) that overlap with SSTable
        // TODO: Remember to mark files picked for compaction

        if let Some(levels) = &self.sstables {
            let levels = levels.read().unwrap();
            let Some(curr_level_sstables) = levels.get(level as usize) else {
                return Ok(None);
            };

            let Some(vector_of_sstables_one_level_up) = levels.get((level + 1) as usize) else {
                return Ok(None);
            };
            for sstable in curr_level_sstables.iter() {
                let mut temp_curr_sum_of_file_sizes: u64 = 0;
                let (min_k, max_k) = sstable.min_max_keys.as_ref().unwrap(); // will fix unwrap
                // keep curr sstable, and go up a level, find every single file that overlaps, do the math, if better than curr best_candidate, switch

                let mut vec_of_overlapping_pathbufs: Vec<PathBuf> = Vec::new();
                vector_of_sstables_one_level_up.iter().for_each(|sst| {
                    KVEngine::does_overlap(sst, min_k, max_k).then(|| {
                        // we should also push to the vector of Pathbufs here
                        vec_of_overlapping_pathbufs.push(sst.file_path.clone());
                        temp_curr_sum_of_file_sizes += sst.file_size;
                    });
                });

                match best_candidate {
                    Some((bytes, sst)) => {
                        if (bytes as f64 / sst.file_size as f64) // RATIO used to determine candidate, we want the lower ration because theres less write ampl
                            > (temp_curr_sum_of_file_sizes as f64 / sstable.file_size as f64)
                        {
                            best_candidate = Some((temp_curr_sum_of_file_sizes, sstable));
                            vec_of_overlapping_pathbufs.push(sstable.file_path.clone());
                            files_to_compact = Some(vec_of_overlapping_pathbufs)
                        }
                    }
                    None => {
                        best_candidate = Some((temp_curr_sum_of_file_sizes, sstable));
                        vec_of_overlapping_pathbufs.push(sstable.file_path.clone());
                        files_to_compact = Some(vec_of_overlapping_pathbufs)
                    }
                }
            }
        }

        Ok(files_to_compact)
    }
}

/*Notes:
 // footer is : sparse_index | bloom_filter | min key | max key |  sparse_index_offset| sizeof(sparse_index) | sizeof(bloom_filter) | sizeof(minkey) | sizeof(maxkey) | LevelofSST(1 byte) | sparse_crc(4 bytes) | bloom_crc(4 bytes) | min_max_key_crc(4) | metadata_crc(4 bytes) |

DataBlocks:  [ tstamp(8) | ksz(8) | value_sz(8) | key | value  tstamp(8) | ksz(8) | value_sz(8) | key | value ... crc(4)]
SSTable: Datablock1 | DataBlock2 ... Datablock N | Footer
Bloom filter: k-hash bit array per SSTable to skip files on negative lookups. Use 10 bits per key. Built during flush of AVL.
*/

// SparseIndex => [ firskey:[offset, datablock_length] ]

// TODO: have SparseIndex be -> [firstkey: [offsert(points to start of data_block)] ]
// then have the data_block be datablock_length | record1| record2 ... crc
// this way we can reconstruct the sparse_index in the case of a sparse_index corruption because we can just read the records front to back without the need for the sparse_index

// wal record looks like: ksz, vsz, k, v, crc(4 bytes)
// When you read a data block in the sparse index, remember to account for the 4 crc bytes yourself, they are not accounted forin the length
/*







 For WAL records, we have deletion and insertion types so far. Will use one byte to define type. 00000100(4) = INSERTION. 00000010(2) = DELETION.
 serialized should look like this: TYPE | RECORD
 WAL RECORD can be tstamp | ksz | key |crc (4 bytes) OR it can be  | tstamp | ksz |vsz | key | value | crc(4 bytes)

 PROBLEM/UPDATE: make Bufreaders with capacity instead
 TODO: Modularize the components into their own files
 TODO.1: Document how things work


Atomics u64

// TODO: when main receives a successful sync from mebtable -> sst, check if curr number of L0 ssts >= NUM_OF_L0_FILES_TO_TRIGGER_COMPACTION, true -> trigger

TODO: USE read_exact_at from FileExt trait in place of every read_exact call()
// chekc static vs dynamic level sizing(rocksdb)

// TODO: WRITE TESTS
 //

*/
