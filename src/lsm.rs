use crc::{CRC_32_ISO_HDLC, Crc};

use core::num;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions, remove_file};
use std::io::{self, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};

use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};
use std::sync::{Weak, mpsc};
use std::thread::spawn;
use std::{todo, unimplemented};

use crate::errors::CorruptionType::Other;

use crate::errors::{
    self, CorruptionType, DataCorruptedErr, DbError, InvalidMemtableInput, Result,
};
use crate::helpers::{
    NUM_HASHES, check_key_value_record_does_not_exceed_max, compute_crc, compute_crc_data_block,
    create_new_data_file, get_hashed_key_positions, new_timestamp, read_range,
};
use crate::lsm::Lookup::{Absent, Deleted, Found};

use std::cmp::{Ordering, Reverse, max};

// const MAX_FILE_SIZE: u64 = 4 * 1024 * 1024; // SUBJECT TO CHANGE
const MEMTABLE_THRESHOLD: u64 = 8 * 1024 * 1024; // SUBJECT TO CHANGE
// this means L0 sstables are 8 mb, so we usually compact all L0 sstablse with all L1 sstables to a single L1 sstable.
const DATA_BLOCK: u16 = 8 * 1024; // Data block in SSTable
pub const DATA_BLOCK_MAX_BYTES_SIZE: u64 = 155673; // 8192(max db_size) + KEY_MAX_BYTES_SIZE + VALUE_MAX_BYTES_SIZE + 25 bytes for metadata(timestamp, ksz,vsz,tmbstone); // if we had a db_size of 8191, we could end up with adding a max val and max key
// const MAX_BLOCK_SIZE: u64 = 1024 * 1024;
pub const MAX_SST_SIZE: u64 = 1024 * 1024 * 160;
const TAG_DELETION: u8 = 2;
const TAG_INSERTION: u8 = 4;
pub const KEY_MAX_BYTES_SIZE: u64 = 16384;
pub const VALUE_MAX_BYTES_SIZE: u64 = 131072;

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
    // num_bits is used by get_hashed_key_positions as a modulus so even though we pad the bits to a whole u64 word, num_bits is still the logical count so still read/write num_bits to footer as is.
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

    fn parse_sparse_index(b: &[u8]) -> Result<Vec<(Vec<u8>, u64, u64)>> {
        // TODO: Match on result in call sites since we change signature
        let mut out = Vec::new();
        // I am parsing this layout: ksz(8) | key(of size: ksz) | offset(8) | datablock_sz(8)
        // to essentially => key | offset | datablock_sz (this lives in memory, the sparseIndex needs key to binary search. meanwhile the sparseIndex in the metadatafooter does need the key size)
        // TODO: Ensure this is safe, data could be corrupted
        // Have caller
        let mut current = 0;
        while current < b.len() {
            // let ksz = u64::from_le_bytes(b[current..(current + 8)].try_into().unwrap());
            // put in a function and reuse
            let ksz = u64::from_le_bytes(read_range(b, current, current + 8)?.try_into().unwrap());
            // if ksz > KEY_MAX_BYTES_SIZE || ksz > (b.len() as u64 - current as u64) {
            //     // err
            // }
            current += 8;
            let key = read_range(b, current, current + (ksz as usize))?.to_vec();

            // let key = b[current..(current + (ksz as usize))].to_vec();
            current += ksz as usize;

            // let offset = u64::from_le_bytes(b[current..(current + 8)].try_into().unwrap());
            let offset =
                u64::from_le_bytes(read_range(b, current, current + 8)?.try_into().unwrap());

            current += 8;
            // let data_block_size = u64::from_le_bytes(b[current..(current + 8)].try_into().unwrap());
            let data_block_size =
                u64::from_le_bytes(read_range(b, current, current + 8)?.try_into().unwrap());
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

struct FileId(u64);

struct WAL {
    wal_writer: Option<BufWriter<File>>,
    sync_c: SyncConfig,
    record_buffer: Vec<u8>,
    threshold: u64,
    path: PathBuf,
}

impl WAL {
    fn new(threshold: u64, sync_c: SyncConfig, parent_dir: &PathBuf) -> io::Result<WAL> {
        let tstamp = new_timestamp();
        let wal_path = parent_dir.join(format!("{}.wal", tstamp));

        //
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
            None => {
                todo!() // TODO: Throw error here // no
            }
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
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    sparse_index: Arc<Vec<(Vec<u8>, u64, u64)>>, // key | offset | datablock block length ( before CRC, which means you need to read the next 4 bytes and compute the crc)
    bloom_filter: BloomFilter,
    corrupted: bool,
}

impl SSTable {
    pub fn load(path: &Path) -> Result<Self> {
        // open reader of file
        // start reading backwards and return the metadata in a SST
        //// footer is :
        // sparse_index | bloom_filter | min key | max key | sizeof(sparse_index) | sparse_index_offset| sizeof(bloom_filter) | bloom filter_offset | sizeof(minkey) | minkey offset | sizeof(maxkey) | maxkey offset | 64 bytes(not including min and max key)
        //TODO: check footers checksum(doesnt have it yet)
        // TOOD: start by reading the crc of the metadata, if its good, then check the bloom_filter, if the crc is good, load it(if not skip it)
        // then check sparse_index_crc, if its good, load it, if not rebuild it
        // this means we should make bloom_filter an option, if its corrupted set to None

        // maybe have the function return Result<Option>> in case of data corruption, dont build ss, or throw err and have caller check why ss couldnt be built
        let mut f = File::open(path)?;
        let stem = path
            .file_stem()
            .and_then(|x| x.to_str())
            .ok_or_else(|| DbError::NonNumericFileIdOnSstable(path.to_path_buf()))?; // skip if this happens

        let id = stem
            .parse::<u64>()
            .ok()
            .ok_or_else(|| DbError::InvalidSstableFileName(path.to_path_buf()))?; // Have the caller skip file if this happens

        f.seek(SeekFrom::End(-40))?;
        let mut footer = [0u8; 40];
        f.read_exact(&mut footer)?;

        let mut sparse_index_crc = [0u8; 4];
        let mut bloom_filter_crc = [0u8; 4];
        let mut metadata_crc = [0u8; 4];
        let metadata_crc_in_file = u32::from_le_bytes(metadata_crc);
        let sparse_index_crc_in_file = u32::from_le_bytes(sparse_index_crc);
        let bloom_filter_crc_in_file = u32::from_le_bytes(bloom_filter_crc);

        f.seek(SeekFrom::End(-12))?;
        f.read_exact(&mut sparse_index_crc)?;
        f.read_exact(&mut bloom_filter_crc)?;
        f.read_exact(&mut metadata_crc)?;
        let footer_metadata_crc_check = compute_crc_data_block(&footer);

        if footer_metadata_crc_check != metadata_crc_in_file {
            // ERROR, throw error or rebuild HERE?
            // throw error, have caller call the rebuild function
            // can rebuild the entire metadata in that case
            // Caller calls rebuild function from this err
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: f.stream_position()?,
                file_path: path.to_path_buf(),
                reason: CorruptionType::CrcMismatch {
                    expected: footer_metadata_crc_check,
                    found: metadata_crc_in_file,
                },
            }));
        }

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

        if sparse_index_crc_in_file != sparse_index_crc_check {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: sparse_index_offset,
                file_path: path.to_path_buf(),
                reason: CorruptionType::CrcMismatch {
                    expected: sparse_index_crc_check,
                    found: sparse_index_crc_in_file,
                },
            }));
        }

        let bloom_filter: &[u8] = read_range(
            &full_sst_data,
            bloom_filter_start as usize,
            bloom_filter_end as usize as usize,
        )?;

        let bloom_filter_crc_check = compute_crc_data_block(bloom_filter);
        if bloom_filter_crc_check != bloom_filter_crc_in_file {
            // handle
            // also TODO: have the errors specify which crc failed: data_block | metadata | sparse_index etc
        }
        // &full_sst_data[(bloom_filter_start as usize)..(bloom_filter_end as usize)];
        let min_key = read_range(
            &full_sst_data,
            min_k_start as usize,
            min_k_end as usize as usize,
        )?;
        // let min_key = &full_sst_data[(min_k_start as usize)..(min_k_end as usize)];
        // let max_k = &full_sst_data[(max_k_start as usize)..(max_k_end as usize)];
        let max_k = read_range(&full_sst_data, max_k_start as usize, max_k_end as usize)?;

        // SAFE unless data is corrupted
        // if
        let bloomf_filter_64 = bloom_filter
            .chunks_exact(8)
            .map(|chunk| {
                u64::from_le_bytes(
                    chunk
                        .try_into()
                        .expect("bloom_filter not divided in 64 bit chunks, data corrupted"), // dont expect
                )
            })
            .collect();

        let parsed_sparse_index = SparseIndex::parse_sparse_index(sparse_index)?; // catch err from caller
        Ok(SSTable {
            id,
            file: f,
            file_path: path.to_path_buf(),
            file_size: file_length,
            min_key: min_key.to_vec(),
            max_key: max_k.to_vec(),
            sparse_index: Arc::new(parsed_sparse_index),
            bloom_filter: BloomFilter {
                bits: bloomf_filter_64,
                num_bits: (size_of_bloom_filter * 8),
            },
            corrupted: false,
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
            self.size_in_bytes += n.entry.value.len() as u64 + n.entry.key.len() as u64 + 25; // 25 account for record metadata
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
    // fn delete_remove_node(&mut self, curr: Option<Box<Node>>, key: &[u8]) -> Option<Box<Node>> {
    //     if let Some(mut node) = curr {
    //         if node.entry.key == key {
    //             if node.left.is_none() && node.right.is_none() {
    //                 return None;
    //             } else if node.right.is_some() != node.left.is_some() {
    //                 // XOR
    //                 // return the child
    //                 if let Some(_x) = node.left.as_ref() {
    //                     return node.left;
    //                 } else {
    //                     return node.right;
    //                 }
    //             } else {
    //                 // safe to unwrap here
    //                 let (successor, new_right) = Self::take_min(node.right.take().unwrap());
    //                 {
    //                     let succ = successor.unwrap();
    //                     node.right = new_right;
    //                     node.entry.value = succ.entry.value;
    //                     node.entry.key = succ.entry.key;
    //                 }
    //             }
    //             return Some(Self::balance(node));
    //         }

    //         if node.entry.key.as_slice() < key {
    //             node.right = self.delete_remove_node(node.right.take(), key);
    //         } else {
    //             node.left = self.delete_remove_node(node.left.take(), key);
    //         }
    //         Some(Self::balance(node))
    //     } else {
    //         curr
    //     }
    // }

    fn get_min_node(node: &Option<Box<Node>>) -> Option<&Vec<u8>> {
        let mut curr = node.as_ref()?;
        while let Some(n) = curr.left.as_ref() {
            curr = n
        }

        Some(&curr.entry.key)
    }
    fn get_max_node(node: &Option<Box<Node>>) -> Option<&Vec<u8>> {
        let mut curr = node.as_ref()?;

        while let Some(n) = curr.right.as_ref() {
            curr = n
        }

        Some(&curr.entry.key)
    }

    pub fn serialize_sstable_footer(
        offset: u64,
        min_key: &[u8],
        max_key: &[u8],
        sizeof_si: u64,
        sizeof_bf: u64,
    ) -> Vec<u8> {
        // TODO! SHOULD HAVE A CRC FOR METADATA
        let mut footer: Vec<u8> = Vec::new();

        footer.extend_from_slice(min_key);
        footer.extend_from_slice(max_key);

        footer.extend_from_slice(&offset.to_le_bytes());
        footer.extend_from_slice(&sizeof_si.to_le_bytes());
        footer.extend_from_slice(&sizeof_bf.to_le_bytes());
        footer.extend_from_slice(&(min_key.len() as u64).to_le_bytes());
        footer.extend_from_slice(&(max_key.len() as u64).to_le_bytes());

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

    fn sync_avl(&self, dir: &Path) -> Result<Option<(File, PathBuf, PathBuf)>> {
        let min_k = match Self::get_min_node(&self.root) {
            Some(k) => k,
            None => return Ok(None),
        };

        let max_k = match Self::get_max_node(&self.root) {
            Some(k) => k,
            None => return Ok(None),
        };
        let (file, ss_path_tmp, ss_path_final) = create_new_data_file(dir)?;
        let tmp_path_for_err_case = ss_path_tmp.clone();

        // ALSO TODO: Anticipate errors, clean up the files if we err
        // return DbErr(SyncFail(DbErr, path)) explicitly
        // TODO LATER: Have a Manifest file that just keeps track of what files are active and if a file isnt in the Manifest it gets deleted.
        (|| -> Result<Option<(File, PathBuf, PathBuf)>> {
            // TODO: Can also put in a function
            let mut writer = BufWriter::new(file); // TODO: file already exists, rotate_memtable_and_wal creates it,
            //
            let mut data_block: Option<SsTableDataBlock> = None;

            // Also TODO: see if you can use SstFinalizer here

            // sizeof(key) | key | offset | datablock block length ( before CRC )
            let mut sparse_index = SparseIndex::new();
            let mut bloom_filter = BloomFilter::new(self.size as usize * 10);
            //TODO: If there is no min k or max k, that means the AVL is empty. Not an error, should return None.
            // None means there is nothing to sync and we just skip

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
            );

            // before we write here: we need to get a CRC for the sparse_index and the bloom_filter

            let footer_crc = compute_crc_data_block(&footer);
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
    Success(SSTable),
    Error(DbError),
}
struct FlushingManager {
    tx: Sender<FlushingThreadResponse>,
    rx: Receiver<FlushingThreadResponse>,
}

impl FlushingManager {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel::<FlushingThreadResponse>();
        Self { tx, rx }
    }

    // main will poll and on success, will add the SST to active memory and delete old_wal from directory
    fn background_flush_memtable(&mut self, frozen: Arc<AVL>, dir: PathBuf) -> Result<()> {
        let tx: Sender<FlushingThreadResponse> = self.tx.clone();

        spawn(move || -> Result<()> {
            let (f, ss_path_final) = match frozen.sync_avl(&dir) {
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
                    let _ = tx.send(FlushingThreadResponse::Error(DbError::SyncFail(
                        Box::new(*err),
                        path.to_path_buf(),
                    )));
                    return Err(DbError::ReportedViaChannel);
                }
                Err(e) => {
                    let _ = tx.send(FlushingThreadResponse::Error(e));
                    return Err(DbError::ReportedViaChannel);
                }
                Ok(None) => {
                    // channel should know
                    return Err(DbError::ReportedViaChannel); // empty AVL, do nothing
                }
            };

            let sstable = SSTable::load(&ss_path_final);
            match sstable {
                Ok(sst) => {
                    let _ = tx.send(FlushingThreadResponse::Success(sst)); // when main receives this, DESTRUCT OLD WAL
                }
                Err(dberr) => {
                    let _ = tx.send(FlushingThreadResponse::Error(dberr));
                    return Err(DbError::ReportedViaChannel);
                }
            }

            Ok(())
        });

        Ok(())
    }

    fn build_avl_from_wal(&mut self, memtable: &mut AVL, path: &PathBuf) -> Result<()> {
        let wal_f = File::open(path)?;

        let file_len = wal_f.metadata()?.len();

        let mut reader = BufReader::new(&wal_f);

        let mut type_of_record: [u8; 1] = [0u8; 1];
        let mut ksz = [0u8; 8];
        let mut tstamp = [0u8; 8];
        let mut vsz = [0u8; 8];
        let mut crc = [0u8; 4];
        let mut pos: u64 = 0;
        let outcome = (|| -> Result<()> {
            while pos < file_len {
                // We should read records up until a truncated record or a corrupted record, then we stop
                reader.read_exact(&mut type_of_record)?; // 1 byte
                let type_tag = type_of_record[0];

                match type_tag {
                    TAG_DELETION => {
                        //  TAG_DELETION handle  [ tstamp(8) | ksz(8) | key(sizeof ksz ) |crc (4 bytes) ]
                        reader.read_exact(&mut tstamp)?;
                        reader.read_exact(&mut ksz)?;

                        let key_size = u64::from_le_bytes(ksz);

                        if key_size > KEY_MAX_BYTES_SIZE || pos + key_size + 21 > file_len {
                            return Err(DbError::DataCorrupted(DataCorruptedErr {
                                offset: pos,
                                file_path: path.to_path_buf(),
                                reason: CorruptionType::Other(format!(
                                    "record size overflow: ksz={key_size}"
                                )),
                            }));
                        }
                        let mut key_buffer = vec![0u8; key_size as usize];

                        reader.read_exact(&mut key_buffer)?;

                        let crc_data_block =
                            [type_of_record.as_slice(), &tstamp, &ksz, &key_buffer].concat();
                        let crc_to_check = compute_crc_data_block(&crc_data_block);

                        reader.read_exact(&mut crc)?;

                        let crc_from_buff = u32::from_le_bytes(crc);
                        if crc_to_check != crc_from_buff {
                            return Err(DbError::DataCorrupted(DataCorruptedErr {
                                offset: pos,
                                file_path: path.to_path_buf(),
                                reason: CorruptionType::CrcMismatch {
                                    expected: crc_to_check,
                                    found: crc_from_buff,
                                },
                            }));
                        }

                        pos = reader.stream_position()?;
                        memtable.delete(&key_buffer, u64::from_le_bytes(tstamp));
                    }
                    TAG_INSERTION => {
                        reader.read_exact(&mut tstamp)?;
                        reader.read_exact(&mut ksz)?;
                        reader.read_exact(&mut vsz)?;
                        let key_size = u64::from_le_bytes(ksz);
                        let val_size = u64::from_le_bytes(vsz);

                        if key_size > KEY_MAX_BYTES_SIZE
                            || val_size > VALUE_MAX_BYTES_SIZE
                            || pos + key_size + val_size + 29 > file_len
                        // 1 + 8 + 8 + 8 + 4 = 29
                        {
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
                        reader.read_exact(&mut key_buffer)?;

                        reader.read_exact(&mut val_buffer)?;

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

                        reader.read_exact(&mut crc)?;

                        let crc_from_buff = u32::from_le_bytes(crc);
                        if crc_to_check != crc_from_buff {
                            return Err(DbError::DataCorrupted(DataCorruptedErr {
                                offset: pos,
                                file_path: path.to_path_buf(),
                                reason: CorruptionType::CrcMismatch {
                                    expected: crc_to_check,
                                    found: crc_from_buff,
                                },
                            }));
                        }
                        pos = reader.stream_position()?;

                        memtable.put(&key_buffer, &val_buffer, u64::from_le_bytes(tstamp)); // PROBLEM: This out call will overwrite the actual timestamp of records

                        // TAG_INSERTION handle tstamp | ksz | vsz | key | value |crc (4 bytes)
                    }
                    _ => {
                        return Err(DbError::DataCorrupted(DataCorruptedErr {
                            offset: pos,
                            file_path: path.to_path_buf(),
                            reason: Other(
                                "Received corrupted record type while retrieving WAL".to_string(),
                            ),
                        }));
                    }
                }
            }
            Ok(())
        })();

        match outcome {
            Ok(()) => {}
            Err(e) => {}
        }
        Ok(())
    }

    fn retrieve_wal_records(&mut self, path: &PathBuf, dir: &PathBuf) -> Result<Option<SSTable>> {
        let mut memtable = AVL::new(MEMTABLE_THRESHOLD);
        self.build_avl_from_wal(&mut memtable, path)?;

        let (f, _, ss_final_path) = match memtable.sync_avl(dir) {
            Ok(Some((f, tmp_file, ss_final_path))) => {
                if let Some(dir) = ss_final_path.parent() {
                    // always should have parent
                    File::open(dir)?.sync_all()?;
                }
                (f, tmp_file, ss_final_path)
            }
            Err(DbError::SyncFail(err, path)) => {
                let _ = fs::remove_file(&path);

                return Err(DbError::SyncFail(Box::new(*err), path.to_path_buf()));
            }

            Err(err) => {
                return Err(err);
            }
            Ok(None) => return Ok(None),
        };
        let sstable = SSTable::load(&ss_final_path)?;

        Ok(Some(sstable))
    }
}
struct KVEngine {
    data_directory: PathBuf, // data_directory now holds all .sst and .wal files
    // TODO: Need a way to split ssts into levels, L0, L1, L2 ..
    // Could be done with a manifest file that keeps metadata, but I can just do an easier way for now
    sstables: Option<Arc<RwLock<Vec<SSTable>>>>,
    sync_config: SyncConfig,
    wal: WAL,
    frozen_wal: Option<WAL>, // TODO: eventually there can be multiple of these
    memtable: AVL,           // ,multiples here too
    flushing_memtable: Option<Weak<AVL>>, // and here
    corrupted_files: HashSet<FileId>,
    flushing_manager: FlushingManager,
}

impl KVEngine {
    // threshold and sync_config can be part of one config struct later.
    fn open(dir_name: &Path, sync_config: SyncConfig) -> Result<KVEngine> {
        // TODO: Put the actual directory somewhere specific not in the working dir
        let path = PathBuf::from(dir_name);

        let mut sstables: Vec<SSTable> = Vec::new();
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
                    _ => {}
                },
                _ => continue,
            };
        }
        let wal = WAL::new(MEMTABLE_THRESHOLD, sync_config, &path)?;
        // IMPORTANT: The new wal is created after we check the actual directory for wal files.
        // This is important because we do not want to call retrieve_wal_records() on the new empty wal

        let mut self_instance = Self {
            sstables: None,
            data_directory: path,
            sync_config,
            memtable,
            flushing_memtable: None,
            wal,
            frozen_wal: None,
            flushing_manager: FlushingManager::new(),
            corrupted_files: HashSet::new(),
        };

        for path in sst_vec {
            match SSTable::load(&path) {
                Ok(s) => sstables.push(s),
                Err(e) => {}
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
            if let Some(ss) = self_instance
                .flushing_manager
                .retrieve_wal_records(&path, &self_instance.data_directory)?
            {
                sstables.push(ss);
            }
        }

        sstables.sort_by_key(|p| Reverse(p.id)); // Descending order

        self_instance.sstables = Some(Arc::new(RwLock::new(sstables)));
        Ok(self_instance)
    }

    fn should_search_sstable_file(key: &[u8], sstable: &SSTable) -> bool {
        if key > sstable.max_key.as_slice() || key < sstable.min_key.as_slice() {
            return false;
        }
        let bf_bit_positions =
            get_hashed_key_positions(key, sstable.bloom_filter.num_bits as usize);
        sstable.bloom_filter.check_bits(bf_bit_positions)
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
        // just read the entire data_buffer instead of using a BufReader
        reader.seek(SeekFrom::Start(offset))?;

        reader.read_exact(&mut data_buffer)?;

        //
        // we read CRC here because data_len above doesnt take into account the 4 bytes for crc

        reader.read_exact(&mut crc)?;
        let crc_from_buff = u32::from_le_bytes(crc);

        let fresh_crc = compute_crc_data_block(&data_buffer);

        if fresh_crc != crc_from_buff {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset,
                file_path: sstable.file_path.clone(),
                reason: CorruptionType::CrcMismatch {
                    expected: crc_from_buff,
                    found: fresh_crc,
                },
            }));
        }

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
                Ordering::Less => {
                    pos = val_end;
                    continue;
                }
                Ordering::Equal => {
                    if deleted == 0xFF {
                        return Ok(Deleted);
                    }
                    return Ok(Found(value.to_vec()));
                }
                Ordering::Greater => break,
            }
        }
        Ok(Absent)
    }
    fn search_for_kv_in_sstables(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(sstables) = &self.sstables {
            // Lock here is held for the entirety of the loop. Ok for now, mostly reads, rare writes
            for element in sstables.read().unwrap().iter() {
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
        Ok(None)
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let flushing = self.flushing_memtable.as_ref().and_then(|x| x.upgrade());

        match self.memtable.get(key) {
            Found(bytes) => return Ok(Some(bytes.to_vec())),
            Deleted => return Ok(None),
            Absent => {} // fall through
        }
        if let Some(frozen_mem) = flushing.as_ref() {
            match frozen_mem.get(key) {
                Found(bytes) => return Ok(Some(bytes.to_vec())),
                Deleted => return Ok(None),
                Absent => {}
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
        let tstamp = new_timestamp();
        self.wal
            .record_to_wal(WalRecordType::Insertion(key, value), tstamp)?;

        self.memtable.put(key, value, tstamp);

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        let k_len = key.len() as u64;
        self.memtable.exceeds_max(k_len, 0)?;
        if (k_len + self.memtable.size_in_bytes) > self.memtable.threshold {
            self.rotate_memtable_and_wal()?;
        }
        let tstamp = new_timestamp();
        self.wal
            .record_to_wal(WalRecordType::Deletion(key), tstamp)?;
        self.memtable.delete(key, tstamp);

        Ok(())
    }

    fn rotate_memtable_and_wal(&mut self) -> Result<()> {
        let old_wal = std::mem::replace(
            &mut self.wal,
            WAL::new(MEMTABLE_THRESHOLD, self.sync_config, &self.data_directory)?,
        );
        // WHEN MAIN(whoever polls it) RECEIVES A SUCCESSFUL FLUSH, REMOVE THE OLD WAL ASSOCIATED WITH THAT FLUSH
        let frozen = Arc::new(std::mem::replace(
            &mut self.memtable,
            AVL::new(MEMTABLE_THRESHOLD),
        ));

        self.flushing_memtable = Some(Arc::downgrade(&frozen));

        self.frozen_wal = Some(old_wal);

        self.flushing_manager
            .background_flush_memtable(frozen, self.data_directory.clone())?;

        Ok(())
    }
}

/*Notes:
 // footer is : sparse_index | bloom_filter | min key | max key |  sparse_index_offset| sizeof(sparse_index) | sizeof(bloom_filter) | sizeof(minkey) | sizeof(maxkey) | sparse_crc(4 bytes) | bloom_crc(4 bytes) | metadata_crc(4 bytes) |
 //TODO: need a CRC for the metadata
DataBlocks:  [ tstamp(8) | ksz(8) | value_sz(8) | key | value  tstamp(8) | ksz(8) | value_sz(8) | key | value ... crc(4)]
SSTable: Datablock1 | DataBlock2 ... Datablock N | Footer
Bloom filter: k-hash bit array per SSTable to skip files on negative lookups. Use 10 bits per key. Built during flush of AVL.
*/
// SparseIndex => [ firskey:[offset, datablock_length] ]
// wal record looks like: ksz, vsz, k, v, crc(4 bytes)
// When you read a data block in the sparse index, remember to account for the 4 crc bytes yourself, they are not accounted forin the length
/*






 For WAL records, we have deletion and insertion types so far. Will use one byte to define type. 00000100(4) = INSERTION. 00000010(2) = DELETION.
 serialized should look like this: TYPE | RECORD
 WAL RECORD can be tstamp | ksz | key |crc (4 bytes) OR it can be  | tstamp | ksz |vsz | key | value | crc(4 bytes)
 POTENTIAL PROBLEM: should I include sequence numbers for each k/v pair ?
 PROBLEM/UPDATE: make Bufreaders with capacity instead
 TODO: Modularize the components into their own files
 TODO.1: Document how things work

 TODO(IMPORTANT): Add crc to the footer metadata as well
 // ALSO TODO(done): Make sure everything gets serialized as u64 and dont use usize
 //TODO: have a crc for bloom_filter, sparse_index, and the metadata.
 //

*/
