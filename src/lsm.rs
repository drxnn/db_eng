use crc::{CRC_32_ISO_HDLC, Crc};

use core::num;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions, remove_file};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};
use std::sync::{Weak, mpsc};
use std::thread::spawn;
use std::unimplemented;

use crate::errors::CorruptionType::Other;
use crate::errors::{CorruptionType, DataCorruptedErr, DbError, Result};
use crate::helpers::{
    NUM_HASHES, compute_crc, compute_crc_data_block, get_hashed_key_positions, new_timestamp,
};
use std::cmp::{Ordering, max};

const MAX_FILE_SIZE: u64 = 4 * 1024 * 1024; // SUBJECT TO CHANGE
const MEMTABLE_THRESHOLD: u64 = 4 * 1024 * 1024; // SUBJECT TO CHANGE
const DATA_BLOCK: u16 = 8 * 1024; // Data block in SSTable
const MAX_BLOCK_SIZE: u64 = 1024 * 1024;
const TAG_DELETION: u8 = 2;
const TAG_INSERTION: u8 = 4;
const KEY_MAX_BYTES_SIZE: u64 = 16384;
const VALUE_MAX_BYTES_SIZE: u64 = 131072;

// WAL config for flush

#[derive(Copy, Clone)]
enum SyncConfig {
    None,       // fast, data can be lost
    Every(u64), // in ms
    Always,     // Ddurable
}

struct BloomFilter {
    bits: Vec<u64>,
    num_bits: u64,
}

enum WalRecordType<'a> {
    Deletion(&'a [u8]),            // ( key )
    Insertion(&'a [u8], &'a [u8]), // (key, value)
}

struct SparseIndex {
    index_entries: Vec<Vec<u8>>, // PROBLEM: Rethink this really fast, doesnt need to be a Vector of Vectors just push bytes into it
    size: u64,
}

impl SparseIndex {
    fn new() -> Self {
        Self {
            index_entries: Vec::new(),
            size: 0,
        }
    }
    fn add_entry(&mut self, ss_data_block: SsTableDataBlock, offset: u64) {
        let full_block = ss_data_block.full_data_block();
        let first_keysz = (full_block.starting_key.len() as u64).to_le_bytes();
        let data_block_sz = (full_block.bytes.len() as u64).to_le_bytes();
        let mut sparse_entry: Vec<u8> = Vec::new();
        // sparse index: sizeof(k), k, offset, datablock_size);
        sparse_entry.extend_from_slice(&first_keysz);
        sparse_entry.extend_from_slice(&full_block.starting_key);
        sparse_entry.extend_from_slice(&offset.to_le_bytes());
        sparse_entry.extend_from_slice(&data_block_sz);
        self.index_entries.push(sparse_entry);
    }

    fn parse_sparse_index(b: &[u8]) -> Vec<(Vec<u8>, u64, u64)> {
        let mut out = Vec::new();
        // I am parsing this layout: ksz(8) | key(ksz) | offset(8) | datablock_sz(8)
        // to essentially => key | offset | datablock_sz (this lives in memory, the sparseIndex needs key to binary search. meanwhile the sparseIndex in the metadatafooter does need the key size)

        let mut current = 0;
        while current < b.len() {
            let ksz = u64::from_le_bytes(b[current..(current + 8)].try_into().unwrap());
            current += 8;
            let key = b[current..(current + (ksz as usize))].to_vec();
            current += (ksz as usize);
            let offset = u64::from_le_bytes(b[current..(current + 8)].try_into().unwrap());
            current += 8;
            let data_block_size = u64::from_le_bytes(b[current..(current + 8)].try_into().unwrap());
            current += 8;
            out.push((key, offset, data_block_size));
        }
        out
    }
}

impl BloomFilter {
    fn new(num_bits: usize) -> Self {
        let words_for_bits = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; words_for_bits],
            num_bits: num_bits as u64,
        }
    }

    fn set_bits(&mut self, positons: [usize; NUM_HASHES]) {
        for position in positons {
            let word_idx = position / 64;
            let bit_idx = position % 64;

            self.bits[word_idx] |= 1u64 << bit_idx; // shift the bit to the left by bit_idx positions and thats our mask. mask OR curr_u64 = done
        }
    }

    fn check_bits(&self, positons: [usize; NUM_HASHES]) -> bool {
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
    fn new(threshold: u64, sync_c: SyncConfig) -> io::Result<WAL> {
        let tstamp = new_timestamp();
        let wal_path = PathBuf::from(format!("{}.wal", tstamp));
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
        remove_file(&self.path)?;
        Ok(())
    }

    // PROBLEM: Right now we sync_all for every single record, make sure you use SyncConfig later on for deciding
    fn record_to_wal<'a>(&mut self, record: WalRecordType<'a>) -> Result<()> {
        let record_buffer = &mut self.record_buffer;
        record_buffer.clear();
        let tstamp = new_timestamp();

        match record {
            WalRecordType::Deletion(k) => {
                record_buffer.extend_from_slice(&TAG_DELETION.to_le_bytes());
                record_buffer.extend_from_slice(&tstamp.to_le_bytes());
                record_buffer.extend_from_slice(&(k.len() as u64).to_le_bytes());
                record_buffer.extend_from_slice(k);
            }
            WalRecordType::Insertion(k, v) => {
                record_buffer.extend_from_slice(&TAG_INSERTION.to_le_bytes());
                record_buffer.extend_from_slice(&tstamp.to_le_bytes());
                record_buffer.extend_from_slice(&(k.len() as u64).to_le_bytes());
                record_buffer.extend_from_slice(&(v.len() as u64).to_le_bytes());
                record_buffer.extend_from_slice(k);
                record_buffer.extend_from_slice(v);
            }
        }

        let crc = compute_crc_data_block(&record_buffer);
        record_buffer.extend_from_slice(&crc.to_le_bytes());

        if let Some(writer) = self.wal_writer.as_mut() {
            writer.write_all(&record_buffer)?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        Ok(())
    }
}

struct SsTableDataBlock {
    bytes: Vec<u8>, //[ tstamp(8) | ksz(8) | value_sz(8) | key | value ] ... crc(4) (crc for the entire datablock);
    size: usize,
    starting_key: Vec<u8>,
}

impl SsTableDataBlock {
    fn new(s_key: &[u8]) -> Self {
        // creates SsTableDataBlock
        Self {
            bytes: Vec::new(),
            size: 0,
            starting_key: s_key.to_vec(),
        }
    }
    fn append_to_block(&mut self, entry: &[u8]) {
        self.bytes.extend_from_slice(entry);
        self.size += entry.len();
    }

    fn is_finished(&self) -> bool {
        self.size > DATA_BLOCK as usize
    }

    fn full_data_block(mut self) -> Self {
        let crc = compute_crc_data_block(&self.bytes);
        self.bytes.extend_from_slice(&crc.to_le_bytes());
        self
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
    sparse_index: Vec<(Vec<u8>, u64, u64)>, // keysz | offset | datablock block length ( before CRC, which means you need to read the next 4 bytes and compute the crc)
    bloom_filter: BloomFilter,
    corrupted: bool,
}

impl SSTable {
    // pass a path, reads footer of file and builds an SStable to have in memory for faster lookup
    fn load(path: &Path) -> Result<Self> {
        // open reader of file
        // start reading backwards and return the metadata in a SST
        //// footer is :
        // sparse_index | bloom_filter | min key | max key | sizeof(sparse_index) | sparse_index_offset| sizeof(bloom_filter) | bloom filter_offset | sizeof(minkey) | minkey offset | sizeof(maxkey) | maxkey offset | 64 bytes(not including min and max key)
        let mut f = File::open(path)?;
        let id = path
            .file_stem()
            .and_then(|x| x.to_str())
            .map(|x| {
                x.parse::<u64>()
                    .expect("Expected id to be convertable to u64")
            })
            .unwrap();

        f.seek(SeekFrom::End(-40))?;
        let mut footer = [0u8; 40];
        f.read_exact(&mut footer)?;
        let file_length = f.metadata()?.len();
        // get data lengths from footer, and offsets
        // then read all the data you need to one buffer, then slice into it for each value
        // this can inside a deserialize_footer function instead of here
        //PROBLEM: make sure sizes are safe, could be corrupted data.
        // PROBLEM 1.1: I dont need to save everything in the footer. For example, bloom filter offset can be found by doing sparse_i_offset + sparse_i_size
        // footer is: sparse_index | bloom_f | min_k | max_k | (footer starts here -> ) sparse_index_offset | sparse_index_size | bloom_filter_size | min_k size | max_k_size
        // 40 bytes instead of 64
        let sparse_index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let size_of_sparse_index = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let size_of_bloom_filter = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let size_of_min_key = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        let size_of_max_key = u64::from_le_bytes(footer[32..40].try_into().unwrap());

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

        let sparse_index: &[u8] = &full_sst_data[0..(size_of_sparse_index as usize)];
        let bloom_filter =
            &full_sst_data[(bloom_filter_start as usize)..(bloom_filter_end as usize)];
        let min_key = &full_sst_data[(min_k_start as usize)..(min_k_end as usize)];
        let max_k = &full_sst_data[(max_k_start as usize)..(max_k_end as usize)];

        let bloomf_filter_64 = bloom_filter
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect();

        let parsed_sparse_index = SparseIndex::parse_sparse_index(sparse_index);
        Ok(SSTable {
            id,
            file: f,
            file_path: path.to_path_buf(),
            file_size: file_length,
            min_key: min_key.to_vec(),
            max_key: max_k.to_vec(),
            sparse_index: parsed_sparse_index,
            bloom_filter: BloomFilter {
                bits: bloomf_filter_64,
                num_bits: (size_of_bloom_filter / 8),
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
struct AVL {
    root: Option<Box<Node>>,
    threshold: u64,
    size: u64,
    buf_file: Option<BufWriter<File>>, // to write to sstable on flush
}
#[derive(PartialEq, Clone, Debug)]
struct AvlEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    deleted: bool,
}
#[derive(PartialEq, Clone, Debug)]
struct Node {
    entry: AvlEntry,
    height: u64,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
}

impl Node {
    fn serialize_kv(&self) -> Vec<u8> {
        // return [ tstamp(8) | ksz(8) | value_sz(8) | tombstone | key | value |  ]
        let tstamp = new_timestamp().to_le_bytes();
        let ksz = self.entry.key.len().to_le_bytes();
        let vsz = self.entry.value.len().to_le_bytes();
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
            buf_file: None,
        }
    }

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        if let Some(mut curr) = self.root.as_ref() {
            loop {
                if curr.entry.key == key {
                    return Some(&curr.entry.value);
                }
                if curr.entry.key.as_slice() > key {
                    curr = curr.left.as_ref()?;
                } else {
                    curr = curr.right.as_ref()?;
                }
            }
        } else {
            None
        }
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
        node.height = 1 + max(left_height, right_height) as u64;
    }
    fn insert(&mut self, curr: Option<Box<Node>>, n: Node) -> Option<Box<Node>> {
        if let Some(mut node) = curr {
            if n.entry.key == node.entry.key {
                node.entry.value = n.entry.value;
                node.entry.deleted = n.entry.deleted;
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
            Some(Box::new(n))
        }
    }
    /*

    */

    fn put(&mut self, key: &[u8], value: &[u8]) {
        let n = Node {
            entry: AvlEntry {
                key: key.to_vec(),
                value: value.to_vec(),
                deleted: false,
            },
            height: 0,
            left: None,
            right: None,
        };
        let root = self.root.take();
        self.root = self.insert(root, n);
        self.size += 1;
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

    fn delete(&mut self, key: &[u8]) {
        let node = Node {
            entry: AvlEntry {
                key: key.to_vec(),
                value: Vec::new(),
                deleted: true,
            },
            height: 1,
            left: None,
            right: None,
        };

        let root = self.root.take();
        self.root = self.insert(root, node);
    }
    fn delete_remove_node(&mut self, curr: Option<Box<Node>>, key: &[u8]) -> Option<Box<Node>> {
        if let Some(mut node) = curr {
            if node.entry.key == key {
                if node.left.is_none() && node.right.is_none() {
                    return None;
                } else if node.right.is_some() != node.left.is_some() {
                    // XOR
                    // return the child
                    if let Some(_x) = node.left.as_ref() {
                        return node.left;
                    } else {
                        return node.right;
                    }
                } else {
                    // safe to unwrap here
                    let (successor, new_right) = Self::take_min(node.right.take().unwrap());
                    {
                        let succ = successor.unwrap();
                        node.right = new_right;
                        node.entry.value = succ.entry.value;
                        node.entry.key = succ.entry.key;
                    }
                }
                return Some(Self::balance(node));
            }

            if node.entry.key.as_slice() < key {
                node.right = self.delete_remove_node(node.right.take(), key);
            } else {
                node.left = self.delete_remove_node(node.left.take(), key);
            }
            Some(Self::balance(node))
        } else {
            curr
        }
    }

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

    fn serialize_sstable_footer(
        offset: &mut u64,
        min_key: &[u8],
        max_key: &[u8],
        sizeof_si: u64,
        sizeof_bf: u64,
    ) -> Vec<u8> {
        // returns the footer
        // | min key | max key | sizeof(sparse_index) | sparse_index_offset| sizeof(bloom_filter) | bloom filter_offset | sizeof(minkey) | minkey offset | sizeof(maxkey) | maxkey offset |
        // PROBLEM: Change the footer to be 40 bytes like the fn load reads it
        let mut footer: Vec<u8> = Vec::new();
        // offset is start_of_sparse_index

        // TODO: I dont need to save all the offsets necessarily in the footer. They can mostly be derived from other offsets = si_offset + size
        // Come up with a more space efficient footer. Works for now
        let min_key_offset = *offset + sizeof_bf + sizeof_si;
        let max_key_offset = min_key_offset + max_key.len() as u64;
        footer.extend_from_slice(min_key);
        footer.extend_from_slice(max_key);
        footer.extend_from_slice(&sizeof_si.to_le_bytes());
        footer.extend_from_slice(&offset.to_le_bytes());
        *offset += sizeof_si;
        footer.extend_from_slice(&sizeof_bf.to_le_bytes());
        footer.extend_from_slice(&offset.to_le_bytes()); //bf offset
        footer.extend_from_slice(&(min_key.len() as u64).to_le_bytes());

        footer.extend_from_slice(&min_key_offset.to_le_bytes());
        footer.extend_from_slice(&(max_key.len() as u64).to_le_bytes());
        footer.extend_from_slice(&max_key_offset.to_le_bytes());

        // serialize the footer, sparse_index
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

                        writer.write_all(&owned_ss_data_block.bytes)?;

                        let data_block_len = owned_ss_data_block.bytes.len() as u64;
                        sparse_index.add_entry(owned_ss_data_block, *offset);
                        *offset += data_block_len;

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

    // What if engine crashes mid sync_avl execution? // check if need to be called on start/restart

    fn sync_avl(&self, ss_path_tmp: &Path, ss_path_final: &Path) -> Result<File> {
        let mut writer_1 = BufWriter::new(File::create(ss_path_tmp)?);
        let mut data_block: Option<SsTableDataBlock> = None;

        // sizeof(key) | key | offset | datablock block length ( before CRC )
        let mut sparse_index = SparseIndex::new();
        let mut bloom_filter = BloomFilter::new(self.size as usize * 10);

        let min_k = Self::get_min_node(&self.root).ok_or_else(|| {
            DbError::MissingKey("Min key missing in memtable during flushing operation".to_string())
        })?;
        let max_k = Self::get_max_node(&self.root).ok_or_else(|| {
            DbError::MissingKey("Max key missing in memtable during flushing operation".to_string())
        })?;

        let mut file_offset: u64 = 0;
        self.build_sstable_recursive(
            &mut writer_1,
            &self.root,
            &mut bloom_filter,
            &mut data_block,
            &mut sparse_index,
            &mut file_offset,
        )?;

        if let Some(last_db) = data_block {
            let len = last_db.bytes.len() as u64;

            writer_1.write_all(&last_db.bytes)?;

            sparse_index.add_entry(last_db, file_offset);

            file_offset += len; // length here is the start of sparse_index // 
        }
        let footer = Self::serialize_sstable_footer(
            &mut file_offset,
            min_k,
            max_k,
            sparse_index.size,
            bloom_filter.num_bits,
        );

        for entry in &sparse_index.index_entries {
            writer_1.write_all(entry)?;
        }
        for word in &bloom_filter.bits {
            writer_1.write_all(&word.to_le_bytes())?;
        }
        writer_1.write_all(&footer)?;

        let f = writer_1.into_inner().map_err(|e| {
            DbError::FileError(
                format!("Failed to extract File from BufWriter: {}", e.error()),
                ss_path_tmp.to_path_buf(),
            )
        })?;
        f.sync_all()?;

        fs::rename(ss_path_tmp, ss_path_final)?;
        if let Some(dir) = ss_path_final.parent() {
            // always should have parent
            File::open(dir)?.sync_all()?;
        }

        Ok(f)
    }
}

pub enum FlushingThreadResponse {
    Success(SSTable),
    SyncError(DbError),
}
struct FlushingManager {
    tx: Sender<FlushingThreadResponse>,
    rx: Receiver<FlushingThreadResponse>, // make a DbError::FlushError(and variations)
}

impl FlushingManager {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel::<FlushingThreadResponse>();
        Self { tx, rx }
    }

    // main will poll and on success, will add the SST to active memory and delete old_wal from directory
    fn background_flush_memtable(
        &mut self,
        frozen: Arc<AVL>,
        ss_path_tmp: PathBuf,
        ss_path_final: PathBuf,
    ) -> Result<()> {
        // PROBLEM: make sure all potential errors here are handled, no silenced errors
        let tx: Sender<FlushingThreadResponse> = self.tx.clone();
        spawn(move || -> Result<()> {
            let f = match frozen.sync_avl(&ss_path_tmp, &ss_path_final) {
                Ok(f) => f,
                Err(err) => {
                    let _ = tx.send(FlushingThreadResponse::SyncError(err));
                    return Err(DbError::ReportedViaChannel);
                }
            };

            // PROBLEM: ?; on load and handle potential error
            let sstable = SSTable::load(&ss_path_final);

            let _ = tx.send(FlushingThreadResponse::Success(sstable));

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

        while pos < file_len {
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
                    memtable.delete(&key_buffer);
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

                    memtable.put(&key_buffer, &val_buffer);
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
    }
    fn retrieve_wal_records(
        &mut self,
        path: &PathBuf,
        sst_tmp_path: &PathBuf,
        ss_final_path: &PathBuf,
    ) -> Result<()> {
        let mut memtable = AVL::new(MEMTABLE_THRESHOLD);
        self.build_avl_from_wal(&mut memtable, path)?;

        let f = match memtable.sync_avl(sst_tmp_path, ss_final_path) {
            Ok(f) => f,
            Err(err) => {
                // PROBLEM: No error exists for this failure, make it
                return Err(DbError::ReportedViaChannel);
            }
        };
        let sstable = SSTable::load(ss_final_path);
        // Problem: Use ?; when loading and handle potential error
        // Then return the ss table to main to be added to the list

        Ok(())
    }
}
struct KVEngine {
    data_directory: PathBuf,
    sstables: Option<Arc<RwLock<Vec<SSTable>>>>,
    curr_file_buffer: Option<BufWriter<File>>,
    curr_file_path: Option<PathBuf>,
    curr_file_offset: u64,
    sync_config: SyncConfig,
    wal: WAL,
    frozen_wal: Option<WAL>,
    memtable: AVL, // Problem: Might need to put in Arc<> for
    flushing_memtable: Option<Weak<AVL>>,
    corrupted_files: HashSet<FileId>,
    flushing_manager: FlushingManager,
}

impl KVEngine {
    fn create_new_data_file(dir: &Path) -> io::Result<(File, PathBuf, PathBuf)> {
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

    // threshold and sync_config can be part of one config struct later.
    fn open(dir_name: &Path, sync_config: SyncConfig, threshold: u64) -> Result<KVEngine> {
        let path = PathBuf::from(dir_name);

        let mut sstables: Vec<SSTable> = Vec::new();
        let memtable = AVL::new(MEMTABLE_THRESHOLD);

        let wal = WAL::new(threshold, sync_config)?;
        let wal_path: &Path = wal.path.as_ref();
        let wal_path_metadata = wal_path.metadata();

        let mut self_instance = Self {
            sstables: None,
            data_directory: path,
            curr_file_buffer: None,
            curr_file_path: None,
            curr_file_offset: 0,
            sync_config,
            memtable,
            flushing_memtable: None,
            wal,
            frozen_wal: None,
            flushing_manager: FlushingManager::new(),
            corrupted_files: HashSet::new(),
        };

        for entry in fs::read_dir(dir_name)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let ext = match path.extension().and_then(|x| x.to_str()) {
                Some(e) => e,
                _ => continue,
            };
            if ext == "sst" {
                let ss_table = SSTable::load(&path);
                sstables.push(ss_table);
            } else if ext == "wal" {
                // flush old wals to disk
                // TODO
                let (file, tmp_path, final_path) =
                    KVEngine::create_new_data_file(&self_instance.data_directory)?;
                // wal populates this and we flush it to disk as an .sst
                let _ = self_instance.flushing_manager.retrieve_wal_records(
                    &path,
                    &tmp_path,
                    &final_path,
                );
            }
        }

        sstables.sort_by_key(|p| p.id);

        // if let Ok(wal_m) = wal_path_metadata
        //     && wal_m.len() > 0
        // {
        //     self_instance.wal.sync_wal()?;
        // }

        self_instance.sstables = Some(Arc::new(RwLock::new(sstables)));
        Ok(self_instance)
    }

    fn should_search_sstable_file(key: &[u8], sstable: &SSTable) -> bool {
        // checks the metadata of sstable and tells us whether we should look for the kv in the sstable
        if key > sstable.max_key.as_slice() || key < sstable.min_key.as_slice() {
            return false;
        }
        let bf_size = sstable.bloom_filter.bits.len();
        let bf_bit_positions = get_hashed_key_positions(key, bf_size);
        bf_bit_positions
            .iter()
            .all(|&pos| *sstable.bloom_filter.bits.get(pos).unwrap() != 0)
    }

    fn search_kv_in_sstable(sstable: &SSTable, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some((offset, data_len)) = sstable.binary_search_sparse_index(key) else {
            return Ok(None);
        };

        if data_len > MAX_BLOCK_SIZE {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset,
                file_path: sstable.file_path.clone(),
                reason: CorruptionType::BufferExceedsMaxLength {
                    size: data_len,
                    max_size: MAX_BLOCK_SIZE,
                },
            }));
        }
        let mut data_buffer = vec![0u8; data_len as usize];

        let mut crc = [0u8; 4];

        // PROBLEM: Lets switch to read_exact_at here since buffer isnt reused
        let mut reader = BufReader::new(&sstable.file);
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
            let ksz =
                u64::from_le_bytes(data_buffer[pos + 8..pos + 16].try_into().unwrap()) as usize;
            let vsz =
                u64::from_le_bytes(data_buffer[pos + 16..pos + 24].try_into().unwrap()) as usize;
            let deleted = &data_buffer[pos + 24..pos + 25][0];
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
            let curr_key = &data_buffer[key_start..val_start];
            let value: &[u8] = &data_buffer[val_start..val_end];

            match curr_key.cmp(key) {
                Ordering::Less => {
                    pos = val_end;
                    continue;
                }
                Ordering::Equal => {
                    if *deleted == 1 {
                        return Ok(None);
                    }
                    return Ok(Some(value.to_vec()));
                }
                Ordering::Greater => break,
            }
        }
        Ok(None)
    }
    fn search_for_kv_in_sstables(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(sstables) = &self.sstables {
            // Lock here is held for the entirety of the loop. Ok for now, mostly reads, rare writes
            for element in sstables.read().unwrap().iter() {
                match Self::should_search_sstable_file(key, element) {
                    true => {
                        if let Some(sstable) = Self::search_kv_in_sstable(element, key)? {
                            return Ok(Some(sstable));
                        }
                    }
                    false => continue,
                }
            }
        }
        Ok(None)
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let flushing = self.flushing_memtable.as_ref().and_then(|x| x.upgrade());

        let val = flushing
            .as_ref()
            .and_then(|x| x.get(key))
            .or_else(|| self.memtable.get(key));

        if let Some(c) = val {
            Ok(Some(c.to_vec()))
        } else {
            match self.search_for_kv_in_sstables(key)? {
                Some(v) => Ok(Some(v)),
                _ => Ok(None),
            }
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        match (key.len() as u64 + value.len() as u64 + self.memtable.size) < self.memtable.threshold
        {
            true => {
                self.memtable.put(key, value);
            }
            false => {
                self.rotate_memtable_and_wal()?;

                self.memtable.put(key, value);
            }
        }
        self.wal
            .record_to_wal(WalRecordType::Insertion(key, value))?;

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.memtable.delete(key);
        self.wal.record_to_wal(WalRecordType::Deletion(key))?;
        Ok(())
    }

    fn sync_memtable(memtable: AVL) {
        unimplemented!()
    }
    fn sync(&mut self) -> io::Result<()> {
        if let Some(writer) = &mut self.curr_file_buffer {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        Ok(())
    }

    fn rotate_memtable_and_wal(&mut self) -> Result<()> {
        let frozen = Arc::new(std::mem::replace(
            &mut self.memtable,
            AVL::new(MEMTABLE_THRESHOLD),
        ));

        self.flushing_memtable = Some(Arc::downgrade(&frozen));
        let old_wal = std::mem::replace(
            &mut self.wal,
            WAL::new(MEMTABLE_THRESHOLD, self.sync_config)?,
        );
        self.frozen_wal = Some(old_wal);
        let (file, tmp_path, final_path) = KVEngine::create_new_data_file(&self.data_directory)?;

        let _ =
            self.flushing_manager
                .background_flush_memtable(frozen, tmp_path.clone(), final_path);

        Ok(())
    }

    // fn serialize_record(tstamp: u64, key: &[u8], value: &[u8]) -> Vec<u8> {
    //     let crc32 = Crc::<u32>::new(&CRC_32_ISO_HDLC);
    //     let body: Vec<u8> = [
    //         &tstamp.to_le_bytes()[..],
    //         &(key.len() as u64).to_le_bytes(),
    //         &(value.len() as u64).to_le_bytes(),
    //         key,
    //         value,
    //     ]
    //     .concat();
    //     let checksum = crc32.checksum(&body);
    //     let mut record = checksum.to_le_bytes().to_vec();
    //     record.extend(body);
    //     record
    // }
}

/*Notes:
 // footer is :  | min key | max key | sizeof(sparse_index) | sparse_index_offset| sizeof(bloom_filter) | bloom filter_offset | sizeof(minkey) | minkey offset | sizeof(maxkey) | maxkey offset | 64 bytes(not including min and max key)
DataBlocks:  [ tstamp(8) | ksz(8) | value_sz(8) | key | value  tstamp(8) | ksz(8) | value_sz(8) | key | value ... crc(4)]
SSTable: Datablock1 | DataBlock2 ... Datablock N | Footer
Bloom filter: k-hash bit array per SSTable to skip files on negative lookups. Use 10 bits per key. Built during flush of AVL.
*/
// SparseIndex => [ firskey:[offset, datablock_length] ]
// wal record looks like: ksz, vsz, k, v, crc(4 bytes)
// When you read a data block in the sparse index, remember to account for the 4 crc bytes yourself, they are not accounted forin the length
/*
TODOS:
Build SSTables on open to have the metadata in memory.
Need to rewrite the delete function. Right now I am removing the Node from the tree but this can cause a bug:
if you delete a key thats in the memtable, you remove the node, but what if its in one of the SStables?
since the memtable hasnt been flushed yet, you will check memtable -> not found then check ss table and return the value even though
it was deleted.
So instead of removing the node, just add a tombstone on deletes. this means that the tree will just grow and now need to rebalance on deletes.
On delete: just do insert(key, node) and have node.deleted true.

On KVEngine get() you check if a kv is in the memtable, if yes, check the deleted flag.
[ tstamp(8) | ksz(8) | value_sz(8) | deletedflag(1) | key | value ]
deletedflag 1 = deleted, 0 = alive


 For WAL records, we have deletion and insertion types so far. Will use one byte to define type. 00000100(4) = INSERTION. 00000010(2) = DELETION.
 serialized should look like this: TYPE | RECORD
 RECORD can be tstamp | ksz | key |crc (4 bytes) OR it can be  | tstamp | ksz |vsz | key | value | crc(4 bytes)
 POTENTIAL PROBLEM: should I include sequence numbers for each k/v pair ?
 PROBLEM/UPDATE: make Bufreaders with capacity instead
 PROBLEM/OPT: metadata footer can

*/
