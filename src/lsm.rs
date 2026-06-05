use crc::{CRC_32_ISO_HDLC, Crc};

use core::num;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use std::path::{Path, PathBuf};
use std::thread::spawn;

use crate::errors::{CorruptionType, DataCorruptedErr, DbError, Result};
use crate::helpers::{NUM_HASHES, compute_crc_data_block, get_hashed_key_positions, new_timestamp};
use std::cmp::{Ordering, max};

const MAX_FILE_SIZE: u64 = 4 * 1024 * 1024; // SUBJECT TO CHANGE
const MEMTABLE_THRESHOLD: u64 = 4 * 1024 * 1024; // SUBJECT TO CHANGE
const DATA_BLOCK: u16 = 8 * 1024; // Data block in SSTable
const MAX_BLOCK_SIZE: u64 = 1024 * 1024;

// WAL config for flush

#[derive(Copy, Clone)]
enum SyncConfig {
    None,       // fast
    Every(u64), // in ms
    Always,     // Ddurable
}

struct BloomFilter {
    bits: Vec<u64>,
    num_bits: u64,
}

struct SparseIndex {
    index_entries: Vec<Vec<u8>>,
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
        // TODO: Figure out a more cache friendly format here? instead of Vec<Vec<u8>>
        let full_block = ss_data_block.full_data_block();
        let first_keysz = (full_block.starting_key.len() as u64).to_le_bytes();
        let data_block_sz = (full_block.bytes.len() as u64).to_le_bytes();
        let mut sparse_entry: Vec<u8> = Vec::new();
        // sparse index: sizeof(k), k, offset, datablock_size);
        sparse_entry.extend_from_slice(&first_keysz);
        sparse_entry.extend_from_slice(&full_block.starting_key);
        sparse_entry.extend_from_slice(&offset.to_le_bytes());
        sparse_entry.extend_from_slice(&data_block_sz);
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
    threshold: u64,
}

impl WAL {
    fn new(threshold: u64, sync_c: SyncConfig) -> io::Result<WAL> {
        let wal_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open("walfile.wal")?;
        Ok(Self {
            wal_writer: Some(BufWriter::new(wal_file)),
            threshold,
            sync_c,
        })
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
// put the cold data into a SStable cold data vector(sparse index, etc)
struct SSTable {
    id: u64,
    file: File,
    file_path: PathBuf,
    file_size: u64,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    // ! fix sparse index to be keysz | offset | db length
    sparse_index: Vec<(Vec<u8>, u64, u64)>, // keysz | offset | datablock block length ( before CRC, which means you need to read the next 4 bytes and compute the crc)
    bloom_filter: BloomFilter,
    corrupted: bool,
}

impl SSTable {
    // pass a path, reads footer of file and builds an SStable to have in memory for faster lookup
    fn load(path: &Path) -> Self {
        unimplemented!()
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
    value: Vec<u8>,
    deleted: bool,
}
#[derive(PartialEq, Clone, Debug)]
struct Node {
    key: Vec<u8>,
    value: AvlEntry,
    height: u64,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
}

impl Node {
    fn serialize_kv(&self) -> Vec<u8> {
        // return [ tstamp(8) | ksz(8) | value_sz(8) | key | value ]
        let tstamp = new_timestamp().to_le_bytes();
        let ksz = self.key.len().to_le_bytes();
        let vsz = self.value.value.len().to_le_bytes();

        [&tstamp, &ksz, &vsz, self.key.as_slice(), &self.value.value].concat()
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
                if curr.key == key {
                    return Some(&curr.value.value);
                }
                if curr.key.as_slice() > key {
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
            if n.key == node.key {
                node.value = n.value;
                return Some(node);
            }
            if n.key < node.key {
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

    fn put(&mut self, key: &[u8], value: &[u8]) {
        let n = Node {
            key: key.to_vec(),
            value: AvlEntry {
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
            key: key.to_vec(),
            value: AvlEntry {
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
            if node.key == key {
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
                        node.value = succ.value;
                        node.key = succ.key;
                    }
                }
                return Some(Self::balance(node));
            }

            if node.key.as_slice() < key {
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

        Some(&curr.key)
    }
    fn get_max_node(node: &Option<Box<Node>>) -> Option<&Vec<u8>> {
        let mut curr = node.as_ref()?;

        while let Some(n) = curr.right.as_ref() {
            curr = n
        }

        Some(&curr.key)
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

    fn in_order_iter_bf_build(
        &mut self,
        n: &Option<Box<Node>>,
        bf: &mut BloomFilter,
        data_block: &mut Option<SsTableDataBlock>,
        sparse_index: &mut SparseIndex,
        offset: &mut u64,
    ) -> Result<()> {
        if let Some(x) = n {
            self.in_order_iter_bf_build(&x.left, bf, data_block, sparse_index, offset)?;
            if let Some(ss_data_block) = data_block {
                match ss_data_block.is_finished() {
                    true => {
                        let owned_ss_data_block =
                            data_block.take().expect("Expected a SsTableDataBlock");
                        if let Some(writer) = self.buf_file.as_mut() {
                            writer.write_all(&owned_ss_data_block.bytes)?;
                        }
                        let data_block_len = owned_ss_data_block.bytes.len() as u64;
                        sparse_index.add_entry(owned_ss_data_block, *offset);
                        *offset += data_block_len;

                        let mut new_ss_db = SsTableDataBlock::new(&x.key);
                        new_ss_db.append_to_block(&x.serialize_kv());
                        *data_block = Some(new_ss_db);
                    }
                    false => {
                        ss_data_block.append_to_block(&x.serialize_kv());
                    }
                }
            } else {
                let mut new_ss_db = SsTableDataBlock::new(&x.key);
                new_ss_db.append_to_block(&x.serialize_kv());
                *data_block = Some(new_ss_db);
            }
            let positions = get_hashed_key_positions(&x.key, bf.num_bits as usize);
            bf.set_bits(positions);
            self.in_order_iter_bf_build(&x.right, bf, data_block, sparse_index, offset)?;
        }
        Ok(())
    }
    fn sync_avl(&mut self, ss_path: &Path) -> Result<File> {
        // maybe dont pass ss_path and instead create it here, timestamp.sst
        let f = File::create(ss_path)?;
        self.buf_file = Some(BufWriter::new(f));
        let mut data_block: Option<SsTableDataBlock> = None;
        // sizeof(key) | key | offset | datablock block length ( before CRC )
        let mut sparse_index = SparseIndex::new();
        let mut bloom_filter = BloomFilter::new(self.size as usize * 10);

        let root = self.root.take();
        let min_k = Self::get_min_node(&root).ok_or_else(|| {
            DbError::MissingKey("Min key missing in memtable during flushing operation".to_string())
        })?;
        let max_k = Self::get_max_node(&root).ok_or_else(|| {
            DbError::MissingKey("Max key missing in memtable during flushing operation".to_string())
        })?;

        let mut file_offset: u64 = 0;
        self.in_order_iter_bf_build(
            &root,
            &mut bloom_filter,
            &mut data_block,
            &mut sparse_index,
            &mut file_offset,
        )?;

        if let Some(last_db) = data_block {
            let len = last_db.bytes.len() as u64;
            if let Some(writer) = self.buf_file.as_mut() {
                writer.write_all(&last_db.bytes)?;
            }

            sparse_index.add_entry(last_db, file_offset);

            file_offset += len; // length here is the start of sparse_index
            let footer = Self::serialize_sstable_footer(
                &mut file_offset,
                min_k,
                max_k,
                sparse_index.size,
                bloom_filter.num_bits,
            );

            if let Some(writer) = self.buf_file.as_mut() {
                for entry in &sparse_index.index_entries {
                    writer.write_all(entry)?;
                }
                for word in &bloom_filter.bits {
                    writer.write_all(&word.to_le_bytes())?;
                }

                writer.write_all(&footer)?;
                writer.flush()?;
                let f = writer.get_ref();
                f.sync_all()?;
            }
        }

        self.root = root; // returning root here because min_k and max_k above are borrows of root

        let f = self
            .buf_file
            .take()
            .ok_or_else(|| {
                DbError::FileError(
                    "Failed to take File out of BufWriter".to_string(),
                    ss_path.to_path_buf(),
                )
            })?
            .into_inner()
            .map_err(|e| {
                DbError::FileError(
                    format!("Failed to extract File from BufWriter: {}", e.error()),
                    ss_path.to_path_buf(),
                )
            })?;

        Ok(f)
    }
}
struct KVEngine {
    data_directory: PathBuf,
    sstables: Option<Vec<SSTable>>,
    curr_file_buffer: Option<BufWriter<File>>, // have a curr file to be the file you are currently writing on
    curr_file_path: Option<PathBuf>,
    curr_file_offset: u64,
    sync_config: SyncConfig,
    wal: WAL,
    memtable: AVL,
    corrupted_files: HashSet<FileId>,
}

impl KVEngine {
    fn create_new_data_file(dir: &Path) -> io::Result<(File, PathBuf)> {
        let tstamp = new_timestamp();
        let data_file_path = dir.join(format!("{}.sst", tstamp));
        let data_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&data_file_path)?;
        Ok((data_file, data_file_path))
    }

    // threshold and sync_config can be part of one config struct later.
    fn open(dir_name: &Path, sync_config: SyncConfig, threshold: u64) -> Result<KVEngine> {
        let path = PathBuf::from(dir_name);

        let mut sstables: Vec<SSTable> = Vec::new();

        for entry in fs::read_dir(dir_name)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            println!("Name: {}", path.display());

            let ext = match path.extension().and_then(|x| x.to_str()) {
                Some(e) => e,
                _ => continue,
            };
            if ext == "sst" {
                let ss_table = SSTable::load(&path);
                sstables.push(ss_table);
            }
        }

        sstables.sort_by_key(|p| p.id);

        let memtable = AVL::new(MEMTABLE_THRESHOLD);

        let wal_path = dir_name.join("walfile.wal");

        let wal = WAL::new(threshold, sync_config)?;

        let mut self_instance = Self {
            sstables: None,
            data_directory: path,
            curr_file_buffer: None,
            curr_file_path: None,
            curr_file_offset: 0,
            sync_config,
            memtable,
            wal,
            corrupted_files: HashSet::new(),
        };
        if let Ok(wal_m) = wal_path.metadata()
            && wal_m.len() > 0
        {
            self_instance.sync_wal()?;
        }

        if let Some(active_sstable) = sstables.pop() {
            let ss_metadata = active_sstable.file.metadata()?;
            if ss_metadata.len() >= MAX_FILE_SIZE {
                self_instance.rotate_active_file()?; // TODO
            } else {
                self_instance.curr_file_buffer =
                    Some(BufWriter::with_capacity(256000, active_sstable.file));
                self_instance.curr_file_path = Some(active_sstable.file_path.clone());
                self_instance.curr_file_offset = ss_metadata.len();
            }
        } else {
            // first run, no sstables yyet
            self_instance.rotate_active_file()?;
        }

        self_instance.sstables = Some(sstables);
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

    //[ tstamp(8) | ksz(8) | value_sz(8) | key | value  tstamp(8) | ksz(8) | value_sz(8) | key | value ... crc(4)]
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

        let mut reader = BufReader::new(&sstable.file);
        reader.seek(SeekFrom::Start(offset))?;

        reader.read_exact(&mut data_buffer)?;
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
            //[ tstamp(8) | ksz(8) | value_sz(8) | key | value  tstamp(8) | ksz(8) | value_sz(8) | key | value ... crc(4)]
            //
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
    fn search_for_kv_in_sstables(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(sstables) = &self.sstables {
            for element in sstables.iter() {
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
        let val = self.memtable.get(key);
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
        // put into wal, check if avl can hold, put into memtable unless memtable full -> new memtable -> old sync
        match ((key.len() + value.len()) as u64) < self.memtable.size {
            true => {
                self.memtable.put(key, value);
            }
            false => {
                self.rotate_active_file()?; // as of right now, we have to wait for this function to be done before we do a mem.put
                // later we will start a new memtable and have the old one flush in the background
                self.memtable.put(key, value);
            }
        }

        Ok(())
    }

    fn sync_wal(&mut self) -> io::Result<()> {
        unimplemented!()
        // reads wal and writes to disk
        // gets called if a crash occurs, if no crash, judt delete wal safely
    }
    fn sync_memtable(memtable: AVL) {
        unimplemented!()
    }
    fn sync(&mut self) -> io::Result<()> {
        // forces any writes to sync to disk
        if let Some(writer) = &mut self.curr_file_buffer {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        Ok(())
    }

    // rotate active file should change to rotate_memtable_and_wal()
    fn rotate_memtable_and_wal(&mut self) -> Result<()> {
        // start a new memtable
        let mut old_memtable = std::mem::replace(&mut self.memtable, AVL::new(MEMTABLE_THRESHOLD));
        let old_wal = std::mem::replace(
            &mut self.wal,
            WAL::new(MEMTABLE_THRESHOLD, self.sync_config)?,
        );
        let data_dir = self.data_directory.clone();

        // hand memtable to another thread to be flushed, and keep it readable until flush is over(put in Arc)
        let handle = spawn(move || -> Result<File> {
            // walks through old_memtable and write it to a new sstable.

            let new_sst_file_path = KVEngine::create_new_data_file(&data_dir)?.1;

            let f = old_memtable.sync_avl(&new_sst_file_path)?;

            Ok(f)
        });

        // only delete the old wal when everything is flushed to disk.

        Ok(())
    }

    fn rotate_active_file(&mut self) -> io::Result<()> {
        if let Some(writer) = &mut self.curr_file_buffer {
            writer.flush()?;
        }
        if let Some(old_path) = self.curr_file_path.take()
            && let Some(files) = &mut self.sstables
        {
            let sstable = SSTable::load(&old_path);
            files.push(sstable);
        }

        let new_data_file_tuple = KVEngine::create_new_data_file(&self.data_directory)?;

        self.curr_file_buffer = Some(BufWriter::with_capacity(256000, new_data_file_tuple.0));
        self.curr_file_path = Some(new_data_file_tuple.1);
        self.curr_file_offset = 0;

        Ok(())
    }
    fn serialize_record(tstamp: u64, key: &[u8], value: &[u8]) -> Vec<u8> {
        let crc32 = Crc::<u32>::new(&CRC_32_ISO_HDLC);
        let body: Vec<u8> = [
            &tstamp.to_le_bytes()[..],
            &(key.len() as u64).to_le_bytes(),
            &(value.len() as u64).to_le_bytes(),
            key,
            value,
        ]
        .concat();
        let checksum = crc32.checksum(&body);
        let mut record = checksum.to_le_bytes().to_vec();
        record.extend(body);
        record
    }
}

/*Notes:
 // footer is :  | min key | max key | sizeof(sparse_index) | sparse_index_offset| sizeof(bloom_filter) | bloom filter_offset | sizeof(minkey) | minkey offset | sizeof(maxkey) | maxkey offset | 64 bytes(not including min and max key)
DataBlocks:  [ tstamp(8) | ksz(8) | value_sz(8) | key | value  tstamp(8) | ksz(8) | value_sz(8) | key | value ... crc(4)]
SSTable: Datablock1 | DataBlock2 ... Datablock N | Footer
Bloom filter: k-hash bit array per SSTable to skip files on negative lookups. Use 10 bits per key. Built during flush of AVL.
*/
// SparseIndex => [ firskey:[offset, datablock_length] ]

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

*/
