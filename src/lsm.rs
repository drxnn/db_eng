use crc::{CRC_32_ISO_HDLC, Crc};

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::mem;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crate::helpers::{
    NUM_HASHES, compute_crc, compute_crc_data_block, get_hashed_key_positions, new_timestamp,
};
use std::cmp::{Ordering, max};

const MAX_FILE_SIZE: u64 = 4 * 1024 * 1024; // SUBJECT TO CHANGE
const MEMTABLE_THRESHOLD: u64 = 4 * 1024 * 1024; // SUBJECT TO CHANGE
const DATA_BLOCK: u16 = 8 * 1024; // Data block in SSTable

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
    size: u16,
    starting_key: Vec<u8>,
}

impl SsTableDataBlock {
    fn new() {
        unimplemented!();
    }
    fn append_to_block(&self) {
        unimplemented!();
    }
}

struct SSTable {
    id: u64,
    file: File,
    file_path: PathBuf,
    file_size: u64,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    sparse_index: Vec<(Vec<u8>, u64, u64)>, // key | offset | datablock block length ( before CRC, which means you need to read the next 4 bytes and compute the crc)
    bloom_filter: BloomFilter,
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
                        // if key is equal just return the offset
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

impl AVL {
    fn new(threshold: u64) -> Self {
        Self {
            root: None,
            threshold,
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
    fn serialize_sstable_metadata() {
        unimplemented!();
    }

    fn build_bloom_filter_on_flush() {
        unimplemented!();
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
}

impl KVEngine {
    fn new_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    fn create_new_data_file(dir: &Path, tstamp: u64) -> io::Result<(File, PathBuf)> {
        let data_file_path = dir.join(format!("{}.data", tstamp));
        let data_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&data_file_path)?;
        Ok((data_file, data_file_path))
    }

    // threshold and sync_config can be part of one config struct later.
    fn open(dir_name: &Path, sync_config: SyncConfig, threshold: u64) -> io::Result<KVEngine> {
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
        if let Ok(wal_m) = wal_path.metadata()
            && wal_m.len() > 0
        {

            // flush to disk
            // TODO
        }

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
        };

        if let Some(active_sstable) = sstables.pop() {
            let ss_metadata = active_sstable.file.metadata()?;
            if ss_metadata.len() >= MAX_FILE_SIZE {
                self_instance.rotate_active_file()?;
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
        let bf_bit_positions = get_hashed_key_positions(key);
        bf_bit_positions
            .iter()
            .all(|&pos| *sstable.bloom_filter.bits.get(pos as usize).unwrap_or(&0) != 0)
    }

    //[ tstamp(8) | ksz(8) | value_sz(8) | key | value  tstamp(8) | ksz(8) | value_sz(8) | key | value ... crc(4)]
    fn search_for_kv_in_sstables(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        if let Some(sstables) = &self.sstables {
            for curr_sstable in sstables.iter() {
                match Self::should_search_sstable_file(key, curr_sstable) {
                    true => {
                        let Some((offset, data_len)) = curr_sstable.binary_search_sparse_index(key)
                        else {
                            continue;
                        };

                        let mut data_buffer = vec![0u8; data_len as usize];
                        let mut crc = [0u8; 4];

                        let mut reader = BufReader::new(&curr_sstable.file);
                        reader.seek(SeekFrom::Start(offset))?;

                        reader.read_exact(&mut data_buffer)?;
                        reader.read_exact(&mut crc)?;
                        let crc_from_buff = u32::from_le_bytes(crc);

                        let fresh_crc = compute_crc_data_block(&data_buffer);

                        if fresh_crc != crc_from_buff {
                            break; // 
                            //
                            // NEED TO RETURN ERROR HERE
                            // ALSO REMOVE FILE ITS CORRUPTED
                            //
                        }

                        let mut data_reader: &[u8] = &data_buffer;
                        let mut timestamp = [0u8; 8];
                        let mut key_size = [0u8; 8];
                        let mut value_size = [0u8; 8];

                        let mut pos = 0;
                        while pos < data_buffer.len() {
                            if pos + 24 > data_buffer.len() {
                                // ERROR SOMETHING WENT WRONG
                                // THROW SS TABLE OUT
                            }
                            //[ tstamp(8) | ksz(8) | value_sz(8) | key | value  tstamp(8) | ksz(8) | value_sz(8) | key | value ... crc(4)]
                            //
                            let ksz = u64::from_le_bytes(
                                data_buffer[pos + 8..pos + 16].try_into().unwrap(),
                            ) as usize;
                            let vsz = u64::from_le_bytes(
                                data_buffer[pos + 16..pos + 24].try_into().unwrap(),
                            ) as usize;

                            let key_start = pos + 24;
                            let val_start = pos + 24 + ksz;
                            let val_end = val_start + vsz;
                            let curr_key = &data_buffer[key_start..val_start];
                            let value: &[u8] = &data_buffer[val_start..val_end];

                            match curr_key.cmp(key) {
                                Ordering::Less => {
                                    pos = val_end;
                                    continue;
                                }
                                Ordering::Equal => {
                                    // here you also need to check whether the value was deleted
                                    // so check if value size == 0, or add an actual tombstone(1 byte);
                                    if vsz == 0 {
                                        // return that this has been deleted. But change deleted to have an actual flag(1 byte).
                                    }
                                    return Ok(Some(value.to_vec()));
                                }
                                Ordering::Greater => break,
                            }
                            // match
                        }
                    }
                    false => continue,
                }
            }
        };
        Ok(None)
    }

    fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
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
    fn binary_search_sstable() -> Option<Vec<u8>> {
        unimplemented!();
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
        let tstamp = KVEngine::new_timestamp();
        let new_data_file_tuple = KVEngine::create_new_data_file(&self.data_directory, tstamp)?;

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
Footer: [ sparse_index(sizeof sio) | bloom_filter(sizeof bfo)|sparse_index_offset(8) | bloom_filter_offset(8) | min_key(8) | max_key(8)]
DataBlocks:  [ tstamp(8) | ksz(8) | value_sz(8) | key | value  tstamp(8) | ksz(8) | value_sz(8) | key | value ... crc(4)]
SSTable: Datablock1 | DataBlock2 ... Datablock N | Footer
Bloom filter: k-hash bit array per SSTable to skip files on negative lookups. Use 10 bits per key. Built during flush of AVL.
*/
// SparseIndex => [firskey:offset]
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


*/
