use crc::{CRC_32_ISO_HDLC, Crc};

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::mem;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crate::helpers::{compute_crc, new_timestamp};
use std::cmp::max;

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
    num_hashes: u16,
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
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    sparse_index: Vec<(Vec<u8>, u64)>,
    bloom_filter: BloomFilter,
}

impl SSTable {
    // pass a path, reads footer of file and builds an SStable to have in memory for faster lookup
    fn load(path: &Path) -> Self {
        unimplemented!()
    }
}
struct AVL {
    root: Option<Node>,
    threshold: u64,
}
#[derive(PartialEq, Clone, Debug)]
struct AvlEntry {
    value: Vec<u8>,
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
                //
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
    fn delete(&mut self, curr: Option<Box<Node>>, key: &[u8]) -> Option<Box<Node>> {
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
                node.right = self.delete(node.right.take(), key);
            } else {
                node.left = self.delete(node.left.take(), key);
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

    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        // check AVL tree first, if found, return, otherwise consult the SStables
        unimplemented!()
        // Steps
        /* Check AVL tree -> if there return
        else -> while loop check SS tables, if min/max or bloom filter say negative skip -> otherwise binary search Datablocks
        when found return, or None
        */
        let val = self.memtable.get(key);
        if let Some(c) = val {
            return Some(Ok(c.to_vec()));
        } else {
            // search ss tables
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
/*
TODOS:
Build SSTables on open to have the metadata in memory.
Need to rewrite the delete function. Right now I am removing the Node from the tree but this can cause a bug:
if you delete a key thats in the memtable, you remove the node, but what if its in one of the SStables?
since the memtable hasnt been flushed yet, you will check memtable -> not found then check ss table and return the value even though 
it was deleted.
So instead of removing the node, just add a tombstone on deletes. this means that the tree will just grow and now need to rebalance on deletes.
On delete: just do insert(key, node) and have node.deleted true
On KVEngine get() you check if a kv is in the memtable, if yes, check the deleted flag.



*/
