use crc::{CRC_32_ISO_HDLC, Crc};

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::mem;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crate::helpers::compute_crc;
use std::cmp::max;

mod errors;
mod helpers;
mod lsm;

const MAX_FILE_SIZE: u64 = 4 * 1024 * 1024; // SUBJECT TO CHANGE
const MEMTABLE_THRESHOLD: u64 = 4 * 1024 * 1024; // SUBJECT TO CHANGE
const DATA_BLOCK: u16 = 8 * 1024; // Data block in SSTable
struct KeydirEntry {
    file_id: String, // basically file name "timestamp.data"
    value_sz: u64,
    value_pos: u64,
    tstamp: u64,
}

enum SyncConfig {
    None,       // fast
    Every(u64), // in ms
    Always,     // Ddurable
}

struct AVL {
    root: Option<Node>,
    threshold: u64, // size before we flush it to disk as an sstable file
}
#[derive(PartialEq, Clone, Debug)]
struct AvlEntry {
    value: String,
    deleted: bool,
}
#[derive(PartialEq, Clone, Debug)]
struct Node {
    key: String,
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

    fn get(&self, key: &str) -> Option<&Node> {
        if let Some(mut curr) = self.root.as_ref() {
            loop {
                if curr.key == key {
                    return Some(curr);
                }
                if curr.key.as_str() > key {
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
    fn delete(&mut self, curr: Option<Box<Node>>, key: &str) -> Option<Box<Node>> {
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

            if node.key.as_str() < key {
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
    files: Option<Vec<PathBuf>>,
    key_dir: HashMap<String, KeydirEntry>,
    curr_file: Option<BufWriter<File>>, // have a curr file to be the file you are currently writing on
    curr_file_path: Option<PathBuf>,
    curr_file_offset: u64,
    sync_config: SyncConfig,
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

    fn create_new_hint_file(dir: &Path, tstamp: u64) -> io::Result<(File, PathBuf)> {
        let hint_file_path = dir.join(format!("{}.hint", tstamp));
        let hint_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&hint_file_path)?;
        Ok((hint_file, hint_file_path))
    }

    fn open(dir_name: &Path, sync_config: SyncConfig) -> io::Result<KVEngine> {
        let path = PathBuf::from(dir_name);
        let mut key_dir: HashMap<String, KeydirEntry> = HashMap::new();

        // when open runs, // scan the directory for all the files
        let mut files: Vec<PathBuf> = Vec::new();
        let mut files_for_keydir_rebuild: HashMap<String, (String, PathBuf)> = HashMap::new();

        for entry in fs::read_dir(dir_name)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            println!("Name: {}", path.display());

            let (stem, ext) = match (path.file_stem(), path.extension()) {
                (Some(s), Some(e)) => match (s.to_str(), e.to_str()) {
                    (Some(s), Some(e)) => (s.to_string(), e.to_string()),
                    _ => continue,
                },
                _ => continue,
            };

            match ext.as_str() {
                "hint" => {
                    files_for_keydir_rebuild.insert(stem, ("hint".to_string(), path.clone()));
                }
                "data" => {
                    files_for_keydir_rebuild
                        .entry(stem)
                        .or_insert(("data".to_string(), path.clone()));
                    files.push(path);
                }

                _ => continue,
            }
        }

        // note: instead of full pathbuf, just include the file_id, then add the extension later when needed
        let mut files_for_keydir_rebuild_as_vec: Vec<_> =
            files_for_keydir_rebuild.into_iter().collect();
        files_for_keydir_rebuild_as_vec.sort_by_key(|x| x.0.parse::<u64>().ok());
        files.sort_by_key(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
        });

        for (file_id, (ext, file)) in &files_for_keydir_rebuild_as_vec {
            match ext.as_str() {
                "hint" => {
                    let hint_file_to_read = fs::read(file)?;
                    let hint_file_len = file.metadata()?.len();
                    let data_file_id = file.with_extension("data");

                    let mut cursor = Cursor::new(hint_file_to_read);
                    let mut timestamp = [0u8; 8];
                    let mut key_size = [0u8; 8];
                    let mut value_size = [0u8; 8];
                    let mut data_position = [0u8; 8];
                    while cursor.position() < hint_file_len {
                        //
                        //[ k_size, key, tstamp, value_sz, data_position ]
                        // [  u64,   ksz     u64,     u64,      u64]
                        cursor.read_exact(&mut key_size)?;
                        let key_size_num = u64::from_le_bytes(key_size) as usize;
                        let mut key = vec![0u8; key_size_num];
                        cursor.read_exact(&mut key)?;
                        cursor.read_exact(&mut timestamp)?;
                        cursor.read_exact(&mut value_size)?;
                        cursor.read_exact(&mut data_position)?;
                        let value_position = u64::from_le_bytes(data_position);
                        let val_size_num = u64::from_le_bytes(value_size) as usize;
                        let key_as_str = match str::from_utf8(&key) {
                            // check here is maybe uneccessary
                            Ok(k) => k,
                            Err(_r) => panic!("Invalid UTF8 on key"),
                        };
                        let timestmp = u64::from_le_bytes(timestamp);

                        key_dir.insert(
                            key_as_str.to_string(),
                            KeydirEntry {
                                file_id: data_file_id.display().to_string(),
                                value_sz: val_size_num as u64,
                                value_pos: value_position,
                                tstamp: timestmp,
                            },
                        );
                    }
                }
                "data" => {
                    let file_vec = fs::read(file)?;

                    let file_name = file.to_str().unwrap();
                    let mut cursor = Cursor::new(file_vec);
                    let mut timestamp = [0u8; 8];
                    let mut key_size = [0u8; 8];
                    let mut value_size = [0u8; 8];
                    let mut crc = [0u8; 4];

                    let file_len = file.metadata()?.len();

                    while cursor.position() < file_len {
                        // reading sequential data that looks like:
                        //                      [ crc | tstamp | ksz | value_sz | key | value ]
                        //             sizes:   [ 32b |  64b   | 64b |    64b   | ksz | valuesz ]
                        cursor.read_exact(&mut crc)?;
                        cursor.read_exact(&mut timestamp)?; // put timestamp bytes into our slice
                        cursor.read_exact(&mut key_size)?; // put keysize bytes into our slice, this tells us how many bytes the key is
                        cursor.read_exact(&mut value_size)?; // put valuesize bytes into our slice

                        let key_size_num = u64::from_le_bytes(key_size) as usize;
                        let val_size_num = u64::from_le_bytes(value_size) as usize;

                        let mut key = vec![0u8; key_size_num];
                        let mut value = vec![0u8; val_size_num];

                        cursor.read_exact(&mut key)?;
                        let value_position = cursor.seek(SeekFrom::Current(0))?; // value starts here

                        cursor.read_exact(&mut value)?;

                        let key_as_str = match str::from_utf8(&key) {
                            // check here is maybe uneccessary
                            Ok(k) => k,
                            Err(_r) => panic!("Invalid UTF8 on key"),
                        };

                        let timestmp = u64::from_le_bytes(timestamp);

                        let crc_from_buff = u32::from_le_bytes(crc);

                        let fresh_crc = compute_crc(
                            &timestamp,
                            &key_size,
                            &value_size,
                            key.as_slice(),
                            value.as_slice(),
                        );

                        if crc_from_buff != fresh_crc {
                            // corrupted data, break
                            break;
                        }

                        if val_size_num != 0 {
                            key_dir.insert(
                                key_as_str.to_string(),
                                KeydirEntry {
                                    file_id: file_name.to_string(),
                                    value_sz: val_size_num as u64,
                                    value_pos: value_position,
                                    tstamp: timestmp,
                                },
                            );
                        } else {
                            key_dir.remove(key_as_str); // if its there
                        }
                    }
                }
                _ => {}
            }
        }

        let memtable = AVL::new(MEMTABLE_THRESHOLD);
        let mut self_instance = Self {
            data_directory: path,
            key_dir,
            files: None,
            curr_file: None,
            curr_file_path: None,
            curr_file_offset: 0,
            sync_config,
            memtable,
        };

        if let Some(f) = files.last() {
            let f_metadata = f.metadata()?;
            if f_metadata.len() >= MAX_FILE_SIZE {
                self_instance.rotate_active_file()?;
            } else {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .read(true)
                    .open(f)?;

                self_instance.curr_file = Some(BufWriter::with_capacity(256000, file));
                self_instance.curr_file_path = Some(f.to_path_buf());
                self_instance.curr_file_offset = f_metadata.len();
                files.pop(); // active file shouldnt be in files
            }
        } else {
            self_instance.rotate_active_file()?;
        }

        self_instance.files = Some(files);
        Ok(self_instance)
    }
    fn get(&self, key: &str) -> io::Result<Vec<u8>> {
        println!("inside get, key is {}", key);
        let key_info = self
            .key_dir
            .get(key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Key not found"))?;

        let file_to_open = &key_info.file_id;
        let value_position = key_info.value_pos;
        let value_size = key_info.value_sz;

        let mut f: File = fs::File::open(file_to_open)?; // opening file on every get, optimize later
        let mut data = vec![0; value_size as usize];

        f.seek(SeekFrom::Start(value_position))?;

        f.read_exact(&mut data)?;

        Ok(data)
    }

    fn put(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        // DataWriteFailed Error later
        // this is the data block when we insert a key value: [ crc | tstamp | ksz | value_sz | key | value ]

        let tstamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // compute crc

        let key_as_bytes = key.as_bytes();
        let value_size = value.len();
        let data_format = KVEngine::serialize_record(tstamp, key_as_bytes, value);
        println!("we are inside put");
        println!("key is {}", key);

        let mut value_position_in_file = 28 + self.curr_file_offset + key_as_bytes.len() as u64;

        if self.curr_file_offset + data_format.len() as u64 <= MAX_FILE_SIZE {
            // we have space so we write it on curr file
            println!("we are writing in the current file");
            if let Some(f) = &mut self.curr_file {
                f.write_all(&data_format)?;

                self.curr_file_offset += data_format.len() as u64;
            } else {
                self.rotate_active_file()?;
                value_position_in_file = 28 + key_as_bytes.len() as u64;
                if let Some(f) = &mut self.curr_file {
                    f.write_all(&data_format)?;
                    self.curr_file_offset += data_format.len() as u64;
                }
            }
        } else {
            self.rotate_active_file()?;

            value_position_in_file = 28 + key_as_bytes.len() as u64;
            if let Some(f) = &mut self.curr_file {
                f.write_all(&data_format)?;
                self.curr_file_offset += data_format.len() as u64;
            }
        }
        let f_id = self
            .curr_file_path
            .as_ref()
            .unwrap()
            .as_os_str()
            .to_string_lossy()
            .into_owned();

        self.key_dir.insert(
            key.to_string(),
            KeydirEntry {
                file_id: f_id,
                value_sz: value_size as u64,
                value_pos: value_position_in_file,
                tstamp,
            },
        );
        Ok(())
    }
    fn delete(&mut self, key: &str) -> io::Result<()> {
        if !self.key_dir.contains_key(key) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Key not found"));
        }

        let t_stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let data_format = KVEngine::serialize_record(t_stamp, key.as_bytes(), &[]);

        if self.curr_file_offset + data_format.len() as u64 <= MAX_FILE_SIZE {
            if let Some(f) = &mut self.curr_file {
                f.write_all(&data_format)?;
                self.curr_file_offset += data_format.len() as u64;
            } else {
                self.rotate_active_file()?;
                if let Some(f) = &mut self.curr_file {
                    f.write_all(&data_format)?;
                    self.curr_file_offset += data_format.len() as u64;
                }
            }
        } else {
            self.rotate_active_file()?;
            if let Some(f) = &mut self.curr_file {
                f.write_all(&data_format)?;
                self.curr_file_offset += data_format.len() as u64;
            }
        }

        self.key_dir.remove(key);

        Ok(())
    }

    fn list_keys(&self) -> io::Result<Vec<String>> {
        let keys: Vec<String> = self.key_dir.keys().map(|k| k.to_string()).collect();

        Ok(keys)
    }
    fn fold<Acc, F>(&self, mut f: F, init: Acc) -> io::Result<Acc>
    where
        F: FnMut(String, &[u8], Acc) -> Acc,
    {
        let mut acc = init;
        let key_vec = self.list_keys()?;

        for key in key_vec {
            let value = self.get(&key)?;
            acc = f(key, &value, acc);
        }

        Ok(acc)
    }
    fn merge(&mut self) -> io::Result<()> {
        // merge should happen in another thread. continue to serve get,put, del, methods
        if let Some(vec) = self.files.as_ref() {
            let mut fresh_files: Vec<PathBuf> = Vec::new();

            let tstamp = KVEngine::new_timestamp();
            let new_data_file_tuple = KVEngine::create_new_data_file(&self.data_directory, tstamp)?;
            let new_hint_file_tuple = KVEngine::create_new_hint_file(&self.data_directory, tstamp)?;

            let mut fresh_file = BufWriter::with_capacity(256000, new_data_file_tuple.0);
            let mut hint_file = BufWriter::with_capacity(256000, new_hint_file_tuple.0);
            let mut f_name = new_data_file_tuple.1;

            fresh_files.push(PathBuf::from(&f_name));

            let mut fresh_file_length: u64 = 0;

            for path in vec {
                if Some(path) == self.curr_file_path.as_ref() {
                    continue; // active file stays shouldnt get touched.
                }
                let file_vec = fs::read(path)?;
                let mut cursor = Cursor::new(file_vec);
                let mut timestamp = [0u8; 8];
                let mut key_size = [0u8; 8];
                let mut value_size = [0u8; 8];
                let mut crc = [0u8; 4];

                let old_file_len = path.metadata()?.len();
                while cursor.position() < old_file_len {
                    cursor.read_exact(&mut crc)?;
                    cursor.read_exact(&mut timestamp)?; // put timestamp bytes into our slice
                    cursor.read_exact(&mut key_size)?; // put keysize bytes into our slice, this tells us how many bytes the key is
                    cursor.read_exact(&mut value_size)?; // put valuesize bytes into our slice
                    let key_size_num = u64::from_le_bytes(key_size) as usize;
                    let val_size_num = u64::from_le_bytes(value_size) as usize;

                    let mut key = vec![0u8; key_size_num];
                    let mut value = vec![0u8; val_size_num];

                    cursor.read_exact(&mut key)?;
                    let old_val_position = cursor.position();

                    cursor.read_exact(&mut value)?;

                    let key_as_str = match str::from_utf8(&key) {
                        Ok(k) => k,
                        Err(_r) => panic!("Invalid UTF8 on key"),
                    };

                    let crc_from_buff = u32::from_le_bytes(crc);

                    let fresh_crc = compute_crc(
                        &timestamp,
                        &key_size,
                        &value_size,
                        key.as_slice(),
                        value.as_slice(),
                    );

                    if crc_from_buff != fresh_crc {
                        // corrupted dont trust file.

                        break;
                    }

                    // problem: if we rewrite a value in our files, and then later it resides in our active file which is not part of files vec.
                    // when merging, there is not way for me to know whether the value I am adding to keydir is the live version
                    // example :put("foo", "v1") lands in an old file. Later, put("foo", "v2") lands in the active file. Merge processes
                    // the old file, writes the stale "v1" to the merged output, and updates the keydir to point there. v2 is unreachable cuz its in the active file

                    //
                    let should_rewrite = self.key_dir.get(key_as_str).map_or(false, |entry| {
                        (entry.file_id == path.to_string_lossy().as_ref())
                            & (entry.value_pos == old_val_position)
                    });

                    if !should_rewrite {
                        continue;
                    }

                    if val_size_num != 0 {
                        // [ crc | tstamp | ksz | value_sz | key | value  ]
                        let bytes_to_write_to_fresh: Vec<u8> = [
                            crc.as_slice(),
                            timestamp.as_slice(),
                            key_size.as_slice(),
                            value_size.as_slice(),
                            key.as_slice(),
                            value.as_slice(),
                        ]
                        .concat();
                        let value_position = fresh_file_length + 28 + key_size_num as u64;

                        // [ k_size, key, file_id, value_sz, data_position ]
                        let bytes_to_write_to_hint: Vec<u8> = [
                            &key_size,
                            key.as_slice(),
                            &timestamp,
                            &value_size,
                            &value_position.to_le_bytes(),
                        ]
                        .concat();

                        let fresh_bytes_len = bytes_to_write_to_fresh.len() as u64;
                        if (fresh_file_length + fresh_bytes_len) < MAX_FILE_SIZE {
                            hint_file.write_all(&bytes_to_write_to_hint)?;

                            fresh_file.write_all(&bytes_to_write_to_fresh)?;

                            fresh_file_length += fresh_bytes_len;
                            self.key_dir.insert(
                                key_as_str.to_string(),
                                KeydirEntry {
                                    file_id: f_name.to_string_lossy().to_string(),
                                    value_sz: val_size_num as u64,
                                    value_pos: value_position,
                                    tstamp: u64::from_le_bytes(timestamp),
                                },
                            );
                        } else {
                            let tstamp = KVEngine::new_timestamp();
                            let new_data_file_tuple =
                                KVEngine::create_new_data_file(&self.data_directory, tstamp)?;
                            let new_hint_file_tuple =
                                KVEngine::create_new_hint_file(&self.data_directory, tstamp)?;

                            fresh_file.flush()?;
                            hint_file.flush()?;
                            hint_file = BufWriter::with_capacity(256000, new_hint_file_tuple.0);

                            fresh_file = BufWriter::with_capacity(256000, new_data_file_tuple.0);
                            fresh_file_length = 0;

                            f_name = new_data_file_tuple.1;
                            // h_name = new_hint_file_tuple.1;
                            fresh_files.push(f_name.clone());
                            // fresh_files.push(h_name);

                            let value_position = 28 + key_size_num as u64;
                            let bytes_to_write_to_hint: Vec<u8> = [
                                key_size.as_slice(),
                                key.as_slice(),
                                &timestamp,
                                &value_size,
                                &value_position.to_le_bytes(),
                            ]
                            .concat();

                            hint_file.write_all(&bytes_to_write_to_hint)?;

                            fresh_file.write_all(&bytes_to_write_to_fresh)?;
                            fresh_file_length += fresh_bytes_len;

                            self.key_dir.insert(
                                key_as_str.to_string(),
                                KeydirEntry {
                                    file_id: f_name.to_string_lossy().to_string(),
                                    value_sz: val_size_num as u64,
                                    value_pos: value_position,
                                    tstamp: u64::from_le_bytes(timestamp),
                                },
                            );
                        }
                    } else {
                        self.key_dir.remove(key_as_str);
                    }
                }

                // delete old file now
                fs::remove_file(path)?;
                // delete old hint file too if it exits
                let stem = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
                    .unwrap();
                let hint_path = self.data_directory.join(format!("{}.hint", stem));
                if hint_path.exists() {
                    fs::remove_file(hint_path)?;
                }
            }

            fresh_file.flush()?;
            hint_file.flush()?;
            self.files = Some(fresh_files);
        }

        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        // forces any writes to sync to disk
        if let Some(writer) = &mut self.curr_file {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        Ok(())
    }

    fn rotate_active_file(&mut self) -> io::Result<()> {
        if let Some(writer) = &mut self.curr_file {
            writer.flush()?;
        }
        if let Some(old_path) = self.curr_file_path.take() {
            if let Some(files) = &mut self.files {
                files.push(old_path);
            }
        }
        let tstamp = KVEngine::new_timestamp();
        let new_data_file_tuple = KVEngine::create_new_data_file(&self.data_directory, tstamp)?;

        self.curr_file = Some(BufWriter::with_capacity(256000, new_data_file_tuple.0));
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

    fn deserialize_record() {
        unimplemented!();
    }

    fn close(&mut self) -> io::Result<()> {
        self.sync()?;

        Ok(())
    }
}
fn main() {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, tempfile};
    #[test]
    fn test_get_after_put() -> io::Result<()> {
        // put value in storage, then retrieve
        let dir = tempdir()?;
        let mut db = KVEngine::open(dir.path(), SyncConfig::None)?;
        db.put("hello", b"world")?;
        db.sync()?; // make sure we put the data in there
        assert_eq!(db.get("hello")?, b"world");
        Ok(())
    }

    #[test]
    fn delete_after_put() -> io::Result<()> {
        let dir = tempdir()?;
        let mut db = KVEngine::open(dir.path(), SyncConfig::None)?;
        db.put("hello", b"world")?;
        db.sync()?;
        assert_eq!(db.get("hello")?, b"world");
        db.delete("hello")?;

        assert!(db.get("hello").is_err());

        Ok(())
    }

    #[test]
    fn print_keys() -> io::Result<()> {
        let dir = tempdir()?;
        let mut db = KVEngine::open(dir.path(), SyncConfig::None)?;
        let mut vec: Vec<String> = Vec::new();
        db.put("hello", b"world")?;
        db.put("otherkey", b"world")?;
        db.put("thekey", b"world")?;
        db.put("space", b"world")?;

        vec = db.list_keys()?;
        vec.sort();

        assert_eq!(vec, vec!["hello", "otherkey", "space", "thekey"]);
        assert_eq!(vec.len(), 4);
        Ok(())
    }

    #[test]
    fn merge_files() -> io::Result<()> {
        let dir = tempdir()?;

        let mut db = KVEngine::open(dir.path(), SyncConfig::None)?;
        let mut vec: Vec<String> = Vec::new();
        db.put("hello", b"world")?;
        db.put("otherkey", b"world")?;
        db.put("thekey", b"world")?;
        db.put("space", b"world")?;
        db.delete("thekey")?;
        db.delete("otherkey")?;

        db.merge()?;
        db.sync()?;
        vec = db.list_keys()?;
        vec.sort();

        assert_eq!(vec, vec!["hello", "space"]);
        assert_eq!(vec.len(), 2);

        assert!(db.get("thekey").is_err());
        assert!(db.get("otherkey").is_err());

        let hint_files: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().unwrap_or_default() == "hint")
            .collect();
        assert!(!hint_files.is_empty());
        Ok(())
    }

    /*AVL Tree tests, insertion, deletion, rotations */
}
