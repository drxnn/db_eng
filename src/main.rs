use crc::{CRC_32_ISO_HDLC, Crc};
use std::collections::btree_map::Values;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{
    collections::HashMap,
    fmt::Error,
    path::{Path, PathBuf},
};

struct KeydirEntry {
    file_id: String, // basically file name "timestamp.data" or "timestamp.hint"
    value_sz: u64,
    value_pos: u64,
    tstamp: u64,
}

enum SyncConfig {
    None,       // fast
    Every(u64), // in ms
    Always,     // Durable
}

struct KVEngine {
    data_directory: PathBuf,
    files: Option<Vec<PathBuf>>,
    key_dir: HashMap<String, KeydirEntry>,
    curr_file: Option<BufWriter<File>>, // have a curr file to be the file you are currently writing on
    curr_file_path: Option<PathBuf>,
    curr_file_offset: u64, // and a cursor
    sync_config: SyncConfig,
}
const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1gb per file
impl KVEngine {
    fn create_new_file(dir: &Path) -> io::Result<((File, PathBuf), (File, PathBuf), u64)> {
        let tstamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(); // maybe keep an index as well just to be sure

        let data_file_path = &dir.join(format!("{}.data", tstamp));
        let hint_file_path = &dir.join(format!("{}.hint", tstamp));
        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(data_file_path)?;

        let hint_file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(hint_file_path)?;

        Ok((
            (data_file, data_file_path.to_path_buf()),
            (hint_file, hint_file_path.to_path_buf()),
            tstamp,
        ))
    }

    fn build_key_dir_from_file(
        keydir: HashMap<String, KeydirEntry>,
    ) -> HashMap<String, KeydirEntry> {
        unimplemented!()
        // move keydir to function, function adds to it and gives it back to caller
    }
    fn open(dir_name: &Path, sync_config: SyncConfig) -> io::Result<KVEngine> {
        let path = PathBuf::from(dir_name);
        let mut key_dir: HashMap<String, KeydirEntry> = HashMap::new();

        // when open runs, // scan the directory for all the files
        let mut files: Vec<PathBuf> = Vec::new();

        for entry in fs::read_dir(dir_name)? {
            let entry = entry?;
            let path = entry.path();
            println!("Name: {}", path.display());

            if path.is_file() && path.extension().unwrap() == "data" {
                // later we check the hint files
                files.push(path);
            }
        }

        files.sort_by_key(|x| {
            x.file_stem()
                .and_then(|y| y.to_str().and_then(|x| x.parse::<u64>().ok()))
        });

        for file in &files {
            let file_vec = fs::read(file)?;

            let file_name = file.to_str().unwrap();
            let mut cursor = Cursor::new(file_vec);
            let mut timestamp = [0u8; 8];
            let mut key_size = [0u8; 8];
            let mut value_size = [0u8; 8];
            let mut crc = [0u8; 4];

            let crc32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);
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

                // let mut buffer_without_crc = Vec::new();
                let mut digest = crc32.digest();
                digest.update(&timestamp);
                digest.update(&key_size);
                digest.update(&value_size);
                digest.update(&key);
                digest.update(&value);

                let fresh_crc = digest.finalize();

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

        let mut self_instance = Self {
            data_directory: path,
            key_dir,
            files: None,
            curr_file: None,
            curr_file_path: None,
            curr_file_offset: 0,
            sync_config,
        };

        if let Some(f) = files.last() {
            let f_metadata = f.metadata()?;
            if f_metadata.len() >= MAX_FILE_SIZE {
                let (new_data_file_tuple, new_hint_file_tuple, _) =
                    KVEngine::create_new_file(dir_name)?;
                self_instance.curr_file =
                    Some(BufWriter::with_capacity(256000, new_data_file_tuple.0));
                self_instance.curr_file_path = Some(new_data_file_tuple.1);
                self_instance.curr_file_offset = 0;
            } else {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .read(true)
                    .write(true)
                    .open(f)?;

                self_instance.curr_file = Some(BufWriter::with_capacity(256000, file));
                self_instance.curr_file_path = Some(f.to_path_buf());
                self_instance.curr_file_offset = f_metadata.len();
            }
        } else {
            let (new_data_file_tuple, new_hint_file_tuple, _) =
                KVEngine::create_new_file(dir_name)?;
            self_instance.curr_file = Some(BufWriter::with_capacity(256000, new_data_file_tuple.0));
            self_instance.curr_file_path = Some(new_data_file_tuple.1);
            self_instance.curr_file_offset = 0;
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

        if value_size != 0 {
            return Ok(data);
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "K/V was deleted",
            ))
        }
    }

    fn put(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        // DataWriteFailed Error later
        let crc32 = Crc::<u32>::new(&CRC_32_ISO_HDLC); // put it somewhere else later
        // this is the data block when we insert a key value: [ crc | tstamp | ksz | value_sz | key | value ]
        let mut bytes_to_write = Vec::new();
        let tstamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // compute crc

        let key_as_bytes = key.as_bytes();
        let ksz: usize = key_as_bytes.len();
        let value_size = value.len();
        println!("we are inside put");
        println!("key is {}", key);
        bytes_to_write.extend_from_slice(&tstamp.to_le_bytes());
        bytes_to_write.extend_from_slice(&ksz.to_le_bytes());
        bytes_to_write.extend_from_slice(&value_size.to_le_bytes());
        bytes_to_write.extend_from_slice(key_as_bytes);

        let value_position_in_file = bytes_to_write.len() as u64 + self.curr_file_offset + 4; // 4 is for the checksum bytes
        bytes_to_write.extend_from_slice(value);

        let checksum = crc32.checksum(&bytes_to_write);
        let mut data_format = checksum.to_le_bytes().to_vec();
        data_format.extend(bytes_to_write);

        if self.curr_file_offset + data_format.len() as u64 <= MAX_FILE_SIZE {
            // we have space so we write it on curr file
            println!("we are writing in the current file");
            if let Some(f) = &mut self.curr_file {
                f.write_all(&data_format)?;

                self.curr_file_offset += data_format.len() as u64;
            } else {
                let (new_data_file_tuple, new_hint_file_tuple, _) =
                    KVEngine::create_new_file(&self.data_directory)?;
                self.curr_file = Some(BufWriter::with_capacity(256000, new_data_file_tuple.0));
                self.curr_file_path = Some(new_data_file_tuple.1);
                self.curr_file_offset = 0;
                if let Some(f) = &mut self.curr_file {
                    f.write_all(&data_format)?;
                    self.curr_file_offset += data_format.len() as u64;
                }
            }
        } else {
            let (new_data_file_tuple, new_hint_file_tuple, _) =
                KVEngine::create_new_file(&self.data_directory)?;
            self.curr_file = Some(BufWriter::with_capacity(256000, new_data_file_tuple.0));
            self.curr_file_path = Some(new_data_file_tuple.1);
            self.curr_file_offset = 0;
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
        let crc32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

        if !self.key_dir.contains_key(key) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Key not found"));
        }

        let t_stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let bytes_to_write: Vec<u8> = [
            t_stamp.to_le_bytes().as_slice(),
            key.len().to_le_bytes().as_slice(),
            &0u64.to_le_bytes(), // 0 means its deleted, flag for stale value
            key.as_bytes(),
        ]
        .concat();

        let checksum = crc32.checksum(&bytes_to_write);
        let mut data_format = checksum.to_le_bytes().to_vec();
        data_format.extend(bytes_to_write);

        if let Some(f) = &mut self.curr_file {
            f.write_all(&data_format)?;
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
        if let Some(vec) = self.files.as_ref() {
            let mut fresh_files: Vec<PathBuf> = Vec::new();

            let (new_data_file_tuple, new_hint_file_tuple, mut tstamp) =
                KVEngine::create_new_file(&self.data_directory)?;

            let mut fresh_file = BufWriter::with_capacity(256000, new_data_file_tuple.0);
            let mut hint_file = BufWriter::with_capacity(256000, new_hint_file_tuple.0);
            let mut f_name = new_data_file_tuple.1;
            let mut h_name = new_hint_file_tuple.1;
            fresh_files.push(PathBuf::from(&f_name));
            fresh_files.push(PathBuf::from(&h_name));

            // no need to track hint_f_length because its always less than fresh_file
            let mut fresh_file_length: u64 = 0;

            for path in vec {
                if Some(path) == self.curr_file_path.as_ref() {
                    continue; // active file stays shouldn't get touched.
                }
                let file_vec = fs::read(path)?;
                let mut cursor = Cursor::new(file_vec);
                let mut timestamp = [0u8; 8];
                let mut key_size = [0u8; 8];
                let mut value_size = [0u8; 8];
                let mut crc = [0u8; 4];

                let crc32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);
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

                    cursor.read_exact(&mut value)?;

                    let key_as_str = match str::from_utf8(&key) {
                        Ok(k) => k,
                        Err(_r) => panic!("Invalid UTF8 on key"),
                    };

                    let crc_from_buff = u32::from_le_bytes(crc);

                    let mut digest = crc32.digest();
                    digest.update(&timestamp);
                    digest.update(&key_size);
                    digest.update(&value_size);
                    digest.update(&key);
                    digest.update(&value);
                    let fresh_crc = digest.finalize();

                    if crc_from_buff != fresh_crc {
                        // corrupted dont trust file.
                        // later we can check if we have a backup for this file and read it from there
                        break;
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
                        let bytes_to_write_to_hint: Vec<u8> = [
                            key_size.as_slice(),
                            key.as_slice(),
                            &timestamp,
                            &fresh_file_length.to_le_bytes(),
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
                            let (new_data_file_tuple, new_hint_file_tuple, _) =
                                KVEngine::create_new_file(&self.data_directory)?;

                            fresh_file.flush()?;
                            hint_file.flush()?;
                            hint_file = BufWriter::with_capacity(256000, new_hint_file_tuple.0);

                            fresh_file = BufWriter::with_capacity(256000, new_data_file_tuple.0);
                            fresh_file_length = 0;

                            f_name = new_data_file_tuple.1;
                            h_name = new_hint_file_tuple.1;
                            fresh_files.push(f_name.clone());
                            fresh_files.push(h_name);

                            let value_position = 28 + key_size_num as u64; // file is fresh
                            let bytes_to_write_to_hint: Vec<u8> = [
                                key_size.as_slice(),
                                key.as_slice(),
                                &timestamp,
                                &fresh_file_length.to_le_bytes(),
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
            }

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
}

/*
Documentation for myself:
data format for files:   [ crc | tstamp | ksz | value_sz | key | value ]
      // deleted value format: [ crc | tstamp | ksz | 0u64 | key ]


data format for hint files:  [ k_size, key, file_id, data_position ] // file is the timestamp, since we are doing tstamp.data
// hint file doesnt actually need file id, because it has the same fileid as the file.data, file.hint file == file

*/
