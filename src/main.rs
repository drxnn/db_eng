use crc::{CRC_32_ISO_HDLC, Crc};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{
    collections::HashMap,
    fmt::Error,
    path::{Path, PathBuf},
};

struct KeydirEntry {
    file_id: String, // basically file name "timestamp.data"
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
    curr_file: Option<File>, // have a curr file to be the file you are currently writing on
    curr_file_path: Option<PathBuf>,
    curr_file_offset: u64, // and a cursor
    sync_config: SyncConfig,
}
const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1gb per file
impl KVEngine {
    fn create_new_file(dir: &Path) -> io::Result<(File, PathBuf)> {
        let tstamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(); // maybe keep an index as well just to be sure

        let file_path = &dir.join(format!("{}.data", tstamp));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)?;

        Ok((file, file_path.to_path_buf()))
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

            let file_name = file.file_stem().unwrap().to_str().unwrap(); // will not error because we are sure its a file therefore it has a stem
            let mut cursor = Cursor::new(file_vec);
            let mut timestamp = [0u8; 8];
            let mut key_size = [0u8; 8];
            let mut value_size = [0u8; 8];
            let mut crc = [0u8; 4];
            let mut flag = [0u8; 1];

            let crc32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);
            let file_len = file.metadata()?.len();
            // MAKE SURE THAT ONLY ALIVE k/v pairs are referenced.
            // use a flag: [ crc | tstamp | ksz | value_sz |  key | value | flag  ]
            // flag is either 1 or 0. 1 == active, 0 = deleted
            while cursor.position() < file_len {
                // reading sequential data that looks like:
                //                      [ crc | tstamp | ksz | value_sz | key | value ]
                //             sizes:   [ 32b |  64b   | 64b |    64b   | ksz | valuesz ]
                cursor.read_exact(&mut crc)?;
                cursor.read_exact(&mut timestamp)?; // put timestamp bytes into our slice
                cursor.read_exact(&mut key_size)?; // put keysize bytes into our slice, this tells us how many bytes the key is
                cursor.read_exact(&mut value_size)?; // put valuesize bytes into our slice
                cursor.read_exact(&mut flag)?;
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
                digest.update(&flag);
                let fresh_crc = digest.finalize();

                if crc_from_buff != fresh_crc {
                    // corrupted data, break
                    break;
                }

                if u8::from_le_bytes(flag) == 1u8 {
                    // if 1, it means it hasnt been deleted, if 0 though we should delete. impl later
                    // what to do if data was bad? discard it?
                    //
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
                    // delete? will figure out
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
                let (new_file, new_file_path) = KVEngine::create_new_file(dir_name)?;
                self_instance.curr_file = Some(new_file);
                self_instance.curr_file_path = Some(new_file_path);
                self_instance.curr_file_offset = 0;
            } else {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .read(true)
                    .write(true)
                    .open(f)?;

                self_instance.curr_file = Some(file);
                self_instance.curr_file_path = Some(f.to_path_buf());
                self_instance.curr_file_offset = f_metadata.len();
            }
        } else {
            let (new_file, new_file_path) = KVEngine::create_new_file(dir_name)?;
            self_instance.curr_file = Some(new_file);
            self_instance.curr_file_path = Some(new_file_path);
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

        // check delete flag before you return

        let mut f: File = fs::File::open(file_to_open)?; // opening file on every get, optimize later 
        let mut data = vec![0; value_size as usize];
        let mut flag_data = [0u8; 1];

        f.seek(SeekFrom::Start(value_position))?;

        f.read_exact(&mut data)?;
        f.read_exact(&mut flag_data)?;

        // this underneath works for now, will rethink later
        if u8::from_le_bytes(flag_data) == 1 {
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
        // this is the data block when we insert a key value: [ crc | tstamp | ksz | value_sz | key | value | flag ]
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
        bytes_to_write.extend_from_slice(&[1u8]); // flag
        let checksum = crc32.checksum(&bytes_to_write);
        let mut data_format = checksum.to_le_bytes().to_vec();
        data_format.extend(bytes_to_write);

        if self.curr_file_offset + data_format.len() as u64 <= MAX_FILE_SIZE {
            // we have space so we write it on curr file
            println!("we are writing in the current file");
            if let Some(f) = &mut self.curr_file {
                f.write_all(&data_format)?;
                f.sync_all()?; // syscalls on every write, ok for now
            } else {
                let (file, filepath) = KVEngine::create_new_file(&self.data_directory)?;
                self.curr_file = Some(file);
                self.curr_file_path = Some(filepath);
                self.curr_file_offset = 0;
                if let Some(f) = &mut self.curr_file {
                    f.write_all(&data_format)?;
                    f.sync_all()?; // syscalls on every write, ok for now
                }
            }
        } else {
            let (new_file, new_file_path) = KVEngine::create_new_file(&self.data_directory)?;
            self.curr_file = Some(new_file);
            self.curr_file_path = Some(new_file_path);
            self.curr_file_offset = 0;
            if let Some(f) = &mut self.curr_file {
                f.write_all(&data_format)?;
                f.sync_all()?; // syscalls on every write, ok for now
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
        let key_info = self
            .key_dir
            .get(key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Key not found"))?;

        let file_to_open = &key_info.file_id;
        let value_position = key_info.value_pos;
        let value_size = key_info.value_sz;
        let flag_position = value_position + value_size;

        let mut f: File = fs::File::open(file_to_open)?;
        f.seek(SeekFrom::Start(flag_position))?;
        f.write_all(&[0u8])?;
        f.sync_data()?;
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
        // merges several data files within a Bitcask datastore into a more compact form. Also, produce hint files for faster startup
        /*
        Go through all the older data files(that dont have a hint file) and merge them together in a FILE.
        Throughout the way, produce the hint file for the big merge file.
        the hint file is the one we go through, to find our k/v pairs. so it should be [tstamp. key, keysz, val_position, val_size] // no value

        // Before merge, make sure that you check if you have enough disk size, to first add the new merged files, then delete old ones, or add one delete one but still check

        */

        if let Some(vec) = self.files.as_ref() {
            let mut fresh_files: Vec<PathBuf> = Vec::new();
            for path in vec {
                let tstamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let f_name = format!("{}.data", tstamp);
                let h_name = format!("{}.hint", tstamp);
                let mut hint_file: File = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(&h_name)?;

                let mut fresh_file: File = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(&f_name)?;

                let file_vec = fs::read(path)?;
                let mut cursor = Cursor::new(file_vec);
                let mut timestamp = [0u8; 8];
                let mut key_size = [0u8; 8];
                let mut value_size = [0u8; 8];
                let mut crc = [0u8; 4];
                let mut flag = [0u8; 1];

                let crc32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);
                let file_len = path.metadata()?.len();
                while cursor.position() < file_len {
                    cursor.read_exact(&mut crc)?;
                    cursor.read_exact(&mut timestamp)?; // put timestamp bytes into our slice
                    cursor.read_exact(&mut key_size)?; // put keysize bytes into our slice, this tells us how many bytes the key is
                    cursor.read_exact(&mut value_size)?; // put valuesize bytes into our slice
                    cursor.read_exact(&mut flag)?;
                    let key_size_num = u64::from_le_bytes(key_size) as usize;
                    let val_size_num = u64::from_le_bytes(value_size) as usize;

                    let mut key = vec![0u8; key_size_num];
                    let mut value = vec![0u8; val_size_num];

                    cursor.read_exact(&mut key)?;
                    let value_position = cursor.seek(SeekFrom::Current(0))?;

                    cursor.read_exact(&mut value)?;

                    let key_as_str = match str::from_utf8(&key) {
                        Ok(k) => k,
                        Err(_r) => panic!("Invalid UTF8 on key"),
                    };

                    let timestmp = u64::from_le_bytes(timestamp);

                    let crc_from_buff = u32::from_le_bytes(crc);

                    let mut digest = crc32.digest();
                    digest.update(&timestamp);
                    digest.update(&key_size);
                    digest.update(&value_size);
                    digest.update(&key);
                    digest.update(&value);
                    digest.update(&flag);
                    let fresh_crc = digest.finalize();

                    if crc_from_buff != fresh_crc {
                        // corrupted dont trust file.
                        // later we can check if we have a backup for this file and read it from there
                        break;
                    }

                    if u8::from_le_bytes(flag) == 1u8 {
                        // [ crc | tstamp | ksz | value_sz | key | value | flag ]
                        let bytes_to_write_to_fresh: Vec<u8> = [
                            crc.as_slice(),
                            timestamp.as_slice(),
                            key_size.as_slice(),
                            value_size.as_slice(),
                            key.as_slice(),
                            value.as_slice(),
                            flag.as_slice(),
                        ]
                        .concat();

                        let fresh_file_cursor_pos = fresh_file.seek(SeekFrom::Current(0))?;
                        let bytes_to_write_to_hint: Vec<u8> = [
                            key_size.as_slice(),
                            key.as_slice(),
                            &tstamp.to_le_bytes(),
                            &fresh_file_cursor_pos.to_be_bytes(),
                        ]
                        .concat();
                        hint_file.write_all(&bytes_to_write_to_hint)?; // syscall
                        hint_file.sync_all()?; // syscall

                        fresh_file.write_all(&bytes_to_write_to_fresh)?; // syscall
                        fresh_file.sync_all()?; // syscall
                    } else {
                        self.key_dir.remove(key_as_str);
                    }
                }

                // delete old file now
                fs::remove_file(path)?;
                fresh_files.push(PathBuf::from(&f_name));
                fresh_files.push(PathBuf::from(&h_name));
            }
        }

        Ok(())
    }

    fn sync(&mut self) {
        unimplemented!()
        // forces any writes to sync to disk
    }

    fn close(&mut self) {
        unimplemented!()
    }
}
fn main() {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, tempfile};
    #[test]
    fn test_get_after_put() {
        // put value in storage, then retrieve
        let dir = tempdir().unwrap();
        let mut db = KVEngine::open(dir.path(), SyncConfig::None).unwrap();
        db.put("hello", b"world").unwrap();
        assert_eq!(db.get("hello").unwrap(), b"world");
    }

    // #[test]
    // fn delete_after_put() {
    //     unimplemented!()
    // }

    #[test]
    fn print_keys() {
        let dir = tempdir().unwrap();
        let mut db = KVEngine::open(dir.path(), SyncConfig::None).unwrap();
        let mut vec: Vec<String> = Vec::new();
        db.put("hello", b"world").unwrap();
        db.put("otherkey", b"world").unwrap();
        db.put("thekey", b"world").unwrap();
        db.put("space", b"world").unwrap();
        vec = db.list_keys().unwrap();
        vec.sort();

        assert_eq!(vec, vec!["hello", "otherkey", "space", "thekey"]);
        assert_eq!(vec.len(), 4);
    }

    // #[test]
    // fn merge_files() {
    //     let dir = tempdir().unwrap();
    //     let file_1 = tempfile().unwrap();
    //     let file_2 = tempfile().unwrap();
    //     let mut db = KVEngine::open(dir.path()).unwrap();
    // }
}

/*
Documentation for myself:
data format for files:   [ crc | tstamp | ksz | value_sz | key | value | flag ], will change instead to [ crc | tstamp | ksz | value_sz | key | value ]
// and do append only.


data format for hint files:  [k_size, key, file_id, data_position ] // file is the timestamp, since we are doing tstamp.data

*/
