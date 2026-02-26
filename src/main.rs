/*
NOTES:
Use fsync for writing data, when we use write() to write data to a file, the OS doesnt immediately put the data on disk.
Intead it puts it in a page cache, which is an in memory buffer managed by the kernel. The OS will flush it to disk eventually,
however the problem is that what if the machine crashes or we lose power or the OS panics before the data is written?
Thats what fsync comes in, you basically tell teh OS "dont return until every byte we write to this file descriptor is in stable storage"
fsync = success then data is safely on disk
use Seek trait

Things I need to do:
we have a directory where we keep the data
we write the data in files(1 gb max, then move to next file).
We write to the active file by appending.
Each write is simply a new entry to the active file.
deletion is simply a write of a special tombstone value which will be removed on the next merge

the format for each key/value entry is this:

[ crc | tstamp | ksz | value_sz | key | value ]

 After the append completes, an in-memory structure called a keydir is updated.
 a keydir is a hashtable that maps every key in a BitCask to a fixed-size structure giving the file, offset
 and size of the most recently written entry for that key.
 key -> [ file_id | value_sz | value_pos | tstamp ]
 key -> [ file_id | value_sz | value_pos | tstamp ]
 key -> [ file_id | value_sz | value_pos | tstamp ]

 for a file_id use a file{index}.timestamp
 if a write occurs, we update the keydir with the location of the newest data. The old data will remain on disk, but
 new reads will use the latest version available in the keydir.

 Reading a value:
 we look up the key in the keydir and from there we use the data using the file_id(which file it is),
 position and size(so we know where in the file to start and where to stop).



*/

use crc::{CRC_32_ISO_HDLC, Crc};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    collections::HashMap,
    fmt::Error,
    path::{Path, PathBuf},
};
// structs and enums

struct KeydirEntry {
    file_id: String, // basically file name "timestamp.data"
    value_sz: usize,
    value_pos: usize,
    tstamp: u64,
}

struct KVEngine {
    data_directory: PathBuf,
    files: Option<Vec<PathBuf>>,
    key_dir: HashMap<String, KeydirEntry>,
    curr_file: Option<File>, // have a curr file to be the file you are currently writing on
    curr_file_offset: u32,   // and a cursor
}
const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1gb per file
impl KVEngine {
    fn create_new_file(dir: &Path) -> io::Result<File> {
        let tstamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let file_path = dir.join(format!("{}.data", tstamp));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)?;

        Ok(file)
    }
    fn open(dir_name: &Path) -> io::Result<KVEngine> {
        let path = PathBuf::from(dir_name);
        let key_dir: HashMap<String, KeydirEntry> = HashMap::new();
        let mut curr_file: File;

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

        for file in files {

            // lets rebuild keydir
            // for now lets just go file by file, later we can do it in parallel + use hint files
        }

        let mut self_instance = Self {
            data_directory: path,
            key_dir,
            files: None,
            curr_file: None,
            curr_file_offset: 0,
        };

        if let Some(f) = files.last() {
            let f_metadata = f.metadata()?;
            if f_metadata.len() >= MAX_FILE_SIZE {
                // self_instance.curr_file = self_instance.create_new_file(dir_name)?
                let new_file: File = KVEngine::create_new_file(dir_name)?; // i cant use .ok() here for some reason
                self_instance.curr_file = Some(new_file);
            } else {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .read(true)
                    .write(true)
                    .open(f)?;

                self_instance.curr_file = Some(file);
            }
        }

        self_instance.files = Some(files);
        Ok(self_instance)
    }
    fn get(&self) -> Result<Option<Vec<u8>>, Error> {
        // will make a custom ValueNotFound Error later
        unimplemented!()
    }
    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), Error> {
        // DataWriteFailed Error later
        let crc32 = Crc::<u32>::new(&CRC_32_ISO_HDLC); // put it somewhere else later
        // this is the data block when we insert a key value: [ crc | tstamp | ksz | value_sz | key | value ]
        let mut bytes_to_write = Vec::new();
        let tstamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // compute crc
        // let checksum = crc32.checksum(&bytes_to_write);

        let key_as_bytes = key.as_bytes();
        let ksz = key_as_bytes.len();
        let value_size = value.len();
        bytes_to_write.extend_from_slice(&tstamp.to_le_bytes());
        bytes_to_write.extend_from_slice(&ksz.to_le_bytes());
        bytes_to_write.extend_from_slice(&value_size.to_le_bytes());
        bytes_to_write.extend_from_slice(key_as_bytes);
        bytes_to_write.extend_from_slice(value);
        let checksum = crc32.checksum(&bytes_to_write);
        let mut data_format = checksum.to_le_bytes().to_vec();
        data_format.extend(bytes_to_write);

        unimplemented!()
    }
    fn delete(&mut self, key: &str) -> Result<(), Error> {
        unimplemented!()
    }

    fn list_keys(&self) -> Result<&[&str], Error> {
        unimplemented!()
    }
    fn fold() {
        unimplemented!()
    }
    fn merge(&mut self) {
        unimplemented!()
        // merges several data files within a Bitcask datastore into a more compact form. Also, produce hint files for faster startup
    }

    fn sync(&mut self) {
        unimplemented!()
        // forces any writes to sync to disk
    }

    fn close(&mut self) {
        unimplemented!()
        // close the data store and flush all pending writes(if any) to disk
    }
}
fn main() {}
