use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use std::thread::spawn;
use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek},
    path::PathBuf,
    sync::mpsc::{self, Receiver, Sender},
};

use crate::{
    errors::{DataCorruptedErr, DbError, Result},
    lsm::{SSTable, VALUE_MAX_BYTES_SIZE},
};

pub enum CompactionThreadResponse {
    Success(SSTable),
    Error(DbError),
}

struct MinHeap {
    heap: BinaryHeap<CompactionFileElement>,
}

impl MinHeap {
    fn new(files: Vec<CompactionFileElement>) -> Self {
        let mut heap: BinaryHeap<CompactionFileElement> = BinaryHeap::with_capacity(files.len());
        files.into_iter().for_each(|x| heap.push(x));
        MinHeap { heap }
    }
}

/*

*/

struct CompactionFileElement {
    current: Option<HeapEntry>,
    file_path: PathBuf,
    reader: BufReader<File>,
}

// the way it works: when we read a HeapEntry into memory, and the readers cursor stays right at where value begins. Use vsz to know how many bytes to read.
//
struct HeapEntry {
    key: Vec<u8>,
    timestamp: u64,
    deleted: bool,
    vsz: u64,
}

impl CompactionFileElement {
    fn new(path: PathBuf) -> Result<Self> {
        let file = File::open(&path)?;
        // read the first key
        let mut reader = BufReader::new(file);
        let mut tstamp = [0u8; 8];
        let mut ksz = [0u8; 8];
        let mut vsz = [0u8; 8];
        let mut tmbstone = [0u8; 1];
        //[ tstamp(8) | ksz(8) | value_sz(8) |tombstone| key | value |  ]
        // PROBLEM: File could be empty, handle
        reader.read_exact(&mut tstamp)?;
        reader.read_exact(&mut ksz)?;
        let key_size = u64::from_le_bytes(ksz);
        let mut key = vec![0u8; key_size as usize];
        reader.read_exact(&mut vsz)?;
        reader.read_exact(&mut tmbstone)?;

        let curr_offset = reader.stream_position()?;

        let deleted = match tmbstone[0] {
            0xFF => true,
            0x00 => false,
            _ => {
                return Err(DbError::DataCorrupted(DataCorruptedErr {
                    offset: curr_offset,
                    file_path: path,
                    reason: crate::errors::CorruptionType::TombstoneCorrupted {
                        found: tmbstone[0],
                    },
                }));
            }
        };

        reader.read_exact(&mut key)?;
        Ok(Self {
            current: Some(HeapEntry {
                key: key.to_vec(),
                timestamp: u64::from_le_bytes(tstamp),
                deleted,
                vsz: u64::from_le_bytes(vsz),
            }),
            file_path: path,
            reader,
        })
    }

    fn consume_val(&mut self) -> Result<Vec<u8>> {
        let curr = self.current.as_ref().ok_or_else(|| {
            DbError::MissingHeapEntry(
                "Heap Entry not found when attempting to read value".to_string(),
                self.file_path.clone(),
            )
        })?;
        let vsz = curr.vsz;

        let curr_offset = self.reader.stream_position()?;

        if vsz > VALUE_MAX_BYTES_SIZE {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: curr_offset,
                file_path: self.file_path.clone(),
                reason: crate::errors::CorruptionType::KeyValueRecordExceedsMaxLength {
                    max: VALUE_MAX_BYTES_SIZE,
                    found: vsz,
                },
            }));
        }
        let mut val = vec![0u8; vsz as usize];

        self.reader.read_exact(&mut val)?;
        Ok(val.to_vec())
    }

    fn take_value_and_advance(&mut self) -> Result<Vec<u8>> {
        //
        let val = self.consume_val()?;

        self.advance_header()?;

        Ok(val)
    }

    fn skip_value_and_advance(&mut self) -> Result<()> {
        let curr = self.current.as_ref().ok_or_else(|| {
            DbError::MissingHeapEntry(
                "Heap Entry not found when attempting to skip value".to_string(),
                self.file_path.clone(),
            )
        })?;
        let vsz = curr.vsz;

        let curr_offset = self.reader.stream_position()?;

        if vsz > VALUE_MAX_BYTES_SIZE {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: curr_offset,
                file_path: self.file_path.clone(),
                reason: crate::errors::CorruptionType::KeyValueRecordExceedsMaxLength {
                    max: VALUE_MAX_BYTES_SIZE,
                    found: vsz,
                },
            }));
        }
        self.reader.seek_relative(vsz as i64)?;
        self.advance_header()?;
        Ok(())
    }
    fn advance_header(&mut self) -> Result<()> {
        self.current.take();
        let mut tstamp = [0u8; 8];
        let mut ksz = [0u8; 8];
        let mut vsz = [0u8; 8];
        let mut tmbstone = [0u8; 1];
        match self.reader.read_exact(&mut tstamp) {
            Ok(()) => {} // fall through
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(()); // self.current is already None from above -> no more elements
            }
            Err(e) => return Err(DbError::Io(e)),
        }
        self.reader.read_exact(&mut ksz)?;
        let key_size = u64::from_le_bytes(ksz);
        let mut key = vec![0u8; key_size as usize];
        self.reader.read_exact(&mut vsz)?;
        self.reader.read_exact(&mut tmbstone)?;

        let curr_offset = self.reader.stream_position()?;

        let deleted = match tmbstone[0] {
            0xFF => true,
            0x00 => false,
            _ => {
                return Err(DbError::DataCorrupted(DataCorruptedErr {
                    offset: curr_offset,
                    file_path: self.file_path.clone(),
                    reason: crate::errors::CorruptionType::TombstoneCorrupted {
                        found: tmbstone[0],
                    },
                }));
            }
        };
        self.reader.read_exact(&mut key)?;

        self.current = Some(HeapEntry {
            key: key.to_vec(),
            timestamp: u64::from_le_bytes(tstamp),
            deleted,
            vsz: u64::from_le_bytes(vsz),
        });

        Ok(())
    }
}

struct CompactionManager {
    tx: Sender<CompactionThreadResponse>,
    rx: Receiver<CompactionThreadResponse>,
    heap: MinHeap,
}

impl CompactionManager {
    fn new(files: Vec<CompactionFileElement>) -> Self {
        let (tx, rx) = mpsc::channel::<CompactionThreadResponse>();
        let heap = MinHeap::new(files);
        Self { tx, rx, heap }
    }

    fn background_compact(&mut self) -> Result<()> {
        let tx: Sender<CompactionThreadResponse> = self.tx.clone();
        spawn(move || -> Result<()> {})
    }
}

impl Ord for CompactionFileElement {
    fn cmp(&self, other: &Self) -> Ordering {
        let slf_key = self
            .current
            .as_ref()
            .map(|heap_entry| Some(&heap_entry.key));

        let other_key = other
            .current
            .as_ref()
            .map(|heap_entry| Some(&heap_entry.key));

        slf_key.cmp(&other_key)
    }
}

impl PartialOrd for CompactionFileElement {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for CompactionFileElement {
    fn eq(&self, other: &Self) -> bool {
        let slf_key = self.current.as_ref().map(|heap_entry| &heap_entry.key);

        let other_key = other.current.as_ref().map(|heap_entry| &heap_entry.key);

        slf_key == other_key
    }
}

impl Eq for CompactionFileElement {}
