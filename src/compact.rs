

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use std::fs::OpenOptions;
use std::io::ErrorKind::UnexpectedEof;
use std::io::Write;
use std::thread::spawn;
use std::unimplemented;
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

struct MergeItem {
    entry: HeapEntry,
    source: usize, // should be the index of CompactionFileElement and we use that to grab the next entry
}

impl MergeItem {
    fn new(cfe: &mut CompactionFileElement, index: usize) -> Result<Self> {
        let heap_entry = match cfe.next_heap_entry() {
            Ok(heap_entry) => heap_entry,
            Err(DbError::Io(io_err)) if io_err.kind() == UnexpectedEof {
                // stop
            },
            Err(err) => {
                // return
            },
        }
        Ok(Self {
            entry: heap_entry,
            source: index,
        })
    }
    fn serialize_record(&mut self, cfe: &mut CompactionFileElement) -> Result<Vec<u8>> {
        // [ tstamp(8) | ksz(8) | value_sz(8) | deletedflag(1) | key | value ]

        let tstamp = self.entry.timestamp.to_le_bytes();
        let ksz = (self.entry.key.len() as u64).to_le_bytes();
        let vsz = self.entry.vsz.to_le_bytes();
        let deleted = if self.entry.deleted {
            0xFF_u8.to_le_bytes()
        } else {
            0x00_u8.to_le_bytes()
        };
        let key = &self.entry.key;

        let value = cfe.take_value_and_advance(&self.entry)?;
        let record = [&tstamp, &ksz, &vsz, &deleted[..], key, &value].concat();
        Ok(record)
    }
}

struct CompactionFileElement {
    file_path: PathBuf,
    reader: BufReader<File>,
//  sparse_index: Vec<(Vec<u8>, u64, u64)>, // keysz | offset | datablock block length ( before CRC,
}

struct HeapEntry {
    key: Vec<u8>,
    timestamp: u64,
    deleted: bool,
    vsz: u64,
}

impl CompactionFileElement {
    fn new(path: PathBuf) -> Result<Self> {
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        Ok(Self {
            file_path: path,
            reader,
        })
    }

    //
    fn next_heap_entry(&mut self) -> Result<HeapEntry> {
        // gets called when cursor for reader is at the beginning of a record
        let mut tstamp = [0u8; 8];
        let mut ksz = [0u8; 8];
        let mut vsz = [0u8; 8];
        let mut tmbstone = [0u8; 1];
        //[ tstamp(8) | ksz(8) | value_sz(8) |tombstone| key | value |  ]
        //  TODO: you are just reading the records, but the way we have them in memory is Data_Record_Block|crc|Data_Record_Block|Crc
        // fix to take into account
        // we need to read the sparse index for each file
        // sparse_index: Vec<(Vec<u8>, u64, u64)>, // keysz | offset | datablock block length ( before CRC,
        // load sparse index for each compaction file element and use it to determine if each data block is still non corrupted.
        // if at any point, crc doesnt match, stop and throw away
        // this functionality is used in lsm.rs,extract it in a helper function
        let reader = &mut self.reader;

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
                    file_path: self.file_path.clone(),
                    reason: crate::errors::CorruptionType::TombstoneCorrupted {
                        found: tmbstone[0],
                    },
                }));
            }
        };

        reader.read_exact(&mut key)?;
        Ok(HeapEntry {
            key: key.to_vec(),
            timestamp: u64::from_le_bytes(tstamp),
            deleted,
            vsz: u64::from_le_bytes(vsz),
        })
    }

    fn consume_val(&mut self, heap_entry: &HeapEntry) -> Result<Vec<u8>> {
        let vsz = heap_entry.vsz;
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

    fn take_value_and_advance(&mut self, heap_entry: &HeapEntry) -> Result<Vec<u8>> {
        //
        let val = self.consume_val(heap_entry)?;

        self.advance_header()?;

        Ok(val)
    }

    fn skip_value_and_advance(&mut self, vsz: u64) -> Result<()> {
        // only call when a HeapEntryExists

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
    fn advance_header(&mut self) -> Result<HeapEntry> {
        let mut tstamp = [0u8; 8];
        let mut ksz = [0u8; 8];
        let mut vsz = [0u8; 8];
        let mut tmbstone = [0u8; 1];
        match self.reader.read_exact(&mut tstamp) {
            Ok(()) => {} // fall through
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // return Ok(()); // TODO: make sure we return Signal that no more entries are to be found
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

        Ok(HeapEntry {
            key: key.to_vec(),
            timestamp: u64::from_le_bytes(tstamp),
            deleted,
            vsz: u64::from_le_bytes(vsz),
        })
    }
}

struct CompactionManager {
    tx: Sender<CompactionThreadResponse>,
    rx: Receiver<CompactionThreadResponse>,
    file_handles: Option<Vec<CompactionFileElement>>,
    heap: Option<BinaryHeap<MergeItem>>,
}

impl CompactionManager {
    // have main have a select_files_for_compaction function -> Vec<PathBuf>
    fn new(files: Vec<PathBuf>) -> Result<Self> {
        let (tx, rx) = mpsc::channel::<CompactionThreadResponse>();
        let mut cfe_vec: Vec<CompactionFileElement> = Vec::with_capacity(files.len());

        let mut heap: BinaryHeap<MergeItem> = BinaryHeap::with_capacity(cfe_vec.len());

        for (i, x) in files.into_iter().enumerate() {
            let mut cfe = match CompactionFileElement::new(x) {
                Ok(compact_el) => compact_el,
                Err(err) => continue, // file is corrupt?
            };
            let m_item = match MergeItem::new(&mut cfe, i) {
                Ok(m_item) => m_item,
                Err(err) => continue,
            };
            heap.push(m_item);
            cfe_vec.push(cfe);
        }
        // let _ = files
        //     .into_iter()
        //     .enumerate()
        //     .try_for_each(|(i, x)| -> Result<()> {

        //         Ok(())
        //     });

        Ok(Self {
            tx,
            rx,
            file_handles: Some(cfe_vec),
            heap: Some(heap),
        })
    }

    fn background_compact(&mut self, sst_paths: (PathBuf, PathBuf)) -> Result<()> {
        let tx: Sender<CompactionThreadResponse> = self.tx.clone();
        let f = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(&sst_paths.0)?;
        let mut writer = BufWriter::new(f);
        let heap = self.heap.take().unwrap(); //fix this unwrap
        let mut cfe_vec = self.file_handles.take().unwrap(); // handle

        spawn(move || -> Result<()> {
            // put into a function for readability
            match CompactionManager::merge_to_final(heap, &mut writer, &mut cfe_vec) {
                Ok(()) => {
                    let sstable = SSTable::load(&sst_paths.1);
                    match sstable {
                        Ok(sst) => {
                            let _ = tx.send(CompactionThreadResponse::Success(sst));
                        }
                        Err(dberr) => {
                            let _ = tx.send(CompactionThreadResponse::Error(dberr));
                        }
                    }
                }
                Err(err) => {
                    let _ = tx.send(CompactionThreadResponse::Error(err));
                }
            }

            Ok(())
        });

        Ok(())
    }

    fn merge_to_final(
        mut heap: BinaryHeap<MergeItem>,
        writer: &mut BufWriter<File>,
        cfe_vec: &mut [CompactionFileElement],
    ) -> Result<()> {
        let mut last_k_written: Option<Vec<u8>> = None;
        while !heap.is_empty() {
            if let Some(curr_merge_item) = heap.pop().as_mut() {
                let cfe = cfe_vec.get_mut(curr_merge_item.source).unwrap();
                match last_k_written.is_some()
                    && curr_merge_item.entry.key == *last_k_written.as_ref().unwrap()
                {
                    true => {
                        cfe.skip_value_and_advance(curr_merge_item.entry.vsz)?;
                    }
                    false => {
                        let record = curr_merge_item.serialize_record(cfe)?;
                        writer.write_all(&record)?;

                        last_k_written = Some(curr_merge_item.entry.key.to_vec()); // can we avoid this here
                    }
                }

                let new_merge_item = MergeItem::new(cfe, curr_merge_item.source)?;
                heap.push(new_merge_item);
            }
        }
        writer.get_mut().sync_all()?;
        Ok(())
    }
}

impl Ord for MergeItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.entry
            .key
            .cmp(&other.entry.key) // smaller wins
            .then_with(|| self.entry.timestamp.cmp(&other.entry.timestamp)) // smaller wins
            .then_with(|| self.source.cmp(&other.source)) // smaller wins, sources are ordered smaller->bigger. smaller id means file is newer, file contains the fresh key
    }
}

impl PartialOrd for MergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MergeItem {
    fn eq(&self, other: &Self) -> bool {
        let slf_key = &self.entry.key;

        let other_key = &other.entry.key;

        slf_key == other_key
    }
}
impl Eq for MergeItem {}
