use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use std::fs::{self, OpenOptions};

use std::io::ErrorKind::UnexpectedEof;
use std::io::Write;

use std::sync::Arc;
use std::thread::spawn;

use std::mem;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek},
    path::PathBuf,
    sync::mpsc::{self, Receiver, Sender},
};

use crate::errors::CorruptionType;
use crate::helpers::{compute_crc_data_block, get_positions_from_hashed_key, hash_key};
use crate::lsm::{AVL, BloomFilter, SparseIndex, SsTableDataBlock};
use crate::{
    errors::{DataCorruptedErr, DbError, Result},
    lsm::{SSTable, VALUE_MAX_BYTES_SIZE},
};

//Comes from SSTable struct
struct CompactionSstSlice {
    file_path: PathBuf,
    sparse_index: Arc<Vec<(Vec<u8>, u64, u64)>>,
    sparse_index_curr_position: usize,
}

impl CompactionSstSlice {
    fn new(file_path: PathBuf, sparse_index: Arc<Vec<(Vec<u8>, u64, u64)>>) -> Self {
        Self {
            file_path,
            sparse_index,
            sparse_index_curr_position: 0,
        }
    }

    fn next_sparse_index_entry(&mut self) -> Option<&(Vec<u8>, u64, u64)> {
        let entry = self.sparse_index.get(self.sparse_index_curr_position);
        if entry.is_some() {
            self.sparse_index_curr_position += 1;
        }
        entry
    }
}
pub enum CompactionThreadResponse {
    Success(SSTable),
    Error(DbError),
}

struct MergeItem {
    entry: HeapEntry,
    source: usize, // should be the index of CompactionFileElement and we use that to grab the next entry
}

impl MergeItem {
    fn new(cfe: &mut CompactionFileElement, index: usize) -> Result<Option<Self>> {
        loop {
            // loop runs at most twice or return Err(e)
            if cfe.is_data_block_exhausted() {
                match cfe.advance_to_next_data_block() {
                    Ok(()) => {} // fall through
                    Err(DbError::DataBlockExhausted) => return Ok(None),
                    Err(e) => return Err(e),
                }
            }
            match cfe.advance_data_block_header() {
                Ok(Some(heap)) => {
                    return Ok(Some(MergeItem {
                        entry: heap,
                        source: index,
                    }));
                }
                Err(DbError::Io(e)) if e.kind() == UnexpectedEof => cfe.current_data_block = None,
                Ok(None) => cfe.current_data_block = None,
                Err(e) => return Err(e),
            }
        }
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
    sst_slice: CompactionSstSlice,
    reader: BufReader<File>,
    current_data_block: Option<SsTableDataBlock>,
}

struct HeapEntry {
    key: Vec<u8>,
    timestamp: u64,
    deleted: bool,
    vsz: u64,
}

//TODOs for CompactionFileElement:
/*
change the apis of the function to work with the already fetched & verified data blocks instead of reading from file
Add a fetch_data_block function that grabs a whole datablock from the open file

*/
impl CompactionFileElement {
    fn new(sst_slice: CompactionSstSlice) -> Result<Self> {
        let file = File::open(&sst_slice.file_path)?;
        let reader = BufReader::with_capacity(65536, file);
        Ok(Self {
            sst_slice,
            reader,
            current_data_block: None,
        })
    }
    fn is_data_block_exhausted(&self) -> bool {
        self.current_data_block.is_none()
    }
    fn advance_data_block_header(&mut self) -> Result<Option<HeapEntry>> {
        // TODO: if none is returned, let the caller fetch another data_block, then try again

        let mut tstamp = [0u8; 8];
        let mut ksz = [0u8; 8];
        let mut vsz = [0u8; 8];
        let mut tmbstone = [0u8; 1];
        if let Some(curr_data_block) = self.current_data_block.as_mut() {
            match curr_data_block.bytes.read_exact(&mut tstamp) {
                Ok(()) => {} // fall through

                Err(e) => {
                    self.current_data_block = None; // so that when we call .is_data_block_exhausted() we return true and we fetch a new one
                    return Err(DbError::Io(e));
                }
            }
            curr_data_block.bytes.read_exact(&mut ksz)?;
            let key_size = u64::from_le_bytes(ksz);
            let mut key = vec![0u8; key_size as usize];
            curr_data_block.bytes.read_exact(&mut vsz)?;
            curr_data_block.bytes.read_exact(&mut tmbstone)?;

            let curr_offset = curr_data_block.bytes.stream_position()?;

            let deleted = match tmbstone[0] {
                0xFF => true,
                0x00 => false,
                _ => {
                    return Err(DbError::DataCorrupted(DataCorruptedErr {
                        offset: curr_offset,
                        file_path: self.sst_slice.file_path.clone(),
                        reason: crate::errors::CorruptionType::TombstoneCorrupted {
                            found: tmbstone[0],
                        },
                    }));
                }
            };
            curr_data_block.bytes.read_exact(&mut key)?;
            Ok(Some(HeapEntry {
                key: key.to_vec(),
                timestamp: u64::from_le_bytes(tstamp),
                deleted,
                vsz: u64::from_le_bytes(vsz),
            }))
        } else {
            Ok(None)
        }
    }

    fn consume_val_from_data_block(&mut self, heap_entry: &HeapEntry) -> Result<Vec<u8>> {
        // only called when theres a value
        let vsz = heap_entry.vsz;
        let curr_offset = self.reader.stream_position()?; // TODO: This is the stream_position from the reader, but its not accurate to where we are when we take into account the current data block, keep track accordingly

        if vsz > VALUE_MAX_BYTES_SIZE {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: curr_offset, // problem:  how do I get offset now? stream_position above is incorrect. Maybe just track it manually in a var
                file_path: self.sst_slice.file_path.clone(),
                reason: crate::errors::CorruptionType::KeyValueRecordExceedsMaxLength {
                    max: VALUE_MAX_BYTES_SIZE,
                    found: vsz,
                },
            }));
        }
        let mut val = vec![0u8; vsz as usize];
        if let Some(curr_data_block) = self.current_data_block.as_mut() {
            curr_data_block.bytes.read_exact(&mut val)?; // 
        }

        Ok(val.to_vec())
    }
    fn advance_to_next_data_block(&mut self) -> Result<()> {
        // TODO: ensure data doesnt exceed max
        // TODO: Account for unexpectedEOF -> done with file
        // Only call when curR_data_block is None
        // next_sparse_index should be able to yield None -> no more data return early

        let (key, offset, data_len) = {
            let entry = self
                .next_sparse_index()
                .ok_or(DbError::DataBlockExhausted)?;
            (&entry.0, entry.1, entry.2)
        };
        let mut new_data_block = SsTableDataBlock::new(key); // doing this first because of borrowing issues with self
        let mut bytes = vec![0u8; data_len as usize];
        let mut crc = [0u8; 4];
        self.reader.read_exact(&mut bytes)?; // check for unexpectedeof err -> file is done
        self.reader.read_exact(&mut crc)?;
        let crc_to_check = compute_crc_data_block(&bytes);
        let crc_from_buff = u32::from_le_bytes(crc);
        // Todo: the crc check/throw error needs to be put in a function, gets reused a lot
        // have the caller account for this error
        if crc_to_check != crc_from_buff {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: offset + data_len,
                file_path: self.sst_slice.file_path.to_path_buf(),
                reason: CorruptionType::CrcMismatch {
                    expected: crc_to_check,
                    found: crc_from_buff,
                },
            }));
        };

        new_data_block.append_to_block(&bytes); // whole datablock

        self.current_data_block = Some(new_data_block);
        Ok(())
    }

    fn next_sparse_index(&mut self) -> Option<&(Vec<u8>, u64, u64)> {
        self.sst_slice.next_sparse_index_entry()
    }

    fn take_value_and_advance(&mut self, heap_entry: &HeapEntry) -> Result<Vec<u8>> {
        //
        let val = self.consume_val_from_data_block(heap_entry)?;

        // self.advance_data_block_header()?;

        Ok(val)
    }

    fn skip_value_and_advance(&mut self, vsz: u64) -> Result<()> {
        // only call when a HeapEntryExists

        let curr_offset = self.reader.stream_position()?;

        if vsz > VALUE_MAX_BYTES_SIZE {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: curr_offset,
                file_path: self.sst_slice.file_path.clone(),
                reason: crate::errors::CorruptionType::KeyValueRecordExceedsMaxLength {
                    max: VALUE_MAX_BYTES_SIZE,
                    found: vsz,
                },
            }));
        }

        if let Some(curr_db) = self.current_data_block.as_mut() {
            curr_db.bytes.seek_relative(vsz as i64)?; // skips value from current position in the Cursor
        }

        // self.advance_data_block_header()?;
        Ok(())
    }
}

struct CompactionManager {
    tx: Sender<CompactionThreadResponse>,
    rx: Receiver<CompactionThreadResponse>,
    file_handles: Option<Vec<CompactionFileElement>>,
    heap: Option<BinaryHeap<Reverse<MergeItem>>>,
}

impl CompactionManager {
    // have main have a select_files_for_compaction function -> Vec<PathBuf>
    fn new(files: Vec<CompactionSstSlice>) -> Result<Self> {
        let (tx, rx) = mpsc::channel::<CompactionThreadResponse>();
        let mut cfe_vec: Vec<CompactionFileElement> = Vec::with_capacity(files.len());

        let mut heap: BinaryHeap<Reverse<MergeItem>> = BinaryHeap::with_capacity(cfe_vec.len());

        for (i, x) in files.into_iter().enumerate() {
            let mut cfe = match CompactionFileElement::new(x) {
                Ok(compact_el) => compact_el,
                Err(_) => continue, // file is corrupt?
            };
            let idx = cfe_vec.len(); // THe index that the cfe is about to take
            let m_item = match MergeItem::new(&mut cfe, idx) {
                Ok(m_item) => m_item,
                Err(_) => continue,
            };

            if let Some(m) = m_item {
                heap.push(Reverse(m));
            }

            cfe_vec.push(cfe);
        }

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
            match CompactionManager::merge_to_final(heap, &mut writer, &mut cfe_vec) {
                Ok(()) => {
                    // TODO: Either communicate all of the potential errors via tx.send or use a join handle
                    // join handle would block the thread so lets just handle everything here explicitly and send errors to the main channel
                    let f = writer.into_inner().map_err(|e| {
                        DbError::FileError(
                            format!("Failed to extract File from BufWriter: {}", e.error()),
                            sst_paths.0.to_path_buf(),
                        )
                    })?;
                    f.sync_all()?;

                    fs::rename(&sst_paths.0, &sst_paths.1)?;
                    if let Some(dir) = sst_paths.1.parent() {
                        // always should have parent
                        File::open(dir)?.sync_all()?;
                    }

                    if let Ok(sst) = SSTable::load(&sst_paths.1).map_err(|e| {
                        let _ = tx.send(CompactionThreadResponse::Error(e));
                    }) {
                        let _ = tx.send(CompactionThreadResponse::Success(sst));
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
        mut heap: BinaryHeap<Reverse<MergeItem>>,
        writer: &mut BufWriter<File>,
        cfe_vec: &mut [CompactionFileElement],
    ) -> Result<()> {
        let mut hashed_keys: Vec<u128> = Vec::new();
        let mut last_k_written: Option<Vec<u8>> = None;
        let mut data_block: Option<SsTableDataBlock> = None;
        let mut new_sparse_index = SparseIndex::new();
        let mut offset: u64 = 0;
        let min_k = heap
            .peek()
            .ok_or(DbError::DataBlockExhausted)? // TODO HERE: Either return a better specific error or just return a Ok(None) meaning nothing to do
            .0
            .entry
            .key
            .clone(); // grab min key before we start
        let mut max_k: Vec<u8> = vec![];

        while !heap.is_empty() {
            if let Some(curr_merge_item) = heap.pop().as_mut() {
                // ^ never fails
                let cfe = cfe_vec.get_mut(curr_merge_item.0.source).unwrap();

                if last_k_written.as_ref() == Some(&curr_merge_item.0.entry.key) {
                    cfe.skip_value_and_advance(curr_merge_item.0.entry.vsz)?;
                } else {
                    hashed_keys.push(hash_key(&curr_merge_item.0.entry.key));
                    let record = curr_merge_item.0.serialize_record(cfe)?;

                    if let Some(db) = data_block.as_mut() {
                        // if heap.is_empty() {
                        //     // not needed to grab max here, last key/s will always fall in the data_block out side
                        //     max_k = db.grab_max_key_from_data_block()?;
                        // }
                        match db.is_finished() {
                            true => {
                                let owned_ss_data_block =
                                    data_block.take().expect("Expected a SsTableDataBlock");
                                let data_len = owned_ss_data_block.bytes.get_ref().len() as u64; // before 4 byte crc

                                let full = owned_ss_data_block.full_data_block();
                                writer.write_all(full.bytes.get_ref())?;
                                new_sparse_index.add_entry(&full.starting_key, data_len, offset);
                                offset += full.bytes.get_ref().len() as u64;
                                // create new data block for next iteration
                                let mut new_ss_db =
                                    SsTableDataBlock::new(&curr_merge_item.0.entry.key);
                                new_ss_db.append_to_block(&record); // last record always ends up in the data_block outside of this loop which means thatgrab_max_key_from_data_block needs to only be called there
                                data_block = Some(new_ss_db);
                            }
                            false => {
                                db.append_to_block(&record);
                            }
                        }
                    } else {
                        let mut new_ss_db = SsTableDataBlock::new(&curr_merge_item.0.entry.key);
                        new_ss_db.append_to_block(&record);
                        data_block = Some(new_ss_db);
                    }
                    let mut k = vec![];
                    mem::swap(&mut curr_merge_item.0.entry.key, &mut k); // swapping with empty vec so that I dont copy vec below
                    last_k_written = Some(k);
                };

                // any of the errors here are just skipped, data corrupted -> skip, end of file -> skip, none -> skip
                let _ = MergeItem::new(cfe, curr_merge_item.0.source).map(|x| {
                    if let Some(x) = x {
                        heap.push(Reverse(x));
                    }
                }); // if err we simply skip without adding anything to heap
            }
        }

        if let Some(mut last_db) = data_block {
            let len = last_db.bytes.get_ref().len() as u64;
            max_k = last_db.grab_max_key_from_data_block()?;

            let full = last_db.full_data_block();
            writer.write_all(full.bytes.get_ref())?;

            new_sparse_index.add_entry(&full.starting_key, len, offset);

            offset += full.bytes.get_ref().len() as u64; // length here is the start of sparse_index // 
        }

        let mut bloom_filter = BloomFilter::new(hashed_keys.len() * 10);
        hashed_keys.iter().for_each(|h_key| {
            let positions = get_positions_from_hashed_key(*h_key, bloom_filter.num_bits as usize);
            bloom_filter.set_bits(positions);
        });
        let footer = AVL::serialize_sstable_footer(
            &mut offset,
            &min_k,
            &max_k,
            new_sparse_index.index_entries.len() as u64,
            (bloom_filter.bits.len() * 8) as u64,
        );

        writer.write_all(&new_sparse_index.index_entries)?;
        for word in &bloom_filter.bits {
            writer.write_all(&word.to_le_bytes())?;
        }
        writer.write_all(&footer)?;

        writer.flush()?;
        writer.get_mut().sync_all()?;
        Ok(())
    }
}

impl Ord for MergeItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.entry
            .key
            .cmp(&other.entry.key) // smaller wins
            .then_with(|| other.entry.timestamp.cmp(&self.entry.timestamp)) // larger wins(newer)
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

/*
TODOs:
NOT DONE YET: Handle all errors
NOT DONE YET: Perform compaction using multiple threads, split work into subCompactionJob where each thread works on specific slices of the input files
NOT DONE YET: need pickFilesForCompaction function
NOT DONE YET: needs pickSubSlicesOfFilesForCompaction // picks ranges(of each file) for each thread to work on.
NOT DONE YET Ensure output files have no overlapping keys, this is easy since thread will work from min range of file1 to max range of file k


*/
