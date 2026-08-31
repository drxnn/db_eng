use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use std::fs::{self, OpenOptions};

use std::io::ErrorKind::UnexpectedEof;
use std::io::{Seek, SeekFrom, Write};

use std::path::Path;
use std::sync::Arc;
use std::thread::spawn;

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read},
    path::PathBuf,
    sync::mpsc::{self, Receiver, Sender},
};
use std::{mem, todo};

use crate::errors::CompactionErr::{self, HeapNotFound};
use crate::errors::CorruptionType::{self, TruncatedRecord};
use crate::helpers::{
    check_key_value_record_does_not_exceed_max, compute_crc_data_block, create_new_data_file,
    get_positions_from_hashed_key, hash_key,
};
use crate::lsm::{
    AVL, BloomFilter, DATA_BLOCK_MAX_BYTES_SIZE, MAX_SST_SIZE, SparseIndex, SsTableDataBlock,
};
use crate::{
    errors::{DataCorruptedErr, DbError, Result},
    lsm::{KEY_MAX_BYTES_SIZE, SSTable, VALUE_MAX_BYTES_SIZE},
};

//Comes from SSTable struct
struct CompactionSstSlice {
    file_path: PathBuf,
    sparse_index: Arc<Vec<(Vec<u8>, u64, u64)>>,
    sparse_index_curr_position: usize,
}

struct SstFinalizer {
    writer: BufWriter<File>,
    hashed_keys: Vec<u128>,
    sparse_index: SparseIndex,
    data_block: SsTableDataBlock,
    sst_paths: (PathBuf, PathBuf), // file that Bufwriter holds
    offset: u64,
    min_key: Vec<u8>, // max_key can be obtained by doing data_block.grab_max_key() at the end
    bytes_written_to_file: u64,
}

impl SstFinalizer {
    fn new(dir: &Path, starting_key: Vec<u8>) -> Result<Self> {
        let (file, tmp_file, final_file) = create_new_data_file(dir)?;
        let writer = BufWriter::new(file);
        Ok(Self {
            writer,
            hashed_keys: Vec::new(),
            sparse_index: SparseIndex::new(),
            data_block: SsTableDataBlock::new(&starting_key),
            min_key: starting_key,
            sst_paths: (tmp_file, final_file),
            offset: 0,
            bytes_written_to_file: 0,
        })
    }

    fn would_exceed_max_sst_size(&self, record_len: u64) -> bool {
        self.bytes_written_to_file + record_len > MAX_SST_SIZE
    }
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
    Success(CompactionOutcome),
    Error(DbError),
}

struct MergeItem {
    entry: HeapEntry,
    source: usize, // should be the index of CompactionFileElement and we use that to grab the next entry
}

impl MergeItem {
    fn new(cfe: &mut CompactionFileElement, index: usize) -> Result<Option<Self>> {
        loop {
            if cfe.is_data_block_exhausted() {
                match cfe.advance_to_next_data_block() {
                    Ok(()) => {} // fall through
                    Err(DbError::DataBlockExhausted) => return Ok(None),
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            match cfe.advance_data_block_header() {
                Ok(Some(heap)) => {
                    return Ok(Some(MergeItem {
                        entry: heap,
                        source: index,
                    }));
                }

                Ok(None) => cfe.current_data_block = None,
                Err(e) => return Err(e),
            }
        }
    }
    fn serialize_record(&mut self) -> Result<Vec<u8>> {
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
        let value = &self.entry.value;

        // let value = cfe.take_value_and_advance(&self.entry)?;
        let record = [&tstamp, &ksz, &vsz, &deleted[..], key, value].concat();
        Ok(record)
    }
}

struct CompactionFileElement {
    sst_slice: CompactionSstSlice,
    reader: BufReader<File>,
    current_data_block: Option<SsTableDataBlock>,
    curr_offset_from_file: u64,
}

struct HeapEntry {
    key: Vec<u8>,
    timestamp: u64,
    deleted: bool,
    vsz: u64,
    value: Vec<u8>,
}

impl CompactionFileElement {
    fn new(sst_slice: CompactionSstSlice) -> Result<Self> {
        let file = File::open(&sst_slice.file_path)?;
        let reader = BufReader::with_capacity(65536, file);
        Ok(Self {
            sst_slice,
            reader,
            current_data_block: None,
            curr_offset_from_file: 0,
        })
    }
    fn is_data_block_exhausted(&self) -> bool {
        self.current_data_block.is_none()
    }

    fn read_exact_or_corrupt(
        reader: &mut impl Read,
        buf: &mut [u8],
        offset: u64,
        file_path: &Path,
    ) -> Result<()> {
        // if we get a truncated record, we have corrupted data
        reader.read_exact(buf).map_err(|e| {
            if e.kind() == UnexpectedEof {
                DbError::DataCorrupted(DataCorruptedErr {
                    offset,
                    file_path: file_path.to_path_buf(),
                    reason: TruncatedRecord,
                })
            } else {
                DbError::Io(e)
            }
        })
    }

    fn advance_data_block_header(&mut self) -> Result<Option<HeapEntry>> {
        let mut tstamp = [0u8; 8];
        let mut ksz = [0u8; 8];
        let mut vsz = [0u8; 8];
        let mut tmbstone = [0u8; 1];

        if let Some(curr_data_block) = self.current_data_block.as_mut() {
            if curr_data_block.bytes.position() == curr_data_block.bytes.get_ref().len() as u64 {
                return Ok(None); // we are at the end of data block, fetch new
            }
            let file_path = &self.sst_slice.file_path;

            // if any read_exact_or_corrupt calls return UnexpectedEof, then we have a truncated error and we should throw data block and remainder of file away
            Self::read_exact_or_corrupt(
                &mut curr_data_block.bytes,
                &mut tstamp,
                self.curr_offset_from_file,
                file_path,
            )?;

            self.curr_offset_from_file += 8;
            Self::read_exact_or_corrupt(
                &mut curr_data_block.bytes,
                &mut ksz,
                self.curr_offset_from_file,
                file_path,
            )?;
            let key_size = u64::from_le_bytes(ksz);

            check_key_value_record_does_not_exceed_max(
                key_size,
                KEY_MAX_BYTES_SIZE,
                self.curr_offset_from_file,
                file_path,
            )?;
            self.curr_offset_from_file += 8;

            Self::read_exact_or_corrupt(
                &mut curr_data_block.bytes,
                &mut vsz,
                self.curr_offset_from_file,
                file_path,
            )?;

            let val_size = u64::from_le_bytes(vsz);
            check_key_value_record_does_not_exceed_max(
                val_size,
                VALUE_MAX_BYTES_SIZE,
                self.curr_offset_from_file,
                file_path,
            )?;
            self.curr_offset_from_file += 8;
            Self::read_exact_or_corrupt(
                &mut curr_data_block.bytes,
                &mut tmbstone,
                self.curr_offset_from_file,
                file_path,
            )?;
            self.curr_offset_from_file += 1;

            let deleted = match tmbstone[0] {
                0xFF => true,
                0x00 => false,
                _ => {
                    return Err(DbError::DataCorrupted(DataCorruptedErr {
                        offset: self.curr_offset_from_file - 1, // should point to the byte where tombstone begins
                        file_path: self.sst_slice.file_path.clone(),
                        reason: crate::errors::CorruptionType::TombstoneCorrupted {
                            found: tmbstone[0],
                        },
                    }));
                }
            };

            let mut key = vec![0u8; key_size as usize];
            Self::read_exact_or_corrupt(
                &mut curr_data_block.bytes,
                &mut key,
                self.curr_offset_from_file,
                file_path,
            )?;
            self.curr_offset_from_file += key_size;

            let mut val = vec![0u8; val_size as usize];
            Self::read_exact_or_corrupt(
                &mut curr_data_block.bytes,
                &mut val,
                self.curr_offset_from_file,
                &self.sst_slice.file_path,
            )?;
            self.curr_offset_from_file += val_size;
            Ok(Some(HeapEntry {
                key: key.to_vec(),
                timestamp: u64::from_le_bytes(tstamp),
                deleted,
                vsz: val_size,
                value: val.to_vec(),
            }))
        } else {
            Ok(None)
        }
    }

    fn advance_to_next_data_block(&mut self) -> Result<()> {
        let file_path = self.sst_slice.file_path.clone();
        let (key, offset, data_len) = {
            let entry = self
                .next_sparse_index()
                .ok_or(DbError::DataBlockExhausted)?;
            (&entry.0, entry.1, entry.2)
        };
        // IF WE GET a sparse_index_entry, that means there should be data in the file
        // if an error is thrown after this, it means data is corrupted or truncated(which also means this particular datablock is corrupted) and we are done with this file
        let mut new_data_block = SsTableDataBlock::new(key);

        let reader = &mut self.reader;

        check_key_value_record_does_not_exceed_max(
            data_len,
            DATA_BLOCK_MAX_BYTES_SIZE,
            offset,
            &file_path,
        )?;

        // This reassignment below accounts for the 4 bytes of the CRC that the data_block_len doesnt account for.
        // Instead of adding 4 to the curr_offset, we just reassign to the next datablock offset start
        self.curr_offset_from_file = offset;
        reader.seek(SeekFrom::Start(offset))?;

        let mut bytes = vec![0u8; data_len as usize];
        let mut crc = [0u8; 4];
        Self::read_exact_or_corrupt(reader, &mut bytes, self.curr_offset_from_file, &file_path)?;
        self.curr_offset_from_file += data_len;

        Self::read_exact_or_corrupt(reader, &mut crc[..], self.curr_offset_from_file, &file_path)?;

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
}

struct CompactionManager {
    tx: Sender<CompactionThreadResponse>,
    rx: Receiver<CompactionThreadResponse>,
    file_handles: Option<Vec<CompactionFileElement>>,
    heap: Option<BinaryHeap<Reverse<MergeItem>>>,
    compaction_outcome: Option<CompactionOutcome>,
    data_dir: PathBuf,
}
#[derive(Default)]
pub struct CompactionOutcome {
    pub final_sst_files: Vec<(PathBuf, PathBuf)>, // (tmp_file, final_file). Tmp holds the data, atomically rename to final
    pub consumed_sst_files: Vec<PathBuf>,         // files that were completely merged
    pub partially_consumed_sst_files: Vec<(PathBuf, DbError)>, // files that were partially merged but then stumbled upon corrupted data
    pub skipped_sst_files: Vec<(PathBuf, DbError)>, // files were skipped because they threw an error during new(), data is most likely corrupted, main can decide what to do with these depending on the error, maybe the File::open() failed for some reason which doesnt mean data is corrupted
}

impl CompactionOutcome {
    pub fn new() -> Self {
        Self {
            final_sst_files: Vec::new(),
            consumed_sst_files: Vec::new(),
            partially_consumed_sst_files: Vec::new(),
            skipped_sst_files: Vec::new(),
        }
    }
}

impl CompactionManager {
    // have main have a select_files_for_compaction function -> Vec<PathBuf>
    fn new(files: Vec<CompactionSstSlice>, data_dir: PathBuf) -> Result<Self> {
        let mut cmpt_outcome = CompactionOutcome::new();
        let (tx, rx) = mpsc::channel::<CompactionThreadResponse>();
        let mut cfe_vec: Vec<CompactionFileElement> = Vec::with_capacity(files.len());

        let mut heap: BinaryHeap<Reverse<MergeItem>> = BinaryHeap::with_capacity(files.len());

        for x in files.into_iter() {
            let x_path = x.file_path.clone();
            let mut cfe = match CompactionFileElement::new(x) {
                Ok(compact_el) => compact_el,
                Err(e) => {
                    cmpt_outcome.skipped_sst_files.push((x_path, e)); // have main check what error is, could be DbError:TooManyFilesOpenInProcess so main can decide what to do
                    continue;
                }
            };

            let idx = cfe_vec.len(); // THe index that the cfe is about to take
            match MergeItem::new(&mut cfe, idx) {
                Ok(m_item) => {
                    if let Some(m) = m_item {
                        heap.push(Reverse(m));
                    } else {
                        cmpt_outcome
                            .consumed_sst_files
                            .push(cfe.sst_slice.file_path.clone());
                    }
                }
                Err(e) => {
                    cmpt_outcome.skipped_sst_files.push((x_path, e)); // main can handle
                    // we continue meaning cfe doesnt get pushed to vec
                    continue;
                } // read above
            };

            cfe_vec.push(cfe);
        }

        Ok(Self {
            tx,
            rx,
            file_handles: Some(cfe_vec),
            heap: Some(heap),
            compaction_outcome: Some(cmpt_outcome),
            data_dir,
        })
    }

    fn background_compact(&mut self) -> Result<()> {
        let tx: Sender<CompactionThreadResponse> = self.tx.clone();

        // TODO: let sst_paths get generated in merge_to_final since we might have to output multiple ssts
        // so return the finalized merged files(both tmp and final and loop them to rename them atomically)
        // so merge_to_final should check whether the current open sst final file has exceeded max size(metadata footer not included)
        // if yes, sync everything, add tmp_path and final_path to a vector to return to background_compact
        // then create new paths create_new_data_file, mutate the writer to wrap the new file
        // loop this

        // TODO: dont pass sst paths, have merge_to_final open the files and if it exceeds MAX_SST_SIZE, then open a new sst file and continue merging the inputs therex

        // this error should actually never happen, heap gets built during new() and if new returns, it will always be populated
        let Some(heap) = self.heap.take() else {
            let _ = tx.send(CompactionThreadResponse::Error(DbError::CompactionError(
                HeapNotFound,
            )));
            return Err(DbError::CompactionError(HeapNotFound));
        };

        // same as above, should not happen
        let Some(mut cfe_vec) = self.file_handles.take() else {
            let _ = tx.send(CompactionThreadResponse::Error(DbError::CompactionError(
                CompactionErr::EmptyCompactionFileElementCollection,
            )));
            return Err(DbError::CompactionError(
                CompactionErr::EmptyCompactionFileElementCollection,
            ));
        };
        let cmpt_outcome = self.compaction_outcome.take().unwrap_or_default();

        let data_directory = self.data_dir.clone();

        spawn(move || -> Result<()> {
            let compaction_result = (|| -> Result<CompactionOutcome> {
                let cmpt = CompactionManager::merge_to_final(
                    cmpt_outcome,
                    heap,
                    &mut cfe_vec,
                    data_directory,
                )?;

                cmpt.final_sst_files.iter().try_for_each(
                    |sst_paths: &(PathBuf, PathBuf)| -> Result<()> {
                        fs::rename(&sst_paths.0, &sst_paths.1)?;
                        if let Some(dir) = sst_paths.1.parent() {
                            File::open(dir)?.sync_all()?;
                        };
                        // let sst = SSTable::load(&sst_paths.1)?; // should I load the ssts here or let main do it from the paths?
                        // let MAIN DO IT

                        Ok(())
                    },
                )?;

                Ok(cmpt)
            })();

            match compaction_result {
                Ok(cmpt) => {
                    let _ = tx.send(CompactionThreadResponse::Success(cmpt));
                }
                Err(e) => {
                    // LOOP HERE and remove files
                    // OR should I have a separate successful_sst_outputs where merge_to_final does the atomic rename itself and then here we can only get rid of incomplete ones
                    // Problem: there are a lot of different errors that can be returned by compaction_result (or merge_to_final inside compaction_result), it would be better to anticipate every single one(or important failures) and decide what to do depending on that
                    // let _ = fs::remove_file(&sst_paths.1);
                    // let _ = fs::remove_file(&sst_paths.2);
                    let _ = tx.send(CompactionThreadResponse::Error(e));
                }
            }

            Ok(())
        });

        Ok(())
    }

    fn finalize_output_merged_file(mut sst_finalizer: SstFinalizer) -> Result<(PathBuf, PathBuf)> {
        // return tmp and final.sst

        let len = sst_finalizer.data_block.bytes.get_ref().len() as u64;
        let max_k = sst_finalizer.data_block.grab_max_key_from_data_block()?;
        let min_k = sst_finalizer.min_key; // initialized when we create the SstFinalizer

        let full = sst_finalizer.data_block.full_data_block();
        sst_finalizer.writer.write_all(full.bytes.get_ref())?;

        sst_finalizer
            .sparse_index
            .add_entry(&full.starting_key, len, sst_finalizer.offset);

        sst_finalizer.offset += full.bytes.get_ref().len() as u64; // length here is the start of sst_finalizer.sparse_index //

        let mut bloom_filter = BloomFilter::new(sst_finalizer.hashed_keys.len() * 10);
        sst_finalizer.hashed_keys.iter().for_each(|h_key| {
            let positions = get_positions_from_hashed_key(*h_key, bloom_filter.num_bits as usize);
            bloom_filter.set_bits(positions);
        });
        let footer = AVL::serialize_sstable_footer(
            sst_finalizer.offset,
            &min_k,
            &max_k,
            sst_finalizer.sparse_index.index_entries.len() as u64,
            (bloom_filter.bits.len() * 8) as u64,
        );

        sst_finalizer
            .writer
            .write_all(&sst_finalizer.sparse_index.index_entries)?;
        for word in &bloom_filter.bits {
            sst_finalizer.writer.write_all(&word.to_le_bytes())?;
        }
        sst_finalizer.writer.write_all(&footer)?;

        sst_finalizer.writer.flush()?;
        sst_finalizer.writer.get_mut().sync_all()?;

        Ok((sst_finalizer.sst_paths.0, sst_finalizer.sst_paths.1))
    }

    fn merge_to_final(
        mut compaction_outcome: CompactionOutcome,
        mut heap: BinaryHeap<Reverse<MergeItem>>,
        cfe_vec: &mut [CompactionFileElement],
        data_dir: PathBuf,
    ) -> Result<CompactionOutcome> {
        let min_k = heap
            .peek()
            .ok_or(DbError::DataBlockExhausted)? // TODO HERE: Either return a better specific error or just return a Ok(None) meaning nothing to do
            .0
            .entry
            .key
            .clone(); // grab min key before we start
        let mut sst_finalizer = SstFinalizer::new(&data_dir, min_k)?;
        let mut last_k_written: Option<Vec<u8>> = None;

        while let Some(curr_merge_item) = heap.pop().as_mut() {
            if last_k_written.as_ref() != Some(&curr_merge_item.0.entry.key) {
                let record = curr_merge_item.0.serialize_record()?;
                if sst_finalizer.would_exceed_max_sst_size(record.len() as u64) {
                    let finished_ssts = Self::finalize_output_merged_file(sst_finalizer)?;
                    // TODO: add finished file to vector of finished files
                    compaction_outcome.final_sst_files.push(finished_ssts);
                    sst_finalizer =
                        SstFinalizer::new(&data_dir, curr_merge_item.0.entry.key.clone())?; // after 160MB, one sst is done
                }

                sst_finalizer
                    .hashed_keys
                    .push(hash_key(&curr_merge_item.0.entry.key));

                match sst_finalizer.data_block.is_finished() {
                    true => {
                        //TODO: can be a function
                        let mut new_ss_db = SsTableDataBlock::new(&curr_merge_item.0.entry.key);
                        new_ss_db.append_to_block(&record);
                        let old_data_block = mem::replace(&mut sst_finalizer.data_block, new_ss_db);

                        let data_len = old_data_block.bytes.get_ref().len() as u64; // before 4 byte crc

                        let full = old_data_block.full_data_block();
                        sst_finalizer.writer.write_all(full.bytes.get_ref())?;
                        sst_finalizer.sparse_index.add_entry(
                            &full.starting_key,
                            data_len,
                            sst_finalizer.offset,
                        );
                        let full_bytes_len = full.bytes.get_ref().len() as u64;
                        sst_finalizer.offset += full_bytes_len;
                        sst_finalizer.bytes_written_to_file += full_bytes_len;
                    }
                    false => {
                        sst_finalizer.data_block.append_to_block(&record);
                    }
                }

                let mut k = vec![];
                mem::swap(&mut curr_merge_item.0.entry.key, &mut k); // swapping with empty vec so that I dont copy vec below
                last_k_written = Some(k);
            };

            let cfe = cfe_vec.get_mut(curr_merge_item.0.source).expect(
                "source index is always valid: cfe_vec is append-only and idx is assigned pre push",
            );
            // if item is the same as last one, we are skipping it because the newer key has already been written to final file
            let _ = MergeItem::new(cfe, curr_merge_item.0.source)
                .map(|x| {
                    match x {
                        Some(m) => heap.push(Reverse(m)),
                        None => {
                            compaction_outcome
                                .consumed_sst_files
                                .push(cfe.sst_slice.file_path.clone());
                            // DATA BLOCK EXHAUSTED -> fully_consumed vec
                        } // if this returns none, file has been fully read and we push it to the fully_consumed_file vector
                    }
                })
                .map_err(|e| {
                    compaction_outcome
                        .partially_consumed_sst_files
                        .push((cfe.sst_slice.file_path.clone(), e))
                });
        }

        // use herre Self::finalize_output_merged_file(sst_finalizer)
        let sst_paths = Self::finalize_output_merged_file(sst_finalizer)?;
        // put paths into vec
        compaction_outcome.final_sst_files.push(sst_paths);

        Ok(compaction_outcome)
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
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for MergeItem {}

/*
TODOs:
NOT DONE YET: Handle all errors
NOD DONE YET(IMPORTANT): We need to ensure that the output files that are created after the merge, dont get prioritized over the other sstables by the timestamp id otherwise we might have older data take precedence over newer data
NOT DONE YET: Perform compaction using multiple threads, split work into subCompactionJob where each thread works on specific slices of the input files
NOT DONE YET: need pickFilesForCompaction function(before we do this, we should separate directories into L0, L1, L2, L3 etc)
NOT DONE YET: needs pickSubSlicesOfFilesForCompaction // picks ranges(of each file) for each thread to work on.
NOT DONE YET Ensure output files have no overlapping keys, this is easy since thread will work from min range of file1 to max range of file k
NOT DONE YET: Fix all unwraps in lsm.rs
NOT DONE YET: Modularize the code
NOT DONE YET: Extract some duplicate functionality into their own functions
NOT DONE YET(Important): If we are at the bottom level of ssts, deleted records do not have to be pushed to the final merged_file, instead they are really deleted.
NOT DONE YET: Compress bytes. check lz4 library for that

*/
