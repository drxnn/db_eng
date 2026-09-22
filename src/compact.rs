use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use std::fs::{self};

use std::io::{Seek, SeekFrom, Write};

use std::path::Path;
use std::sync::Arc;
use std::thread::{JoinHandle, spawn};

use std::mem;
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::PathBuf,
};

use crate::errors::CompactionErr::{self};

use crate::errors::CrcType;
use crate::helpers::{
    CRC32, check_crc, check_key_value_record_does_not_exceed_max, create_new_data_file,
    get_positions_from_hashed_key, hash_key, read_exact_or_corrupt,
};
use crate::lsm::{
    AVL, BloomFilter, DATA_BLOCK_MAX_BYTES_SIZE, Hlc, MAX_SST_SIZE, SparseIndex, SsTableDataBlock,
};
use crate::{
    errors::{DataCorruptedErr, DbError, Result},
    lsm::{KEY_MAX_BYTES_SIZE, VALUE_MAX_BYTES_SIZE},
};

//Comes from SSTable struct
pub struct CompactionSstSlice {
    file_path: PathBuf,
    sparse_index: Arc<Vec<(Vec<u8>, u64, u64)>>,
    sparse_index_curr_position: usize,
    level: u8,
}

struct SstFinalizer {
    level: u8,
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
    fn new(dir: &Path, starting_key: Vec<u8>, hlc: &Hlc, lvl: u8) -> Result<Self> {
        // PROBLEM: Our OUTPUT Sst file has the newest hlc regardless of the data thats in there,
        // could be old data thats being compacted and now its the newest data
        // Do: just use the highest HLC of the files being compacted into this one
        // The Hlc of the SST is calculated at the time of flushing which means the Hlc is > than the biggest Hlc of the max key in the sst
        // Bigger Hlc == more recent
        // we are also going up a level so we need to make sure the key range in the newly compacted sst is correctly ordered with the other sst files in that level
        // ALSO TODO: Add another metadata byte in the ssts, Level:
        // File picker for compaction: pick one file in L_n, then find all the overlapping ssts in L_(n+1) and compact all of these together.
        // output gets placed in L(n+1)
        // in L0, compact every L0 wit every L1 sst
        //

        let (file, tmp_file, final_file) = create_new_data_file(dir, hlc.tick())?;
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
            level: lvl,
        })
    }

    fn would_exceed_max_sst_size(&self, record_len: u64) -> bool {
        self.bytes_written_to_file + record_len > MAX_SST_SIZE
    }
}

impl CompactionSstSlice {
    pub fn new(file_path: PathBuf, sparse_index: Arc<Vec<(Vec<u8>, u64, u64)>>, level: u8) -> Self {
        Self {
            file_path,
            sparse_index,
            sparse_index_curr_position: 0,
            level,
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

        let record = [&tstamp, &ksz, &vsz, &deleted[..], key, value].concat();
        Ok(record)
    }
}

struct CompactionFileElement {
    sst_slice: CompactionSstSlice,
    reader: BufReader<File>,
    reader_pos: u64,
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
            reader_pos: 0,
            curr_offset_from_file: 0,
        })
    }
    fn is_data_block_exhausted(&self) -> bool {
        self.current_data_block.is_none()
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
            read_exact_or_corrupt(
                &mut curr_data_block.bytes,
                &mut tstamp,
                self.curr_offset_from_file,
                file_path,
            )?;

            self.curr_offset_from_file += 8;
            read_exact_or_corrupt(
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

            read_exact_or_corrupt(
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
            read_exact_or_corrupt(
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
                        offset: self.curr_offset_from_file - 1, //  point to the byte where tombstone begins
                        file_path: self.sst_slice.file_path.clone(),
                        reason: crate::errors::CorruptionType::TombstoneCorrupted {
                            found: tmbstone[0],
                        },
                    }));
                }
            };

            let mut key: Vec<u8> = vec![0u8; key_size as usize];
            read_exact_or_corrupt(
                &mut curr_data_block.bytes,
                &mut key,
                self.curr_offset_from_file,
                file_path,
            )?;
            self.curr_offset_from_file += key_size;

            let mut val: Vec<u8> = vec![0u8; val_size as usize];
            read_exact_or_corrupt(
                &mut curr_data_block.bytes,
                &mut val,
                self.curr_offset_from_file,
                &self.sst_slice.file_path,
            )?;
            self.curr_offset_from_file += val_size;
            Ok(Some(HeapEntry {
                key,
                timestamp: u64::from_le_bytes(tstamp),
                deleted,
                vsz: val_size,
                value: val,
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
        if self.reader_pos != offset {
            // if reader_pos has drifted somehow, only then we seek
            reader.seek(SeekFrom::Start(offset))?;
            self.reader_pos = offset;
        }

        let mut bytes = vec![0u8; data_len as usize];
        let mut crc = [0u8; 4];
        read_exact_or_corrupt(reader, &mut bytes, self.curr_offset_from_file, &file_path)?;
        self.curr_offset_from_file += data_len;

        read_exact_or_corrupt(reader, &mut crc[..], self.curr_offset_from_file, &file_path)?;

        let crc_to_check = CRC32.compute_crc_data_block(&bytes);
        let crc_from_buff = u32::from_le_bytes(crc);
        // Todo: the crc check/throw error needs to be put in a function, gets reused a lot
        // have the caller account for this error

        check_crc(
            crc_to_check,
            crc_from_buff,
            offset,
            &self.sst_slice.file_path,
            CrcType::DataBlock,
        )?;
        self.reader_pos += data_len + 4;
        new_data_block.append_to_block(&bytes); // whole datablock

        self.current_data_block = Some(new_data_block);

        Ok(())
    }

    fn next_sparse_index(&mut self) -> Option<&(Vec<u8>, u64, u64)> {
        self.sst_slice.next_sparse_index_entry()
    }
}

pub struct CompactionManager {
    // if theres a compaction currently running while we have another one just add to queue,
    // then remove from queue when compaction is done // pop
    running_job: Option<JoinHandle<Result<CompactionOutcome>>>,
}

impl CompactionManager {
    pub fn new() -> Self {
        CompactionManager { running_job: None }
    }

    pub fn start(&mut self, job: CompactionJob) -> Result<()> {
        // returns CompactionOutcome
        if self.running_job.is_none() {
            self.running_job = Some(spawn(move || job.run()));
            Ok(())
        } else {
            Err(DbError::CompactionError(
                CompactionErr::CompactionJobAlreadyInFlight,
            ))
        }
        // we can have main run a poll function that
        // takes the join handle, checks is_finished(doesnt block), if yes we join it, if not we put it back
    }
    pub fn is_busy(&self) -> bool {
        self.running_job.is_some()
    }

    pub fn poll(&mut self) -> Option<Result<CompactionOutcome>> {
        let handle = self.running_job.take()?;
        if !handle.is_finished() {
            self.running_job = Some(handle);
            return None;
        }

        match handle.join() {
            Ok(cmpt_outcome) => Some(cmpt_outcome),
            Err(e) => None, // what would be done here?
        }
    }
}
pub struct CompactionJob {
    files: Vec<CompactionSstSlice>,
    data_dir: PathBuf,
    hlc: Arc<Hlc>,
    level_for_output_sst: u8,
}

#[derive(Default)]
pub struct CompactionOutcome {
    pub final_sst_files: Vec<(PathBuf, PathBuf)>, // (tmp_file, final_file). Tmp holds the data, atomically rename to final, tmp is necessary in case of an error during cmpt
    pub consumed_sst_files: Vec<PathBuf>,         // files that were completely merged
    pub level_for_output_sst: u8, // files were skipped because they threw an error during new(), data is most likely corrupted, main can decide what to do with these depending on the error, maybe the File::open() failed for some reason which doesnt mean data is corrupted
}

impl CompactionOutcome {
    pub fn new(level_for_output_sst: u8) -> Self {
        Self {
            final_sst_files: Vec::new(),
            consumed_sst_files: Vec::new(),
            level_for_output_sst,
        }
    }
}

impl CompactionJob {
    pub fn new(
        files: Vec<CompactionSstSlice>,
        level_for_output_sst: u8,
        data_dir: PathBuf,
        hlc: Arc<Hlc>,
    ) -> Self {
        Self {
            files,
            data_dir,
            hlc,
            level_for_output_sst,
        }
    }

    fn run(self) -> Result<CompactionOutcome> {
        let CompactionJob {
            files,
            data_dir,
            hlc,
            level_for_output_sst,
        } = self;
        let mut cmpt_outcome = CompactionOutcome::new(level_for_output_sst);
        let mut cfe_vec: Vec<CompactionFileElement> = Vec::with_capacity(files.len());

        let result = (|| -> Result<()> {
            let mut heap: BinaryHeap<Reverse<MergeItem>> = BinaryHeap::with_capacity(files.len());
            for x in files.into_iter() {
                let mut cfe = CompactionFileElement::new(x)?;

                let idx = cfe_vec.len();
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
                    Err(e) => return Err(e),
                };

                cfe_vec.push(cfe);
            }
            CompactionJob::merge_to_final(&mut cmpt_outcome, heap, &mut cfe_vec, data_dir, hlc)
        })();

        match result {
            Ok(()) => Ok(cmpt_outcome),
            Err(e) => {
                for (tmp, _) in &cmpt_outcome.final_sst_files {
                    let _ = fs::remove_file(tmp);
                }
                Err(e)
            }
        }
    }

    fn finalize_output_merged_file(mut sst_finalizer: SstFinalizer) -> Result<(())> {
        // return final.sst

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

        // Level needs to be provided
        let footer = AVL::serialize_sstable_footer(
            sst_finalizer.offset,
            &min_k,
            &max_k,
            sst_finalizer.sparse_index.index_entries.len() as u64,
            (bloom_filter.bits.len() * 8) as u64,
            sst_finalizer.level,
        );
        // Repeating myself below with the boundary checks, put in a function
        let footer_crc = CRC32.compute_crc_data_block(&footer[footer.len() - 41..]);
        let min_max_crc = CRC32.compute_crc_data_block(&footer[..footer.len() - 41]);
        let sparse_crc = CRC32.compute_crc_data_block(&sst_finalizer.sparse_index.index_entries);

        sst_finalizer
            .writer
            .write_all(&sst_finalizer.sparse_index.index_entries)?;

        let mut bloom_digest = CRC32.digest();
        for word in &bloom_filter.bits {
            bloom_digest.update(&word.to_le_bytes());
            sst_finalizer.writer.write_all(&word.to_le_bytes())?;
        }
        let bloom_crc = bloom_digest.finalize();
        sst_finalizer.writer.write_all(&footer)?;
        sst_finalizer.writer.write_all(&sparse_crc.to_le_bytes())?;
        sst_finalizer.writer.write_all(&bloom_crc.to_le_bytes())?;
        sst_finalizer.writer.write_all(&min_max_crc.to_le_bytes())?;
        sst_finalizer.writer.write_all(&footer_crc.to_le_bytes())?;

        sst_finalizer.writer.flush()?;
        sst_finalizer.writer.get_mut().sync_all()?;

        Ok(())
    }

    fn merge_to_final(
        compaction_outcome: &mut CompactionOutcome,
        mut heap: BinaryHeap<Reverse<MergeItem>>,
        cfe_vec: &mut [CompactionFileElement],
        data_dir: PathBuf,
        hlc: Arc<Hlc>,
    ) -> Result<()> {
        // grab min key before we start
        let Some(first) = heap.peek() else {
            return Ok(());
        }; // nothing to comoact
        let min_k = first.0.entry.key.clone();

        // TODO: THE sst_finalizer below is assigned a new HLC, use the HIGHEST HLC from the input files instead.
        let mut sst_finalizer = SstFinalizer::new(
            &data_dir,
            min_k,
            &hlc,
            compaction_outcome.level_for_output_sst,
        )?;
        compaction_outcome
            .final_sst_files
            .push(sst_finalizer.sst_paths.clone());
        let mut last_k_written: Option<Vec<u8>> = None;

        while let Some(curr_merge_item) = heap.pop().as_mut() {
            if last_k_written.as_ref() != Some(&curr_merge_item.0.entry.key) {
                let record = curr_merge_item.0.serialize_record()?;
                if sst_finalizer.would_exceed_max_sst_size(record.len() as u64) {
                    Self::finalize_output_merged_file(sst_finalizer)?;

                    sst_finalizer = SstFinalizer::new(
                        &data_dir,
                        curr_merge_item.0.entry.key.clone(),
                        &hlc,
                        compaction_outcome.level_for_output_sst,
                    )?; // after 160MB, one sst is done // 
                    compaction_outcome
                        .final_sst_files
                        .push(sst_finalizer.sst_paths.clone());
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
            ); // TODO: dont expect anyways, just throw err
            // if item is the same as last one, we are skipping it because the newer key has already been written to final file
            MergeItem::new(cfe, curr_merge_item.0.source).map(|x| {
                match x {
                    Some(m) => heap.push(Reverse(m)),
                    None => {
                        compaction_outcome
                            .consumed_sst_files
                            .push(cfe.sst_slice.file_path.clone());
                        // DATA BLOCK EXHAUSTED -> fully_consumed vec
                    } // if this returns none, file has been fully read and we push it to the fully_consumed_file vector
                }
            })?; // err
        }

        Self::finalize_output_merged_file(sst_finalizer)?;
        // put paths into vec
        // compaction_outcome.final_sst_files.push(sst_paths);

        Ok(())
    }
}

impl Ord for MergeItem {
    fn cmp(&self, other: &Self) -> Ordering {
        let (tstamp_other, counter_other) = Hlc::deserialize_hlc(other.entry.timestamp);
        let (tstamp_self, counter_self) = Hlc::deserialize_hlc(self.entry.timestamp);
        self.entry
            .key
            .cmp(&other.entry.key) // smaller wins
            .then_with(|| {
                // "timestamp" is now an hlc, the most significant 52 bits are the timestamp
                // the other 12 are the counter so keep in mind

                tstamp_other.cmp(&tstamp_self)
                // other.entry.timestamp.cmp(&self.entry.timestamp)
            }) // larger wins(newer)
            .then_with(|| counter_other.cmp(&counter_self))
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
