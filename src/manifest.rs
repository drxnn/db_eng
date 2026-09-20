use std::format;
use std::fs::File;
use std::io::Write;
use std::{collections::BTreeSet, path::PathBuf};

use crate::errors::DbError;
use crate::helpers::CRC32;
use crate::{errors::Result, lsm::SST_LEVEL_COUNT};
pub const MAX_MANIFEST_SIZE: u64 = 4 * 1024 * 1024;
pub const TAG_ADD_FILE: u8 = 1;
pub const TAG_DELETE_FILE: u8 = 2;
pub const TAG_MIN_LIVE_WAL: u8 = 3;

/*
The Manifest is another append only file that helps us determine the engines state
// if the engine crashes, we can use the manifest to go back to the correct valid state
something that can go wrong:
we havde 10 files to compact, we compact them down to 3 new output files
// we need to delete the input but we delete the files one by one, if the engine crashes mid way, we will have duplicate data, or we could
// have leftover inputs that would shadow newer data
the manifest would fix this because we would append the entire compaction record as one log to the Manifest
// then we would delete the files, if we have a crash mid way, doesnt matter, we check the manifest to determine what files are live

entry example format: | NEW_FILE(the tag) | level | file_id
TypeOfEntry: NEW_FILE | DELETED_FILE | MIN_WAL_ID(min wal id basically says "wal files below this id are completely flushed, so if we find any we can safely just delete them, the other wals get retrieved and flushed")


NEW_FILE | DELETED_FILE format: | level(1 byte) | file_id(8 bytes)

RECORD format: length(8 bytes) | payload(length bytes) | crc(4) |
paylod: entry*
as the manifest files get bigger, we need to compact it as well, the way we do that is we read the current manifest(the in memory one) and we
write a new manifest with only the live data
for example, lets say our manifest added a few files, then compaction deleted those files, this manifest has records that are just taking space
so we make a new one and we put all the current active files in there, our min_wal_id and then its smaller insize because the compaction records that are
now irrelevant are gone.
so a manifest file really looks like this: | snapshot | edit_records |
the snapshot will just be a bunch of NEW_FILE records essentially, no need to distinguish, it still is just a bunch of records(NEW_FILE records)

when you write a change, for example [Deleted L0 #4,Deleted L0 #6,Deleted L0 #1,NEW L1 #12](all one record)
you write the record to the manifest file first(sync it)
then you apply this edit to the in memory ManifestState and if the check passes, the caller updates sstables
methods:
append_record()
edit_state()
edit_and_append(ManifestEdit) // the function we call, calls the other 2 functions above


Records:
LENGTH(8 bytes) | TAG_ADD_FILE(1 byte) | FILE_ID(8 bytes) | crc(4)


*/

pub struct ManifestEdit {
    pub new_files: Vec<(u8, u64)>,     // lvl, file_id
    pub deleted_files: Vec<(u8, u64)>, // lvl, file_id
    pub min_live_wal: Option<u64>,     // if none dont change
}
pub struct ManifestState {
    pub levels: [BTreeSet<u64>; SST_LEVEL_COUNT], // id of sst files that are live for each level, when theres an edit make sure to mutate
    // when we reach manifest_file_size_threshold for example and we make a new manifest file
    // we iterate through levels to create the snapshot for the new manifest
    pub min_live_wal: u64,
}

pub struct ValidatedEdit<'a>(&'a ManifestEdit);

impl ManifestState {
    fn edit_state(&mut self, edit: &ValidatedEdit) -> Result<()> {
        // should only be called when check_edit_is_compatible_with_state passes

        for (lvl, sst_id) in &edit.0.new_files {
            self.levels[*lvl as usize].insert(*sst_id);
        }
        for (lvl, sst_id) in &edit.0.deleted_files {
            self.levels[*lvl as usize].remove(sst_id);
        }
        if let Some(n) = edit.0.min_live_wal {
            self.min_live_wal = n;
        }
        Ok(())
    }
    fn check_edit_is_compatible_with_state<'a>(
        &self,
        edit: &'a ManifestEdit,
    ) -> Result<ValidatedEdit<'a>> {
        // if Ok() we can go ahead with the edit
        // would return an error if we are trying to delete a file thats not there in our ManifestState
        // or adding a file thats already there
        // if fails, we cannot edit manifest, and if manifest edit_and_append fails, compaction or flushing should also be rejected

        for (level, sst_id) in &edit.new_files {
            if *level as usize >= SST_LEVEL_COUNT {
                return Err(DbError::ManifestError(format!(
                    "Attempted to add file in level thats out of bounds. Level found: {level}"
                )));
            }

            let is_file_live_anywhere = self.levels.iter().any(|lvl| lvl.contains(sst_id));

            if is_file_live_anywhere {
                return Err(DbError::ManifestError(
                    "Attempted to edit manifest by adding file, but a file with that name was already there"
                        .to_string(),
                ));
            }
        }

        for (level, sst_id) in &edit.deleted_files {
            if *level as usize >= SST_LEVEL_COUNT {
                return Err(DbError::ManifestError(format!(
                    "Attempted to delete file in level thats out of bounds. Level found: {level}"
                )));
            }
            if !self.levels[*level as usize].contains(sst_id) {
                return Err(DbError::ManifestError(
                    "Attempted to edit manifest by deleting a file, but file was not there"
                        .to_string(),
                ));
            }
        }

        if let Some(curr) = edit.min_live_wal.as_ref()
            && *curr < self.min_live_wal
        {
            return Err(DbError::ManifestError(format!(
                "min_live_wal {curr} is behind the current {}",
                self.min_live_wal
            )));
        }
        Ok(ValidatedEdit(edit))
    }
}
pub struct Manifest {
    path: PathBuf,
    state: ManifestState,
    dir: PathBuf,
    file: File,
    size: u64,
    read_only: bool, // if we have a failure like disk full or hardware
}

impl Manifest {
    pub fn recover() {
        // recover on reopen, make sure to delete files that are not accounted for in the Maifest
    }
    pub fn edit_and_append(&mut self, edit: &ManifestEdit) -> Result<()> {
        if self.read_only {
            return Err(DbError::ManifestError(
                "manifest is read-only after a failed write".into(),
            ));
        }
        let validated = self.state.check_edit_is_compatible_with_state(edit)?;

        let record = Self::serialize_record(&validated);
        // write to self.file and sync it
        if let Err(e) = self
            .file
            .write_all(&record)
            .and_then(|_| self.file.sync_all())
        {
            self.read_only = true;
            return Err(e.into());
        }

        self.state.edit_state(&validated)?;
        Ok(())
    }
    pub fn serialize_record(validated_edit: &ValidatedEdit) -> Vec<u8> {
        let mut payload: Vec<u8> = Vec::new();
        // | length | record* | crc
        for (lvl, sst_id) in &validated_edit.0.new_files {
            payload.extend_from_slice(&TAG_ADD_FILE.to_le_bytes());
            payload.extend_from_slice(&lvl.to_le_bytes());
            payload.extend_from_slice(&sst_id.to_le_bytes());
        }
        for (lvl, sst_id) in &validated_edit.0.deleted_files {
            payload.extend_from_slice(&TAG_DELETE_FILE.to_le_bytes());
            payload.extend_from_slice(&lvl.to_le_bytes());
            payload.extend_from_slice(&sst_id.to_le_bytes());
        }

        if let Some(wal) = validated_edit.0.min_live_wal {
            payload.extend_from_slice(&TAG_MIN_LIVE_WAL.to_le_bytes());
            payload.extend_from_slice(&wal.to_le_bytes());
        }

        // get crc
        let crc32 = CRC32.compute_crc_data_block(&payload);
        let mut record: Vec<u8> = Vec::with_capacity(payload.len() + 12); // 4 for crc, 8 for length
        record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        record.extend_from_slice(&payload);
        record.extend_from_slice(&crc32.to_le_bytes());

        record
    } // takes a record and writes in in bytes
    pub fn deserialize_record() {} // reads bytes of a record and returns a ManifestEdit, so on reboot, we call on each record to build ManifestState
}
