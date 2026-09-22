use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};

use std::path::Path;
use std::{collections::BTreeSet, path::PathBuf};
use std::{format, unimplemented};

use crate::errors::{CorruptionType, CrcType, DataCorruptedErr, DbError};
use crate::helpers::{CRC32, check_crc, read_range};
use crate::{errors::Result, lsm::SST_LEVEL_COUNT};
pub const MAX_MANIFEST_SIZE: u64 = 4 * 1024 * 1024;
pub const TAG_ADD_FILE: u8 = 1;
pub const TAG_DELETE_FILE: u8 = 2;
pub const TAG_MIN_LIVE_WAL: u8 = 3;
pub const MANIFEST_FILE_NAME: &str = "MANIFEST";
pub const MANIFEST_TMP_FILE_NAME: &str = "MANIFEST.tmp";
/*


NEW_FILE | DELETED_FILE format: | level(1 byte) | file_id(8 bytes)


LENGTH(8 bytes) | TAG_ADD_FILE(1 byte) | LVL(1) | FILE_ID(8 bytes) | ... | crc(4)(does not cover the length)

record format: length | entry* | crc
entries:
wal format: tag | id
add file format: tag | level | id
delete file format: tag | level | id
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

impl Default for ManifestState {
    fn default() -> Self {
        Self {
            levels: [const { BTreeSet::new() }; SST_LEVEL_COUNT],
            min_live_wal: 0,
        }
    }
}

impl ManifestState {
    fn new(min_live_wal: u64) -> Self {
        Self {
            levels: [const { BTreeSet::new() }; SST_LEVEL_COUNT],
            min_live_wal,
        }
    }

    pub fn replay(bytes: &[u8], path: &Path) -> Result<ManifestState> {
        let mut starting_state = ManifestState::default();
        let mut offset: usize = 0;

        while offset < bytes.len() {
            let byte_to_deserialize = &bytes[offset..];
            if byte_to_deserialize.len() < 8 {
                break; // 
            }

            let length = u64::from_le_bytes(byte_to_deserialize[..8].try_into().unwrap()) as usize;
            if length > MAX_MANIFEST_SIZE as usize {
                return Err(DbError::ManifestError(format!(
                    "record length {length} is larger than the MAX_MANIFEST_SIZE"
                )));
            }
            if 12 + length > byte_to_deserialize.len() {
                break;
            }

            let (edit, consumed_bytes) = Manifest::deserialize_record(byte_to_deserialize, &path)?;
            let validated_edit = starting_state.check_edit_is_compatible_with_state(&edit)?;
            starting_state.edit_state(&validated_edit);
            offset += consumed_bytes;
        }

        Ok(starting_state)
    }
    fn edit_state(&mut self, edit: &ValidatedEdit) -> () {
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
    // if None, open a completely new Manifest
    pub fn open(dir: &Path) -> Result<Option<Manifest>> {
        // checks whether we already have a manifest in the directory, if yes, replay it into memory
        // if no start return None -> start new
        let path = dir.join(MANIFEST_FILE_NAME);
        let bytes = match fs::read(dir.join(MANIFEST_FILE_NAME)) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let state = ManifestState::replay(&bytes, &dir.join(MANIFEST_FILE_NAME))?;
        let (size, file) = Manifest::write_snapshot(dir, &state)?;

        Ok(Some(Manifest {
            path,
            state,
            dir: dir.to_path_buf(),
            file,
            size,
            read_only: false,
        }))
    }

    fn write_snapshot(dir: &Path, state: &ManifestState) -> Result<(u64, File)> {
        // writes all the add_file tags to ManifestState to a tmp file, then the min_wal_id
        // then atomically rename and sync
        //

        let tmp = dir.join(MANIFEST_TMP_FILE_NAME);
        let mut f = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&tmp)?;

        // finish
        let snapshot = ManifestEdit {
            new_files: state
                .levels
                .iter()
                .enumerate()
                .flat_map(|(lvl, ssts)| ssts.iter().map(move |sst| ((lvl as u8), *sst)))
                .collect(),

            deleted_files: Vec::new(),
            min_live_wal: Some(state.min_live_wal),
        };

        let validated = ManifestState::default().check_edit_is_compatible_with_state(&snapshot)?; // needs empty state so we can add without a confict
        let full_record = Manifest::serialize_record(&validated);
        f.write_all(&full_record)?;
        f.sync_all()?;
        fs::rename(&tmp, dir.join("MANIFEST"))?;
        File::open(dir)?.sync_all()?;

        Ok((full_record.len() as u64, f))
    }

    pub fn new_manifest(dir: &Path) -> Result<Manifest> {
        // returns empty Manifest(first time db opened)
        let path = dir.join(MANIFEST_FILE_NAME);

        let f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        File::open(dir)?.sync_all()?;
        Ok(Manifest {
            path,
            state: ManifestState::default(),
            dir: dir.to_path_buf(),
            file: f,
            size: 0,
            read_only: false,
        })
    }
    pub fn remove_obsolete_files(&self) {
        // cleans up the directory from unaccounted for files
        // call during KVEngine::open
    }

    // fn replay_state(bytes: &[u8]) -> ManifestState {}
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

        self.state.edit_state(&validated);
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
    pub fn deserialize_record(bytes: &[u8], path: &Path) -> Result<(ManifestEdit, usize)> {
        // so we read a record: length | payload | crc and return an edit so we can build state

        let length = u64::from_le_bytes(read_range(bytes, 0, 8)?.try_into().unwrap());
        if length > MAX_MANIFEST_SIZE {
            return Err(DbError::ManifestError(format!(
                "Record found in Manifest exceeds max manifest size. Length found: {length}"
            )));
        }

        let mut new_files: Vec<(u8, u64)> = Vec::new();
        let mut deleted_files: Vec<(u8, u64)> = Vec::new();
        let mut wal_id: Option<u64> = None;
        let payload = read_range(bytes, 8, 8 + length as usize)?;
        let crc_to_check = CRC32.compute_crc_data_block(payload);
        let crc_in_file = u32::from_le_bytes(
            read_range(
                bytes,
                (8 + length as usize) as usize,
                (8 + length as usize) + 4,
            )?
            .try_into()
            .unwrap(),
        );

        check_crc(crc_to_check, crc_in_file, 8, &path, CrcType::ManifestRecord)?;
        let mut pos: usize = 0;

        while pos < payload.len() {
            let tag = u8::from_le_bytes(read_range(payload, pos, (pos + 1))?.try_into().unwrap());
            pos += 1;
            match tag {
                TAG_ADD_FILE => {
                    let lvl =
                        u8::from_le_bytes(read_range(payload, pos, (pos + 1))?.try_into().unwrap());
                    pos += 1;
                    let sst_id = u64::from_le_bytes(
                        read_range(payload, pos, (pos + 8))?.try_into().unwrap(),
                    );
                    pos += 8;
                    new_files.push((lvl, sst_id));
                }
                TAG_DELETE_FILE => {
                    let lvl =
                        u8::from_le_bytes(read_range(payload, pos, (pos + 1))?.try_into().unwrap());
                    pos += 1;
                    let sst_id = u64::from_le_bytes(
                        read_range(payload, pos, (pos + 8))?.try_into().unwrap(),
                    );
                    pos += 8;
                    deleted_files.push((lvl, sst_id));
                }
                TAG_MIN_LIVE_WAL => {
                    let w_id = u64::from_le_bytes(
                        read_range(payload, pos, (pos + 8))?.try_into().unwrap(),
                    );
                    pos += 8;
                    wal_id = Some(w_id);
                }
                found => {
                    return Err(DbError::DataCorrupted(DataCorruptedErr {
                        offset: 8 + pos as u64 - 1,
                        file_path: path.to_path_buf(),
                        reason: CorruptionType::RecordTypeCorrupted { found },
                    }));
                }
            }
        }

        Ok((
            ManifestEdit {
                new_files,
                deleted_files,
                min_live_wal: wal_id,
            },
            pos + 4 + 8, // for the crc and length
        ))
    } // reads bytes of a record and returns a ManifestEdit, so on reboot, we call on each record to build ManifestState
}
