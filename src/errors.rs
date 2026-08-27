use core::fmt;
use std::{
    error::Error,
    fmt::{Formatter, write},
    io,
    path::PathBuf,
    write,
};

#[derive(Debug)]

pub struct DataCorruptedErr {
    pub offset: u64,
    pub file_path: PathBuf,
    pub reason: CorruptionType,
}

#[derive(Debug)]
pub enum CorruptionType {
    CrcMismatch { expected: u32, found: u32 },
    Other(String),
    LengthMismatch { expected: usize, found: usize },
    BufferExceedsMaxLength { size: u64, max_size: u64 },
    MetadataSizeOverflow { sizes: [u64; 4] },
    MetaDataSizeExceedsFileSize { file_size: u64, metadata_size: u64 },
    KeyValueRecordExceedsMaxLength { max: u64, found: u64 },
    TombstoneCorrupted { found: u8 }, // add value that was expected too, either 0xFF or 0x00
    TruncatedRecord,
}

#[derive(Debug)]

// TODO: DbError is the umbrella for th errors, later on group errors together, for example CompactionErr, FlushingErrs etc as well as have
// generic errors for everythign
pub enum DbError {
    DataCorrupted(DataCorruptedErr),
    MissingKey(String),
    MissingHeapEntry(String, PathBuf),
    Io(std::io::Error),
    FileError(String, PathBuf),
    MemTableSyncError(String),
    ReportedViaChannel,
    SyncFail(Box<DbError>, PathBuf),
    CompactionError(CompactionErr),
    DataBlockExhausted,
    TooManyFilesOpenInProcess,
    TooManyFilesOpenInSystem,
}

impl From<io::Error> for DbError {
    fn from(e: io::Error) -> Self {
        #[cfg(target_os = "macos")]
        match e.raw_os_error() {
            Some(24) => return DbError::TooManyFilesOpenInProcess,
            Some(23) => return DbError::TooManyFilesOpenInSystem,
            _ => {}
        }
        #[cfg(target_os = "linux")]
        match e.raw_os_error() {
            Some(24) => return DbError::TooManyFilesOpenInProcess,
            Some(23) => return DbError::TooManyFilesOpenInSystem,
            _ => {}
        }
        #[cfg(windows)]
        match e.raw_os_error() {
            Some(4) => return DbError::TooManyFilesOpenInProcess,
            _ => {}
        }
        DbError::Io(e)
    }
}
#[derive(Debug)]
pub enum CompactionErr {
    HeapNotFound,
    EmptyCompactionFileElementCollection,
}

pub enum FlushingError {
    SyncError(DbError),
}
impl fmt::Display for CorruptionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::CrcMismatch { expected, found } => {
                write!(f, "Expected crc: {}. Found crc: {}", expected, found)
            }
            Self::Other(str) => {
                write!(f, "{}", str)
            }
            Self::LengthMismatch { expected, found } => {
                write!(f, "Expected length: {}. Found length: {}", expected, found)
            }
            Self::BufferExceedsMaxLength { size, max_size } => {
                write!(f, "Buffer Size: {}. Max size allowed: {}", size, max_size)
            }
            Self::MetadataSizeOverflow { sizes } => {
                write!(
                    f,
                    "Corruption in metadata footer. Sizes overflow. Sparse_index_size:{}. Bloom_filter_size:{}. Min_key_size:{}. Max_key_size:{}",
                    sizes[0], sizes[1], sizes[2], sizes[3]
                )
            }
            Self::MetaDataSizeExceedsFileSize {
                file_size,
                metadata_size,
            } => {
                write!(
                    f,
                    "Metadata size exceeds file size: File_size: {}. Metadata_size:{}",
                    file_size, metadata_size
                )
            }
            Self::KeyValueRecordExceedsMaxLength { max, found } => {
                write!(
                    f,
                    "Key/Value Exceeds Max Length. Max Length: {}. Found Length: {}",
                    max, found
                )
            }
            Self::TombstoneCorrupted { found } => {
                write!(f, "Corrupted Tombstone. Instead found: {}", found)
            }
            Self::TruncatedRecord => {
                write!(f, "Trucated Record Found")
            }
        }
    }
}

impl fmt::Display for DataCorruptedErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} (occurred at {}, in file: {})",
            self.reason,
            self.offset,
            self.file_path.display()
        )
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::DataCorrupted(err) => write!(f, "data corrupted: {}", err),
            Self::Io(err) => write!(f, "I/O error: {}", err),
            Self::MissingKey(err) => write!(f, "Missing key in memtable: {}", err),
            Self::FileError(err, path) => {
                write!(f, "Error in file:{}. Err: {}", path.display(), err)
            }
            Self::MemTableSyncError(err) => {
                write!(f, "Error while syncing memtable: {}", err)
            }
            Self::ReportedViaChannel => {
                write!(f, "Error reported to main thread via channel. ")
            }
            Self::SyncFail(err, path) => {
                write!(
                    f,
                    "Error while syncing file {}. Err: {}",
                    path.display(),
                    err
                )
            }
            Self::MissingHeapEntry(s, p) => {
                write!(f, "HeapEntryMissingError at {}. ErrMsg: {}", p.display(), s)
            }
            Self::CompactionError(c) => match c {
                CompactionErr::EmptyCompactionFileElementCollection => {
                    write!(f, "Empty CFE collection during compaction. ")
                }
                CompactionErr::HeapNotFound => {
                    write!(f, "Empty Heap during compaction. ")
                }
            },
            Self::DataBlockExhausted => {
                write!(f, "Datablock exhausted. Unfinished. ")
            }
            Self::TooManyFilesOpenInProcess => {
                write!(f, "errno: 24. Too many open files in the proccess.")
            }
            Self::TooManyFilesOpenInSystem => {
                write!(f, "errno: 23. Too many open files in the system.")
            }
        }
    }
}

impl Error for DbError {}
impl Error for DataCorruptedErr {}
impl Error for CorruptionType {}
pub type Result<T> = std::result::Result<T, DbError>;
