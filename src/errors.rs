use core::fmt;
use std::{
    error::Error,
    fmt::{Formatter, write},
    io,
    num::ParseIntError,
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
pub enum CrcType {
    // can add more as needed
    MinMaxFooterKeys,
    BloomFilter,
    SparseIndex,
    DataBlock,
    WalRecord,
    SstFooterMetadata,
}

#[derive(Debug)]

pub enum CorruptionType {
    CrcMismatch {
        expected: u32,
        found: u32,
        mismatch_type: CrcType,
    },
    Other(String),
    LengthMismatch {
        expected: usize,
        found: usize,
    },
    BufferExceedsMaxLength {
        size: u64,
        max_size: u64,
    },
    MetadataSizeOverflow {
        sizes: [u64; 4],
    },
    MetaDataSizeExceedsFileSize {
        file_size: u64,
        metadata_size: u64,
    },
    KeyValueRecordExceedsMaxLength {
        max: u64,
        found: u64,
    },
    RecordTypeCorrupted {
        found: u8,
    },
    TombstoneCorrupted {
        found: u8,
    }, // add value that was expected too, either 0xFF or 0x00
    TruncatedRecord, // TODO: Not a corruption necessarily
}

#[derive(Debug)]

// TODO: DbError is the umbrella for th errors, later on group errors together, for example CompactionErr, FlushingErrs etc as well as have
// generic errors for everythign
pub enum DbError {
    DataCorrupted(DataCorruptedErr),
    MissingKey(String),
    MissingHeapEntry(String, PathBuf),
    Io(std::io::Error),
    PathFailedToParseToInt(PathBuf, ParseIntError),
    FileError(String, PathBuf),
    MemTableSyncError(String),
    ReportedViaChannel,
    SyncFail(Box<DbError>, PathBuf),
    WalFailed,
    CompactionError(CompactionErr),
    DataBlockExhausted,
    TooManyFilesOpenInProcess,
    TooManyFilesOpenInSystem,
    MalformedDataBlock(String),
    WalNotFound,
    NonNumericFileIdOnSstable(PathBuf),
    InvalidSstableFileName(PathBuf),
    InvalidMemtableInput(InvalidMemtableInput),
    OutOfBoundsRead { start: u64, end: u64, len: u64 }, // TODO: extend this to be more elaborate
}
#[derive(Debug)]
pub enum InvalidMemtableInput {
    KeySizeTooLarge { max: u64, found: u64 },
    ValueSizeTooLarge { max: u64, found: u64 },
}

impl From<InvalidMemtableInput> for DbError {
    fn from(e: InvalidMemtableInput) -> Self {
        DbError::InvalidMemtableInput(e)
    }
}

impl From<io::Error> for DbError {
    fn from(e: io::Error) -> Self {
        #[cfg(unix)]
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
    CompactionJobAlreadyInFlight,
}

pub enum FlushingError {
    SyncError(DbError),
}
impl fmt::Display for CorruptionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::CrcMismatch {
                expected,
                found,
                mismatch_type,
            } => {
                write!(
                    f,
                    "CrcTypeFails: {:?}. Expected crc: {}. Found crc: {}",
                    mismatch_type, expected, found
                )
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
            Self::RecordTypeCorrupted { found } => {
                write!(f, "Corrupted RecordType byte. Instead found: {}", found)
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
                CompactionErr::CompactionJobAlreadyInFlight => {
                    write!(
                        f,
                        "Couldn't start CompactionJob. There is one already running"
                    )
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
            Self::NonNumericFileIdOnSstable(p) => {
                write!(
                    f,
                    "Expected SStable to have a valid file id(numeric file stem). File path: {}",
                    p.display()
                )
            }
            Self::InvalidSstableFileName(p) => {
                write!(
                    f,
                    "Expected SStable to have a valid file name. Found {} instead",
                    p.display()
                )
            }
            Self::InvalidMemtableInput(e) => match e {
                InvalidMemtableInput::ValueSizeTooLarge { max, found } => {
                    write!(
                        f,
                        "Value size exceeds maximum value size allowed. Max size: {}. Found size: {}",
                        max, found
                    )
                }
                InvalidMemtableInput::KeySizeTooLarge { max, found } => {
                    write!(
                        f,
                        "Key size exceeds maximum value size allowed. Max size: {}. Found size: {}",
                        max, found
                    )
                }
            },
            Self::OutOfBoundsRead { start, end, len } => {
                write!(
                    f,
                    "tried to read {start}..{end}, but the buffer is {len} bytes"
                )
            }
            Self::WalNotFound => {
                write!(f, "Invalid State: WAL not found in running engine")
            }
            Self::PathFailedToParseToInt(p, e) => {
                write!(
                    f,
                    "Failed to parse the numeric part of file name to an integer. Path: {}. Error: {}",
                    p.display(),
                    e
                )
            }
            Self::MalformedDataBlock(s) => {
                write!(f, "MalformedDataBlock. ErrMsg:{}", s)
            }
            Self::WalFailed => {
                write!(f, "Wal Failure.")
            }
        }
    }
}

impl Error for DbError {}
impl Error for DataCorruptedErr {}
impl Error for CorruptionType {}
pub type Result<T> = std::result::Result<T, DbError>;
