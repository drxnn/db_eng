use core::fmt;
use std::{
    error::Error,
    fmt::{Formatter, write},
    path::PathBuf,
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
}

#[derive(Debug)]
pub enum DbError {
    DataCorrupted(DataCorruptedErr),
    Io(std::io::Error),
}

impl fmt::Display for CorruptionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::CrcMismatch { expected, found } => {
                write!(f, "expected crc: {}. Found crc: {}", expected, found)
            }
            Self::Other(str) => {
                write!(f, "{}", str)
            }
            Self::LengthMismatch { expected, found } => {
                write!(f, "expected length: {}. Found length: {}", expected, found)
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
        }
    }
}

impl From<std::io::Error> for DbError {
    fn from(value: std::io::Error) -> Self {
        DbError::Io(value)
    }
}

impl Error for DbError {}
impl Error for DataCorruptedErr {}
impl Error for CorruptionType {}
pub type Result<T> = std::result::Result<T, DbError>;
