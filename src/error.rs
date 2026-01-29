use std::path::PathBuf;

use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum KiraError {
    #[error("invalid dataset specifier: {0}")]
    InvalidSpecifier(String),

    #[error("invalid protein id: {0}")]
    InvalidProteinId(String),

    #[error("invalid genome accession: {0}")]
    InvalidGenomeAccession(String),

    #[error("missing config file kira-bm.json in current directory")]
    MissingConfig,

    #[error("failed to read config file at {0}")]
    ConfigRead(PathBuf),

    #[error("NCBI request failed: {0}")]
    NcbiHttp(String),

    #[error("NCBI returned status {status}: {message}")]
    NcbiStatus { status: u16, message: String },

    #[error("RCSB request failed: {0}")]
    RcsbHttp(String),

    #[error("RCSB returned status {status}: {message}")]
    RcsbStatus { status: u16, message: String },

    #[error("dataset not found locally: {0}")]
    DatasetNotFound(String),

    #[error("failed to parse JSON config: {0}")]
    ConfigParse(String),

    #[error("filesystem error: {0}")]
    Filesystem(String),

    #[error("invalid include value: {0}")]
    InvalidInclude(String),

    #[error("protein format not supported by NCBI MMDB: {0}")]
    UnsupportedProteinFormat(String),
}
