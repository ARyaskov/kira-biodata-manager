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

    #[error("invalid SRR accession: {0}")]
    InvalidSrrId(String),

    #[error("invalid UniProt accession: {0}")]
    InvalidUniprotId(String),

    #[error("invalid DOI: {0}")]
    InvalidDoi(String),

    #[error("invalid GEO series accession: {0}")]
    InvalidExpressionAccession(String),

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

    #[error("Crossref request failed: {0}")]
    CrossrefHttp(String),

    #[error("Crossref returned status {status}: {message}")]
    CrossrefStatus { status: u16, message: String },

    #[error("GEO request failed: {0}")]
    GeoHttp(String),

    #[error("GEO returned status {status}: {message}")]
    GeoStatus { status: u16, message: String },

    #[error("{0}")]
    GeoResolution(String),

    #[error("knowledge base request failed: {0}")]
    KnowledgeHttp(String),

    #[error("knowledge base returned status {status}: {message}")]
    KnowledgeStatus { status: u16, message: String },

    #[error("dataset not found locally: {0}")]
    DatasetNotFound(String),

    #[error("failed to parse JSON config: {0}")]
    ConfigParse(String),

    #[error("filesystem error: {0}")]
    Filesystem(String),

    #[error("invalid include value: {0}")]
    InvalidInclude(String),

    #[error("invalid format for dataset type: {0}")]
    InvalidFormat(String),

    #[error("required tool not found: {0}")]
    MissingTool(String),

    #[error("sra conversion failed: {0}")]
    SrrConversion(String),

    #[error("uniprot request failed: {0}")]
    UniprotHttp(String),

    #[error("uniprot returned status {status}: {message}")]
    UniprotStatus { status: u16, message: String },

    #[error("{0}")]
    DoiResolution(String),

    #[error("protein format not supported by NCBI MMDB: {0}")]
    UnsupportedProteinFormat(String),
}
