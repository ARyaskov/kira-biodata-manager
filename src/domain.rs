use std::fmt;
use std::str::FromStr;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::error::KiraError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ProteinFormat {
    Cif,
    Pdb,
    Bcif,
}

impl fmt::Display for ProteinFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProteinFormat::Cif => write!(f, "cif"),
            ProteinFormat::Pdb => write!(f, "pdb"),
            ProteinFormat::Bcif => write!(f, "bcif"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum SrrFormat {
    Fastq,
    Fasta,
}

impl fmt::Display for SrrFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SrrFormat::Fastq => write!(f, "fastq"),
            SrrFormat::Fasta => write!(f, "fasta"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum FetchFormat {
    Cif,
    Pdb,
    Bcif,
    Fastq,
    Fasta,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProteinId(String);

impl ProteinId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProteinId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ProteinId {
    type Err = KiraError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_uppercase();
        let is_valid =
            normalized.len() == 4 && normalized.chars().all(|ch| ch.is_ascii_alphanumeric());
        if !is_valid {
            return Err(KiraError::InvalidProteinId(value.to_string()));
        }
        Ok(Self(normalized))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GenomeAccession(String);

impl GenomeAccession {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SrrId(String);

impl SrrId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UniprotId(String);

impl UniprotId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Doi(String);

impl Doi {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Doi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for UniprotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for UniprotId {
    type Err = KiraError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_uppercase();
        let is_valid = normalized.len() >= 6
            && normalized.len() <= 10
            && normalized.chars().all(|ch| ch.is_ascii_alphanumeric());
        if !is_valid {
            return Err(KiraError::InvalidUniprotId(value.to_string()));
        }
        Ok(Self(normalized))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GeoSeriesAccession(String);

impl GeoSeriesAccession {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GeoSeriesAccession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for SrrId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for SrrId {
    type Err = KiraError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_uppercase();
        let is_valid = (normalized.starts_with("SRR") || normalized.starts_with("ERR"))
            && normalized.len() > 3
            && normalized[3..].chars().all(|ch| ch.is_ascii_digit());
        if !is_valid {
            return Err(KiraError::InvalidSrrId(value.to_string()));
        }
        Ok(Self(normalized))
    }
}

impl fmt::Display for GenomeAccession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for GenomeAccession {
    type Err = KiraError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_string();
        let is_valid = normalized.starts_with("GCF_") || normalized.starts_with("GCA_");
        let parts = normalized.split('.').collect::<Vec<_>>();
        let has_numeric = parts
            .first()
            .map(|prefix| prefix.trim_start_matches("GCF_").trim_start_matches("GCA_"))
            .map(|rest| rest.chars().all(|ch| ch.is_ascii_digit()) && !rest.is_empty())
            .unwrap_or(false);
        if !is_valid || !has_numeric {
            return Err(KiraError::InvalidGenomeAccession(value.to_string()));
        }
        Ok(Self(normalized))
    }
}

impl FromStr for Doi {
    type Err = KiraError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let trimmed = value.trim();
        let normalized = trimmed.to_lowercase();
        let is_valid = normalized.starts_with("10.") && normalized.contains('/');
        if !is_valid {
            return Err(KiraError::InvalidDoi(value.to_string()));
        }
        Ok(Self(normalized))
    }
}

impl FromStr for GeoSeriesAccession {
    type Err = KiraError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let trimmed = value.trim();
        let normalized = trimmed.to_uppercase();
        let is_valid = normalized.starts_with("GSE")
            && normalized.chars().skip(3).all(|ch| ch.is_ascii_digit());
        if !is_valid {
            return Err(KiraError::InvalidExpressionAccession(value.to_string()));
        }
        Ok(Self(normalized))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetSpecifier {
    Protein(ProteinId),
    Genome(GenomeAccession),
    Srr(SrrId),
    Uniprot(UniprotId),
    Doi(Doi),
    Expression(GeoSeriesAccession),
    Expression10x(GeoSeriesAccession),
    Go,
    Kegg,
    Reactome,
}

impl DatasetSpecifier {
    pub fn dataset_type(&self) -> &'static str {
        match self {
            DatasetSpecifier::Protein(_) => "protein",
            DatasetSpecifier::Genome(_) => "genome",
            DatasetSpecifier::Srr(_) => "srr",
            DatasetSpecifier::Uniprot(_) => "uniprot",
            DatasetSpecifier::Doi(_) => "doi",
            DatasetSpecifier::Expression(_) => "expression",
            DatasetSpecifier::Expression10x(_) => "expression10x",
            DatasetSpecifier::Go => "go",
            DatasetSpecifier::Kegg => "kegg",
            DatasetSpecifier::Reactome => "reactome",
        }
    }

    pub fn resolve_registry(&self, format: Option<ProteinFormat>) -> Registry {
        match self {
            DatasetSpecifier::Protein(_) => match format.unwrap_or(ProteinFormat::Cif) {
                ProteinFormat::Cif | ProteinFormat::Pdb | ProteinFormat::Bcif => Registry::Rcsb,
            },
            DatasetSpecifier::Genome(_) => Registry::Ncbi,
            DatasetSpecifier::Srr(_) => Registry::Ncbi,
            DatasetSpecifier::Uniprot(_) => Registry::Uniprot,
            DatasetSpecifier::Doi(_) => Registry::Doi,
            DatasetSpecifier::Expression(_) => Registry::Geo,
            DatasetSpecifier::Expression10x(_) => Registry::Geo,
            DatasetSpecifier::Go => Registry::Go,
            DatasetSpecifier::Kegg => Registry::Kegg,
            DatasetSpecifier::Reactome => Registry::Reactome,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Registry {
    Rcsb,
    Ncbi,
    Uniprot,
    Doi,
    Geo,
    Go,
    Kegg,
    Reactome,
}

impl FromStr for DatasetSpecifier {
    type Err = KiraError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let trimmed = value.trim();
        if let Some((kind, rest)) = trimmed.split_once(':') {
            return match kind {
                "protein" => Ok(DatasetSpecifier::Protein(rest.parse()?)),
                "genome" => Ok(DatasetSpecifier::Genome(rest.parse()?)),
                "srr" => Ok(DatasetSpecifier::Srr(rest.parse()?)),
                "uniprot" => Ok(DatasetSpecifier::Uniprot(rest.parse()?)),
                "doi" => Ok(DatasetSpecifier::Doi(rest.parse()?)),
                "expression" => Ok(DatasetSpecifier::Expression(rest.parse()?)),
                "expression10x" => Ok(DatasetSpecifier::Expression10x(rest.parse()?)),
                _ => Err(KiraError::InvalidSpecifier(value.to_string())),
            };
        }
        match trimmed {
            "go" => Ok(DatasetSpecifier::Go),
            "kegg" => Ok(DatasetSpecifier::Kegg),
            "reactome" => Ok(DatasetSpecifier::Reactome),
            _ => Err(KiraError::InvalidSpecifier(value.to_string())),
        }
    }
}
