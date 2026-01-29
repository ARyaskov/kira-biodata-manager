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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetSpecifier {
    Protein(ProteinId),
    Genome(GenomeAccession),
}

impl DatasetSpecifier {
    pub fn dataset_type(&self) -> &'static str {
        match self {
            DatasetSpecifier::Protein(_) => "protein",
            DatasetSpecifier::Genome(_) => "genome",
        }
    }

    pub fn resolve_registry(&self, format: Option<ProteinFormat>) -> Registry {
        match self {
            DatasetSpecifier::Protein(_) => match format.unwrap_or(ProteinFormat::Cif) {
                ProteinFormat::Cif | ProteinFormat::Pdb | ProteinFormat::Bcif => Registry::Rcsb,
            },
            DatasetSpecifier::Genome(_) => Registry::Ncbi,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Registry {
    Rcsb,
    Ncbi,
}

impl FromStr for DatasetSpecifier {
    type Err = KiraError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let trimmed = value.trim();
        let (kind, rest) = trimmed
            .split_once(':')
            .ok_or_else(|| KiraError::InvalidSpecifier(value.to_string()))?;
        match kind {
            "protein" => Ok(DatasetSpecifier::Protein(rest.parse()?)),
            "genome" => Ok(DatasetSpecifier::Genome(rest.parse()?)),
            _ => Err(KiraError::InvalidSpecifier(value.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;

    #[test]
    fn parse_protein_id_valid() {
        let id: ProteinId = "1lyz".parse().unwrap();
        assert_eq!(id.as_str(), "1LYZ");
    }

    #[test]
    fn parse_protein_id_invalid() {
        let err = "XYZ".parse::<ProteinId>().unwrap_err();
        assert_matches!(err, KiraError::InvalidProteinId(_));
    }

    #[test]
    fn parse_genome_accession_valid() {
        let acc: GenomeAccession = "GCF_000005845.2".parse().unwrap();
        assert_eq!(acc.as_str(), "GCF_000005845.2");
    }

    #[test]
    fn parse_genome_accession_invalid() {
        let err = "ABC_123".parse::<GenomeAccession>().unwrap_err();
        assert_matches!(err, KiraError::InvalidGenomeAccession(_));
    }

    #[test]
    fn parse_dataset_specifier() {
        let spec: DatasetSpecifier = "protein:1LYZ".parse().unwrap();
        assert_matches!(spec, DatasetSpecifier::Protein(_));
    }

    #[test]
    fn registry_routing() {
        let protein: DatasetSpecifier = "protein:1LYZ".parse().unwrap();
        assert_eq!(protein.resolve_registry(None), Registry::Rcsb);
        assert_eq!(
            protein.resolve_registry(Some(ProteinFormat::Bcif)),
            Registry::Rcsb
        );

        let genome: DatasetSpecifier = "genome:GCF_000005845.2".parse().unwrap();
        assert_eq!(genome.resolve_registry(None), Registry::Ncbi);
    }
}
