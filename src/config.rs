use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::domain::{Doi, GenomeAccession, ProteinFormat, ProteinId, SrrFormat, SrrId, UniprotId};
use crate::error::KiraError;

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub schema_version: Option<u32>,
    #[serde(default)]
    pub proteins: Vec<ProteinEntry>,
    #[serde(default)]
    pub genomes: Vec<GenomeEntry>,
    #[serde(default)]
    pub srr: Vec<SrrEntry>,
    #[serde(default)]
    pub uniprot: Vec<UniprotEntry>,
    #[serde(default)]
    pub doi: Vec<DoiEntry>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ProteinEntry {
    Shorthand(String),
    Detailed(ProteinEntryObject),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProteinEntryObject {
    pub id: String,
    #[serde(default)]
    pub format: Option<ProteinFormat>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum GenomeEntry {
    Shorthand(String),
    Detailed(GenomeEntryObject),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GenomeEntryObject {
    pub accession: String,
    #[serde(default)]
    pub include: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum UniprotEntry {
    Shorthand(String),
    Detailed(UniprotEntryObject),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct UniprotEntryObject {
    pub id: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum DoiEntry {
    Shorthand(String),
    Detailed(DoiEntryObject),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DoiEntryObject {
    pub id: String,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum SrrEntry {
    Shorthand(String),
    Detailed(SrrEntryObject),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SrrEntryObject {
    pub id: String,
    #[serde(default)]
    pub format: Option<SrrFormat>,
    #[serde(default)]
    pub paired: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct ProteinRequest {
    pub id: ProteinId,
    pub format: ProteinFormat,
}

#[derive(Debug, Clone)]
pub struct GenomeRequest {
    pub accession: GenomeAccession,
    pub include: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub schema_version: u32,
    pub proteins: Vec<ProteinRequest>,
    pub genomes: Vec<GenomeRequest>,
    pub srr: Vec<SrrRequest>,
    pub uniprot: Vec<UniprotRequest>,
    pub doi: Vec<DoiRequest>,
}

#[derive(Debug, Clone)]
pub struct SrrRequest {
    pub id: SrrId,
    pub format: SrrFormat,
    pub paired: bool,
}

#[derive(Debug, Clone)]
pub struct UniprotRequest {
    pub id: UniprotId,
}

#[derive(Debug, Clone)]
pub struct DoiRequest {
    pub id: Doi,
}

pub struct ConfigLoader;

impl ConfigLoader {
    pub fn resolve(path: Option<&str>) -> Result<ResolvedConfig, KiraError> {
        let config_path = match path {
            Some(path) => PathBuf::from(path),
            None => PathBuf::from("kira-bm.json"),
        };

        if path.is_none() && !config_path.exists() {
            return Err(KiraError::MissingConfig);
        }

        let content = fs::read_to_string(&config_path)
            .map_err(|_| KiraError::ConfigRead(config_path.clone()))?;
        let config: Config = serde_json::from_str(&content)
            .map_err(|err| KiraError::ConfigParse(err.to_string()))?;

        Self::resolve_config(config)
    }

    pub fn resolve_config(config: Config) -> Result<ResolvedConfig, KiraError> {
        let schema_version = config.schema_version.unwrap_or(1);

        let proteins = config
            .proteins
            .into_iter()
            .map(|entry| match entry {
                ProteinEntry::Shorthand(value) => Ok(ProteinRequest {
                    id: value.parse()?,
                    format: ProteinFormat::Cif,
                }),
                ProteinEntry::Detailed(obj) => Ok(ProteinRequest {
                    id: obj.id.parse()?,
                    format: obj.format.unwrap_or(ProteinFormat::Cif),
                }),
            })
            .collect::<Result<Vec<_>, KiraError>>()?;

        let genomes = config
            .genomes
            .into_iter()
            .map(|entry| match entry {
                GenomeEntry::Shorthand(value) => Ok(GenomeRequest {
                    accession: value.parse()?,
                    include: default_genome_include(),
                }),
                GenomeEntry::Detailed(obj) => Ok(GenomeRequest {
                    accession: obj.accession.parse()?,
                    include: obj.include.unwrap_or_else(default_genome_include),
                }),
            })
            .collect::<Result<Vec<_>, KiraError>>()?;

        let srr = config
            .srr
            .into_iter()
            .map(|entry| match entry {
                SrrEntry::Shorthand(value) => Ok(SrrRequest {
                    id: value.parse()?,
                    format: SrrFormat::Fastq,
                    paired: false,
                }),
                SrrEntry::Detailed(obj) => Ok(SrrRequest {
                    id: obj.id.parse()?,
                    format: obj.format.unwrap_or(SrrFormat::Fastq),
                    paired: obj.paired.unwrap_or(false),
                }),
            })
            .collect::<Result<Vec<_>, KiraError>>()?;

        let uniprot = config
            .uniprot
            .into_iter()
            .map(|entry| match entry {
                UniprotEntry::Shorthand(value) => Ok(UniprotRequest { id: value.parse()? }),
                UniprotEntry::Detailed(obj) => Ok(UniprotRequest {
                    id: obj.id.parse()?,
                }),
            })
            .collect::<Result<Vec<_>, KiraError>>()?;

        let doi = config
            .doi
            .into_iter()
            .map(|entry| match entry {
                DoiEntry::Shorthand(value) => Ok(DoiRequest { id: value.parse()? }),
                DoiEntry::Detailed(obj) => Ok(DoiRequest {
                    id: obj.id.parse()?,
                }),
            })
            .collect::<Result<Vec<_>, KiraError>>()?;

        Ok(ResolvedConfig {
            schema_version,
            proteins,
            genomes,
            srr,
            uniprot,
            doi,
        })
    }
}

pub fn default_genome_include() -> Vec<String> {
    vec![
        "genome".to_string(),
        "gff3".to_string(),
        "protein".to_string(),
        "seq-report".to_string(),
    ]
}
