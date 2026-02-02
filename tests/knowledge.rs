use std::path::Path;

use kira_biodata_manager::app::{App, FetchOptions, FetchOverrides, ProgressSink};
use kira_biodata_manager::domain::DatasetSpecifier;
use kira_biodata_manager::error::KiraError;
use kira_biodata_manager::geo::GeoClient;
use kira_biodata_manager::knowledge::KnowledgeClient;
use kira_biodata_manager::ncbi::NcbiClient;
use kira_biodata_manager::rcsb::RcsbClient;
use kira_biodata_manager::srr::SrrClient;
use kira_biodata_manager::store::Store;
use kira_biodata_manager::uniprot::UniprotClient;

struct NoopSink;

impl ProgressSink for NoopSink {
    fn event(&self, _event: kira_biodata_manager::app::ProgressEvent) {}
}

#[derive(Clone, Copy)]
struct DummyNcbi;
#[derive(Clone, Copy)]
struct DummyRcsb;
#[derive(Clone, Copy)]
struct DummySrr;
#[derive(Clone, Copy)]
struct DummyUniprot;
#[derive(Clone, Copy)]
struct DummyGeo;
#[derive(Clone)]
struct DummyKnowledge;

impl NcbiClient for DummyNcbi {
    fn download_protein(
        &self,
        _id: &kira_biodata_manager::domain::ProteinId,
        _format: kira_biodata_manager::domain::ProteinFormat,
        _destination: &Path,
    ) -> Result<kira_biodata_manager::ncbi::DownloadInfo, KiraError> {
        Err(KiraError::NcbiHttp("not used".to_string()))
    }

    fn download_genome(
        &self,
        _accession: &kira_biodata_manager::domain::GenomeAccession,
        _include: &[String],
        _destination: &Path,
    ) -> Result<kira_biodata_manager::ncbi::DownloadInfo, KiraError> {
        Err(KiraError::NcbiHttp("not used".to_string()))
    }
}

impl RcsbClient for DummyRcsb {
    fn download_structure(
        &self,
        _id: &kira_biodata_manager::domain::ProteinId,
        _format: kira_biodata_manager::domain::ProteinFormat,
        _destination: &Path,
    ) -> Result<(), KiraError> {
        Err(KiraError::RcsbHttp("not used".to_string()))
    }

    fn fetch_metadata(
        &self,
        _id: &kira_biodata_manager::domain::ProteinId,
    ) -> Result<kira_biodata_manager::rcsb::RcsbMetadata, KiraError> {
        Err(KiraError::RcsbHttp("not used".to_string()))
    }
}

impl SrrClient for DummySrr {
    fn download_fastq(
        &self,
        _id: &kira_biodata_manager::domain::SrrId,
        _paired: bool,
        _destination_dir: &Path,
    ) -> Result<Vec<std::path::PathBuf>, KiraError> {
        Err(KiraError::SrrConversion("not used".to_string()))
    }

    fn tool_info(&self) -> kira_biodata_manager::srr::ToolInfo {
        kira_biodata_manager::srr::ToolInfo {
            datasets: None,
            sra_toolkit: None,
        }
    }
}

impl UniprotClient for DummyUniprot {
    fn fetch(
        &self,
        _id: &kira_biodata_manager::domain::UniprotId,
    ) -> Result<kira_biodata_manager::uniprot::UniprotRecord, KiraError> {
        Err(KiraError::UniprotHttp("not used".to_string()))
    }
}

impl GeoClient for DummyGeo {
    fn fetch_soft_text(
        &self,
        _accession: &kira_biodata_manager::domain::GeoSeriesAccession,
    ) -> Result<String, KiraError> {
        Err(KiraError::GeoHttp("not used".to_string()))
    }

    fn download_url(&self, _url: &str, _destination: &Path) -> Result<(), KiraError> {
        Err(KiraError::GeoHttp("not used".to_string()))
    }
}

impl KnowledgeClient for DummyKnowledge {
    fn download_go(&self, destination: &Path) -> Result<Vec<u8>, KiraError> {
        let payload = b"format-version: 1.2\ndata-version: 2025-01-01\n";
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        }
        std::fs::write(destination, payload)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        Ok(payload.to_vec())
    }

    fn download_kegg_pathways(&self, _destination: &Path) -> Result<(), KiraError> {
        Err(KiraError::KnowledgeHttp("not used".to_string()))
    }

    fn download_kegg_pathway_links(&self, _destination: &Path) -> Result<(), KiraError> {
        Err(KiraError::KnowledgeHttp("not used".to_string()))
    }

    fn download_reactome_pathways(&self, _destination: &Path) -> Result<(), KiraError> {
        Err(KiraError::KnowledgeHttp("not used".to_string()))
    }

    fn download_reactome_mappings(&self, _destination: &Path) -> Result<(), KiraError> {
        Err(KiraError::KnowledgeHttp("not used".to_string()))
    }
}

#[test]
fn go_fetch_reuses_cache() {
    let temp = tempfile::tempdir().unwrap();
    let project = camino::Utf8PathBuf::from_path_buf(temp.path().join("project")).unwrap();
    let cache = camino::Utf8PathBuf::from_path_buf(temp.path().join("cache")).unwrap();
    let store = Store::new_with_paths(project, cache);

    let app = App::new(
        store,
        DummyNcbi,
        DummyRcsb,
        DummySrr,
        DummyUniprot,
        DummyGeo,
        DummyKnowledge,
    );
    let options = FetchOptions {
        force: false,
        no_cache: false,
        dry_run: false,
    };
    let result = app
        .fetch(
            Some(DatasetSpecifier::Go),
            None,
            FetchOverrides::default(),
            options.clone(),
            &NoopSink,
        )
        .unwrap();
    assert_eq!(result.items[0].action, "download");

    let result = app
        .fetch(
            Some(DatasetSpecifier::Go),
            None,
            FetchOverrides::default(),
            options,
            &NoopSink,
        )
        .unwrap();
    assert_eq!(result.items[0].action, "cache");
}
