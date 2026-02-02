use std::path::Path;
use std::sync::Mutex;

use camino::Utf8PathBuf;

use kira_biodata_manager::app::{App, FetchOptions, FetchOverrides};
use kira_biodata_manager::domain::{
    DatasetSpecifier, GenomeAccession, ProteinFormat, ProteinId, SrrId, UniprotId,
};
use kira_biodata_manager::error::KiraError;
use kira_biodata_manager::ncbi::{DownloadInfo, NcbiClient};
use kira_biodata_manager::output::JsonOutput;
use kira_biodata_manager::rcsb::{RcsbClient, RcsbMetadata};
use kira_biodata_manager::srr::{SrrClient, ToolInfo};
use kira_biodata_manager::store::Store;
use kira_biodata_manager::uniprot::{UniprotClient, UniprotRecord};

#[derive(Default)]
struct MockNcbi;

impl NcbiClient for MockNcbi {
    fn download_protein(
        &self,
        _id: &ProteinId,
        _format: ProteinFormat,
        _destination: &Path,
    ) -> Result<DownloadInfo, KiraError> {
        Err(KiraError::NcbiHttp("not implemented".to_string()))
    }

    fn download_genome(
        &self,
        _accession: &GenomeAccession,
        _include: &[String],
        _destination: &Path,
    ) -> Result<DownloadInfo, KiraError> {
        Ok(DownloadInfo { is_zip: true })
    }
}

#[derive(Default)]
struct MockRcsb {
    calls: Mutex<usize>,
}

impl RcsbClient for MockRcsb {
    fn download_structure(
        &self,
        _id: &ProteinId,
        _format: ProteinFormat,
        _destination: &Path,
    ) -> Result<(), KiraError> {
        let mut guard = self.calls.lock().unwrap();
        *guard += 1;
        Ok(())
    }

    fn fetch_metadata(&self, _id: &ProteinId) -> Result<RcsbMetadata, KiraError> {
        Err(KiraError::RcsbHttp("not implemented".to_string()))
    }
}

#[derive(Default)]
struct MockSrr;

impl SrrClient for MockSrr {
    fn download_fastq(
        &self,
        _id: &SrrId,
        _paired: bool,
        _destination_dir: &Path,
    ) -> Result<Vec<std::path::PathBuf>, KiraError> {
        Err(KiraError::MissingTool("mock".to_string()))
    }

    fn tool_info(&self) -> ToolInfo {
        ToolInfo {
            datasets: None,
            sra_toolkit: None,
        }
    }
}

#[derive(Default)]
struct MockUniprot;

impl UniprotClient for MockUniprot {
    fn fetch(&self, _id: &UniprotId) -> Result<UniprotRecord, KiraError> {
        Err(KiraError::UniprotHttp("not implemented".to_string()))
    }
}

#[test]
fn fetch_prefers_cache_over_download() {
    let temp = tempfile::tempdir().unwrap();
    let project_root = Utf8PathBuf::from_path_buf(temp.path().join("project")).unwrap();
    let cache_root = Utf8PathBuf::from_path_buf(temp.path().join("cache")).unwrap();
    let store = Store::new_with_paths(project_root, cache_root);
    store.ensure_project_root().unwrap();
    store.ensure_cache_root().unwrap();

    let id: ProteinId = "1LYZ".parse().unwrap();
    let cache_path = store.cache_protein_path(&id, ProteinFormat::Cif);
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent.as_std_path()).unwrap();
    }
    std::fs::write(cache_path.as_std_path(), b"data").unwrap();

    let app = App::new(
        store,
        MockNcbi::default(),
        MockRcsb::default(),
        MockSrr::default(),
        MockUniprot::default(),
    );
    let options = FetchOptions {
        force: false,
        no_cache: false,
        dry_run: false,
    };

    let result = app
        .fetch(
            Some(DatasetSpecifier::Protein(id)),
            None,
            FetchOverrides::default(),
            options,
            &JsonOutput,
        )
        .unwrap();

    assert_eq!(result.items[0].action, "cache");
}
