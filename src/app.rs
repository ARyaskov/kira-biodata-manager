use std::fs;
use std::time::Duration;

use camino::Utf8PathBuf;
use serde::Serialize;

use crate::config::ResolvedConfig;
use crate::domain::{DatasetSpecifier, GenomeAccession, ProteinFormat, ProteinId, Registry};
use crate::error::KiraError;
use crate::ncbi::NcbiClient;
use crate::rcsb::{RcsbClient, RcsbMetadata};
use crate::store::{Metadata, Store, atomic_rename_dir};

#[derive(Debug, Clone)]
pub struct FetchOptions {
    pub force: bool,
    pub no_cache: bool,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct FetchResult {
    pub items: Vec<FetchItemResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FetchItemResult {
    pub dataset_type: String,
    pub id: String,
    pub format: Option<String>,
    pub source: String,
    pub action: String,
    pub project_path: Option<String>,
    pub cache_path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListResult {
    pub datasets: Vec<ListEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListEntry {
    pub dataset_type: String,
    pub id: String,
    pub format: Option<String>,
    pub source: Option<String>,
    pub project_path: Option<String>,
    pub cache_path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InfoResult {
    pub dataset_type: String,
    pub id: String,
    pub format: Option<String>,
    pub source: Option<String>,
    pub project_path: Option<String>,
    pub cache_path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClearResult {
    pub cleared: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum ProgressSinkKind {
    Fetch,
    List,
    Info,
    Clear,
}

#[derive(Debug, Clone)]
pub struct ProgressEvent {
    pub message: String,
    pub elapsed: Option<Duration>,
}

pub trait ProgressSink {
    fn event(&self, event: ProgressEvent);
}

#[derive(Clone)]
pub struct App<N: NcbiClient, R: RcsbClient> {
    store: Store,
    ncbi: N,
    rcsb: R,
}

impl<N: NcbiClient, R: RcsbClient> App<N, R> {
    pub fn new(store: Store, ncbi: N, rcsb: R) -> Self {
        Self { store, ncbi, rcsb }
    }

    pub fn fetch(
        &self,
        specifier: Option<DatasetSpecifier>,
        config: Option<&ResolvedConfig>,
        format_override: Option<ProteinFormat>,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchResult, KiraError> {
        let mut items = Vec::new();

        if let Some(spec) = specifier {
            items.push(self.fetch_single(spec, format_override, options.clone(), sink)?);
        } else if let Some(config) = config {
            for protein in &config.proteins {
                let spec = DatasetSpecifier::Protein(protein.id.clone());
                let format = format_override.unwrap_or(protein.format);
                items.push(self.fetch_single(spec, Some(format), options.clone(), sink)?);
            }
            for genome in &config.genomes {
                items.push(self.fetch_genome_with_include(
                    genome.accession.clone(),
                    genome.include.clone(),
                    options.clone(),
                    sink,
                )?);
            }
        }

        Ok(FetchResult { items })
    }

    pub fn list(&self, sink: &dyn ProgressSink) -> Result<ListResult, KiraError> {
        sink.event(ProgressEvent {
            message: "phase=Resolve; scanning stores".to_string(),
            elapsed: None,
        });

        let project_metadata = Store::list_metadata(self.store.project_root())?;
        let cache_metadata = Store::list_metadata(self.store.cache_root())?;

        let mut map = std::collections::HashMap::<(String, String), ListEntry>::new();
        for entry in project_metadata {
            let key = (entry.dataset_type.clone(), entry.id.clone());
            let value = map.entry(key).or_insert_with(|| ListEntry {
                dataset_type: entry.dataset_type.clone(),
                id: entry.id.clone(),
                format: entry.format.clone(),
                source: Some(entry.source.clone()),
                project_path: None,
                cache_path: None,
            });
            value.project_path = Some(entry.resolved_path.clone());
        }

        for entry in cache_metadata {
            let key = (entry.dataset_type.clone(), entry.id.clone());
            let value = map.entry(key).or_insert_with(|| ListEntry {
                dataset_type: entry.dataset_type.clone(),
                id: entry.id.clone(),
                format: entry.format.clone(),
                source: Some(entry.source.clone()),
                project_path: None,
                cache_path: None,
            });
            value.cache_path = Some(entry.resolved_path.clone());
        }

        Ok(ListResult {
            datasets: map.into_values().collect(),
        })
    }

    pub fn info(
        &self,
        specifier: DatasetSpecifier,
        sink: &dyn ProgressSink,
    ) -> Result<InfoResult, KiraError> {
        let key = match &specifier {
            DatasetSpecifier::Protein(id) => ("protein".to_string(), id.as_str().to_string()),
            DatasetSpecifier::Genome(acc) => ("genome".to_string(), acc.as_str().to_string()),
        };

        sink.event(ProgressEvent {
            message: format!("phase=Resolve; looking up {}", key.1),
            elapsed: None,
        });

        let project = Store::list_metadata(self.store.project_root())?;
        let cache = Store::list_metadata(self.store.cache_root())?;
        let project_meta = project
            .into_iter()
            .find(|meta| meta.dataset_type == key.0 && meta.id == key.1);
        let cache_meta = cache
            .into_iter()
            .find(|meta| meta.dataset_type == key.0 && meta.id == key.1);

        if project_meta.is_none() && cache_meta.is_none() {
            return Err(KiraError::DatasetNotFound(format!("{}:{}", key.0, key.1)));
        }

        Ok(InfoResult {
            dataset_type: key.0,
            id: key.1,
            format: project_meta
                .as_ref()
                .and_then(|meta| meta.format.clone())
                .or_else(|| cache_meta.as_ref().and_then(|meta| meta.format.clone())),
            source: project_meta
                .as_ref()
                .map(|meta| meta.source.clone())
                .or_else(|| cache_meta.as_ref().map(|meta| meta.source.clone())),
            project_path: project_meta.map(|meta| meta.resolved_path),
            cache_path: cache_meta.map(|meta| meta.resolved_path),
        })
    }

    pub fn clear(&self, sink: &dyn ProgressSink) -> Result<ClearResult, KiraError> {
        sink.event(ProgressEvent {
            message: "phase=Store; clearing project store".to_string(),
            elapsed: None,
        });
        self.store.clear_project()?;
        Ok(ClearResult { cleared: true })
    }

    fn fetch_single(
        &self,
        specifier: DatasetSpecifier,
        format_override: Option<ProteinFormat>,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchItemResult, KiraError> {
        if !options.dry_run {
            self.store.ensure_project_root()?;
            self.store.ensure_cache_root()?;
        }

        let registry = specifier.resolve_registry(format_override);
        match (specifier, registry) {
            (DatasetSpecifier::Protein(id), Registry::Rcsb) => {
                self.fetch_protein(id, format_override, options, sink)
            }
            (DatasetSpecifier::Protein(id), Registry::Ncbi) => {
                self.fetch_protein(id, format_override, options, sink)
            }
            (DatasetSpecifier::Genome(acc), Registry::Ncbi) => self.fetch_genome_with_include(
                acc,
                crate::config::default_genome_include(),
                options,
                sink,
            ),
            (DatasetSpecifier::Genome(acc), Registry::Rcsb) => self.fetch_genome_with_include(
                acc,
                crate::config::default_genome_include(),
                options,
                sink,
            ),
        }
    }

    fn fetch_protein(
        &self,
        id: ProteinId,
        format_override: Option<ProteinFormat>,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchItemResult, KiraError> {
        sink.event(ProgressEvent {
            message: format!("phase=Resolve; protein {}", id.as_str()),
            elapsed: None,
        });
        let format = format_override.unwrap_or(ProteinFormat::Cif);
        if !options.dry_run {
            self.store.ensure_project_root()?;
            self.store.ensure_cache_root()?;
        }

        let project_path = self.store.project_protein_path(&id, format);
        let cache_path = self.store.cache_protein_path(&id, format);
        let project_dir = self.store.project_protein_dir(&id);
        let cache_dir = self.store.cache_protein_dir(&id);

        if !options.force && self.store.project_exists(&project_path) {
            sink.event(ProgressEvent {
                message: "phase=Store; already in project store".to_string(),
                elapsed: None,
            });
            return Ok(FetchItemResult {
                dataset_type: "protein".to_string(),
                id: id.as_str().to_string(),
                format: Some(format.to_string()),
                source: "rcsb".to_string(),
                action: "project".to_string(),
                project_path: Some(project_path.to_string()),
                cache_path: cache_path
                    .as_std_path()
                    .exists()
                    .then(|| cache_path.to_string()),
            });
        }

        if !options.force && self.store.cache_exists(&cache_path) {
            sink.event(ProgressEvent {
                message: "phase=Store; using cached dataset".to_string(),
                elapsed: None,
            });
            if !options.dry_run {
                Store::copy_file_atomic(&cache_path, &project_path)?;
                let (cache_meta, cache_raw) = rcsb_metadata_paths(&cache_dir);
                let (project_meta, project_raw) = rcsb_metadata_paths(&project_dir);
                if cache_meta.as_std_path().exists() {
                    Store::copy_file_atomic(&cache_meta, &project_meta)?;
                }
                if cache_raw.as_std_path().exists() {
                    Store::copy_file_atomic(&cache_raw, &project_raw)?;
                }
                let meta = self.build_metadata(
                    "rcsb",
                    "protein",
                    id.as_str(),
                    Some(format.to_string()),
                    project_path.as_str(),
                );
                Store::write_metadata(
                    &self.store.project_metadata_path("protein", id.as_str()),
                    &meta,
                )?;
            }
            return Ok(FetchItemResult {
                dataset_type: "protein".to_string(),
                id: id.as_str().to_string(),
                format: Some(format.to_string()),
                source: "rcsb".to_string(),
                action: "cache".to_string(),
                project_path: Some(project_path.to_string()),
                cache_path: Some(cache_path.to_string()),
            });
        }

        if options.dry_run {
            return Ok(FetchItemResult {
                dataset_type: "protein".to_string(),
                id: id.as_str().to_string(),
                format: Some(format.to_string()),
                source: "rcsb".to_string(),
                action: "download".to_string(),
                project_path: Some(project_path.to_string()),
                cache_path: (!options.no_cache).then(|| cache_path.to_string()),
            });
        }

        let temp_dir = tempfile::Builder::new()
            .prefix("kira-bm-protein")
            .tempdir_in(self.store.project_root().as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let temp_path = temp_dir.path().join(format!("{}.tmp", id.as_str()));
        let temp_meta = temp_dir.path().join("metadata.json");
        let temp_raw = temp_dir.path().join("metadata.raw.json");

        sink.event(ProgressEvent {
            message: "phase=Prepare; preparing download".to_string(),
            elapsed: None,
        });
        sink.event(ProgressEvent {
            message: "rcsb.request".to_string(),
            elapsed: None,
        });
        let start = std::time::Instant::now();
        self.rcsb.download_structure(&id, format, &temp_path)?;
        let mut rcsb_meta = self.rcsb.fetch_metadata(&id)?;
        rcsb_meta.source_structure_url = crate::rcsb::RcsbHttpClient::structure_url(&id, format);
        let latency = start.elapsed().as_millis();
        sink.event(ProgressEvent {
            message: format!("rcsb.response latency_ms={latency}"),
            elapsed: None,
        });

        sink.event(ProgressEvent {
            message: "phase=Verify; validating package".to_string(),
            elapsed: None,
        });
        let meta_payload = RcsbMetadataFile::from(&rcsb_meta);
        let meta_bytes = serde_json::to_vec_pretty(&meta_payload)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let raw_bytes = serde_json::to_vec_pretty(&rcsb_meta.raw_json)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        std::fs::write(&temp_meta, &meta_bytes)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        std::fs::write(&temp_raw, &raw_bytes)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        sink.event(ProgressEvent {
            message: "phase=Store; writing files".to_string(),
            elapsed: None,
        });
        let data_file = Utf8PathBuf::from_path_buf(temp_path)
            .map_err(|_| KiraError::Filesystem("non-utf8 file path in dataset".to_string()))?;
        Store::copy_file_atomic(&data_file, &project_path)?;
        let (project_meta, project_raw) = rcsb_metadata_paths(&project_dir);
        let temp_meta = Utf8PathBuf::from_path_buf(temp_meta)
            .map_err(|_| KiraError::Filesystem("non-utf8 file path in dataset".to_string()))?;
        let temp_raw = Utf8PathBuf::from_path_buf(temp_raw)
            .map_err(|_| KiraError::Filesystem("non-utf8 file path in dataset".to_string()))?;
        Store::copy_file_atomic(&temp_meta, &project_meta)?;
        Store::copy_file_atomic(&temp_raw, &project_raw)?;
        let meta = self.build_metadata(
            "rcsb",
            "protein",
            id.as_str(),
            Some(format.to_string()),
            project_path.as_str(),
        );
        Store::write_metadata(
            &self.store.project_metadata_path("protein", id.as_str()),
            &meta,
        )?;

        if !options.no_cache {
            Store::copy_file_atomic(&project_path, &cache_path)?;
            let (cache_meta, cache_raw) = rcsb_metadata_paths(&cache_dir);
            Store::copy_file_atomic(&project_meta, &cache_meta)?;
            Store::copy_file_atomic(&project_raw, &cache_raw)?;
            let meta = self.build_metadata(
                "rcsb",
                "protein",
                id.as_str(),
                Some(format.to_string()),
                cache_path.as_str(),
            );
            Store::write_metadata(
                &self.store.cache_metadata_path("protein", id.as_str()),
                &meta,
            )?;
        }

        Ok(FetchItemResult {
            dataset_type: "protein".to_string(),
            id: id.as_str().to_string(),
            format: Some(format.to_string()),
            source: "rcsb".to_string(),
            action: "download".to_string(),
            project_path: Some(project_path.to_string()),
            cache_path: (!options.no_cache).then(|| cache_path.to_string()),
        })
    }

    fn fetch_genome_with_include(
        &self,
        accession: GenomeAccession,
        include: Vec<String>,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchItemResult, KiraError> {
        sink.event(ProgressEvent {
            message: format!("phase=Resolve; genome {}", accession.as_str()),
            elapsed: None,
        });
        if !options.dry_run {
            self.store.ensure_project_root()?;
            self.store.ensure_cache_root()?;
        }

        let project_dir = self.store.project_genome_dir(&accession);
        let cache_dir = self.store.cache_genome_dir(&accession);

        if !options.force && self.store.project_exists(&project_dir) {
            sink.event(ProgressEvent {
                message: "phase=Store; already in project store".to_string(),
                elapsed: None,
            });
            return Ok(FetchItemResult {
                dataset_type: "genome".to_string(),
                id: accession.as_str().to_string(),
                format: None,
                source: "ncbi".to_string(),
                action: "project".to_string(),
                project_path: Some(project_dir.to_string()),
                cache_path: cache_dir
                    .as_std_path()
                    .exists()
                    .then(|| cache_dir.to_string()),
            });
        }

        if !options.force && self.store.cache_exists(&cache_dir) {
            sink.event(ProgressEvent {
                message: "phase=Store; using cached dataset".to_string(),
                elapsed: None,
            });
            if !options.dry_run {
                Store::copy_dir_atomic(&cache_dir, &project_dir)?;
                let meta = self.build_metadata(
                    "ncbi",
                    "genome",
                    accession.as_str(),
                    None,
                    project_dir.as_str(),
                );
                Store::write_metadata(
                    &self
                        .store
                        .project_metadata_path("genome", accession.as_str()),
                    &meta,
                )?;
            }
            return Ok(FetchItemResult {
                dataset_type: "genome".to_string(),
                id: accession.as_str().to_string(),
                format: None,
                source: "ncbi".to_string(),
                action: "cache".to_string(),
                project_path: Some(project_dir.to_string()),
                cache_path: Some(cache_dir.to_string()),
            });
        }

        if options.dry_run {
            return Ok(FetchItemResult {
                dataset_type: "genome".to_string(),
                id: accession.as_str().to_string(),
                format: None,
                source: "ncbi".to_string(),
                action: "download".to_string(),
                project_path: Some(project_dir.to_string()),
                cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
            });
        }

        let temp_dir = tempfile::Builder::new()
            .prefix("kira-bm-genome")
            .tempdir_in(self.store.project_root().as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let zip_path = temp_dir.path().join("dataset.zip");

        sink.event(ProgressEvent {
            message: "phase=Prepare; preparing download".to_string(),
            elapsed: None,
        });
        sink.event(ProgressEvent {
            message: "ncbi.request".to_string(),
            elapsed: None,
        });
        let start = std::time::Instant::now();
        let download = self.ncbi.download_genome(&accession, &include, &zip_path)?;
        let latency = start.elapsed().as_millis();
        sink.event(ProgressEvent {
            message: format!("ncbi.response latency_ms={latency}"),
            elapsed: None,
        });
        if !zip_path.exists() {
            return Err(KiraError::Filesystem(format!(
                "genome download missing file: {}",
                zip_path.display()
            )));
        }

        if !download.is_zip {
            return Err(KiraError::Filesystem(
                "expected genome download to be a zip archive".to_string(),
            ));
        }
        sink.event(ProgressEvent {
            message: "phase=Verify; validating package".to_string(),
            elapsed: None,
        });
        crate::fs_util::validate_zip(&zip_path)?;
        let extract_dir = temp_dir.path().join("extract");
        fs::create_dir_all(&extract_dir).map_err(|err| KiraError::Filesystem(err.to_string()))?;
        crate::fs_util::extract_zip(&zip_path, &extract_dir)?;

        if project_dir.as_std_path().exists() {
            fs::remove_dir_all(project_dir.as_std_path())
                .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        }
        if let Some(parent) = project_dir.parent() {
            fs::create_dir_all(parent.as_std_path())
                .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        }
        sink.event(ProgressEvent {
            message: "phase=Store; writing files".to_string(),
            elapsed: None,
        });
        atomic_rename_dir(&extract_dir, project_dir.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let meta = self.build_metadata(
            "ncbi",
            "genome",
            accession.as_str(),
            None,
            project_dir.as_str(),
        );
        Store::write_metadata(
            &self
                .store
                .project_metadata_path("genome", accession.as_str()),
            &meta,
        )?;

        if !options.no_cache {
            Store::copy_dir_atomic(&project_dir, &cache_dir)?;
            let meta = self.build_metadata(
                "ncbi",
                "genome",
                accession.as_str(),
                None,
                cache_dir.as_str(),
            );
            Store::write_metadata(
                &self.store.cache_metadata_path("genome", accession.as_str()),
                &meta,
            )?;
        }

        Ok(FetchItemResult {
            dataset_type: "genome".to_string(),
            id: accession.as_str().to_string(),
            format: None,
            source: "ncbi".to_string(),
            action: "download".to_string(),
            project_path: Some(project_dir.to_string()),
            cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
        })
    }

    fn build_metadata(
        &self,
        source: &str,
        dataset_type: &str,
        id: &str,
        format: Option<String>,
        path: &str,
    ) -> Metadata {
        Metadata {
            source: source.to_string(),
            dataset_type: dataset_type.to_string(),
            id: id.to_string(),
            format,
            downloaded_at: iso_timestamp(),
            tool: format!("kira-bm/{}", env!("CARGO_PKG_VERSION")),
            resolved_path: path.to_string(),
        }
    }
}

fn iso_timestamp() -> String {
    chrono::Utc::now().to_rfc3339()
}

#[derive(Debug, Serialize)]
struct RcsbMetadataFile {
    registry: String,
    pdb_id: String,
    title: Option<String>,
    experimental_method: Option<String>,
    resolution: Option<f64>,
    deposition_date: Option<String>,
    release_date: Option<String>,
    source_urls: RcsbSourceUrls,
}

#[derive(Debug, Serialize)]
struct RcsbSourceUrls {
    structure: String,
    metadata: String,
}

impl From<&RcsbMetadata> for RcsbMetadataFile {
    fn from(value: &RcsbMetadata) -> Self {
        Self {
            registry: value.registry.clone(),
            pdb_id: value.pdb_id.clone(),
            title: value.title.clone(),
            experimental_method: value.experimental_method.clone(),
            resolution: value.resolution,
            deposition_date: value.deposition_date.clone(),
            release_date: value.release_date.clone(),
            source_urls: RcsbSourceUrls {
                structure: value.source_structure_url.clone(),
                metadata: value.source_metadata_url.clone(),
            },
        }
    }
}

fn rcsb_metadata_paths(dir: &Utf8PathBuf) -> (Utf8PathBuf, Utf8PathBuf) {
    (dir.join("metadata.json"), dir.join("metadata.raw.json"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::JsonOutput;
    use crate::store::Store;
    use camino::Utf8PathBuf;
    use std::path::Path;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockNcbi;

    impl NcbiClient for MockNcbi {
        fn download_protein(
            &self,
            _id: &ProteinId,
            _format: ProteinFormat,
            _destination: &Path,
        ) -> Result<crate::ncbi::DownloadInfo, KiraError> {
            Err(KiraError::NcbiHttp("not implemented".to_string()))
        }

        fn download_genome(
            &self,
            _accession: &GenomeAccession,
            _include: &[String],
            _destination: &Path,
        ) -> Result<crate::ncbi::DownloadInfo, KiraError> {
            Ok(crate::ncbi::DownloadInfo { is_zip: true })
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

        let app = App::new(store, MockNcbi::default(), MockRcsb::default());
        let options = FetchOptions {
            force: false,
            no_cache: false,
            dry_run: false,
        };

        let result = app
            .fetch(
                Some(DatasetSpecifier::Protein(id)),
                None,
                None,
                options,
                &JsonOutput,
            )
            .unwrap();

        assert_eq!(result.items[0].action, "cache");
    }
}
