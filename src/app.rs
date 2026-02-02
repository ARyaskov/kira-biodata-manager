use std::fs;
use std::time::Duration;

use camino::Utf8PathBuf;
use serde::Serialize;
use serde_json::Value;

use crate::config::ResolvedConfig;
use crate::config::{Config, DoiEntry, GenomeEntry, ProteinEntry, SrrEntry, UniprotEntry};
use crate::domain::{
    DatasetSpecifier, Doi, GenomeAccession, GeoSeriesAccession, ProteinFormat, ProteinId, Registry,
    SrrFormat, SrrId, UniprotId,
};
use crate::error::KiraError;
use crate::geo::{GeoClient, extract_organism, extract_supplementary_urls};
use crate::knowledge::{KnowledgeClient, parse_go_header};
use crate::ncbi::NcbiClient;
use crate::providers::doi::{DoiResolution, DoiResolver};
use crate::rcsb::{RcsbClient, RcsbMetadata};
use crate::srr::{SrrClient, ToolInfo};
use crate::store::{Metadata, Store, atomic_rename_dir};
use crate::uniprot::UniprotClient;

#[derive(Debug, Clone)]
pub struct FetchOptions {
    pub force: bool,
    pub no_cache: bool,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Default)]
pub struct FetchOverrides {
    pub protein_format: Option<ProteinFormat>,
    pub srr_format: Option<SrrFormat>,
    pub srr_paired: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FetchResult {
    pub items: Vec<FetchItemResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<FetchSummary>,
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
pub struct FetchSummary {
    pub kind: String,
    pub doi: Option<String>,
    pub id_counts: Vec<IdCount>,
    pub resolved_targets: usize,
    pub unresolved: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct IdCount {
    pub id_type: String,
    pub count: usize,
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
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClearResult {
    pub cleared: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct InitResult {
    pub path: String,
    pub written: bool,
    pub counts: InitCounts,
}

#[derive(Debug, Clone, Serialize)]
pub struct InitCounts {
    pub proteins: usize,
    pub genomes: usize,
    pub srr: usize,
    pub uniprot: usize,
    pub doi: usize,
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
pub struct App<
    N: NcbiClient,
    R: RcsbClient,
    S: SrrClient,
    U: UniprotClient,
    G: GeoClient,
    K: KnowledgeClient,
> {
    store: Store,
    ncbi: N,
    rcsb: R,
    srr: S,
    uniprot: U,
    geo: G,
    knowledge: K,
}

impl<N: NcbiClient, R: RcsbClient, S: SrrClient, U: UniprotClient, G: GeoClient, K: KnowledgeClient>
    App<N, R, S, U, G, K>
{
    pub fn new(store: Store, ncbi: N, rcsb: R, srr: S, uniprot: U, geo: G, knowledge: K) -> Self {
        Self {
            store,
            ncbi,
            rcsb,
            srr,
            uniprot,
            geo,
            knowledge,
        }
    }

    pub fn fetch(
        &self,
        specifier: Option<DatasetSpecifier>,
        config: Option<&ResolvedConfig>,
        overrides: FetchOverrides,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchResult, KiraError> {
        let mut items = Vec::new();

        if let Some(DatasetSpecifier::Doi(doi)) = specifier.clone() {
            return self.fetch_doi(doi, overrides, options, sink);
        }

        if let Some(spec) = specifier {
            items.push(self.fetch_single(spec, overrides, options.clone(), sink)?);
        } else if let Some(config) = config {
            for protein in &config.proteins {
                let spec = DatasetSpecifier::Protein(protein.id.clone());
                let format = overrides.protein_format.unwrap_or(protein.format);
                items.push(self.fetch_single(
                    spec,
                    FetchOverrides {
                        protein_format: Some(format),
                        ..FetchOverrides::default()
                    },
                    options.clone(),
                    sink,
                )?);
            }
            for genome in &config.genomes {
                items.push(self.fetch_genome_with_include(
                    genome.accession.clone(),
                    genome.include.clone(),
                    options.clone(),
                    sink,
                )?);
            }
            for srr in &config.srr {
                let format = overrides.srr_format.unwrap_or(srr.format);
                let paired = overrides.srr_paired.unwrap_or(srr.paired);
                items.push(self.fetch_srr(
                    srr.id.clone(),
                    format,
                    paired,
                    options.clone(),
                    sink,
                )?);
            }
            for uni in &config.uniprot {
                items.push(self.fetch_uniprot(uni.id.clone(), options.clone(), sink)?);
            }
            for doi in &config.doi {
                let result =
                    self.fetch_doi(doi.id.clone(), overrides.clone(), options.clone(), sink)?;
                items.extend(result.items);
            }
        } else {
            return Err(KiraError::MissingConfig);
        }

        Ok(FetchResult {
            items,
            summary: None,
        })
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
            DatasetSpecifier::Srr(id) => ("srr".to_string(), id.as_str().to_string()),
            DatasetSpecifier::Uniprot(id) => ("uniprot".to_string(), id.as_str().to_string()),
            DatasetSpecifier::Doi(id) => ("doi".to_string(), id.as_str().to_string()),
            DatasetSpecifier::Expression(id) => ("expression".to_string(), id.as_str().to_string()),
            DatasetSpecifier::Expression10x(id) => {
                ("expression10x".to_string(), id.as_str().to_string())
            }
            DatasetSpecifier::Go => ("go".to_string(), "go".to_string()),
            DatasetSpecifier::Kegg => ("kegg".to_string(), "kegg".to_string()),
            DatasetSpecifier::Reactome => ("reactome".to_string(), "reactome".to_string()),
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

        let details = match key.0.as_str() {
            "uniprot" => load_uniprot_details(project_meta.as_ref(), cache_meta.as_ref()),
            "doi" => load_doi_details(project_meta.as_ref(), cache_meta.as_ref()),
            "expression" | "expression10x" => {
                load_expression_details(project_meta.as_ref(), cache_meta.as_ref())
            }
            "go" | "kegg" | "reactome" => {
                load_kb_details(project_meta.as_ref(), cache_meta.as_ref())
            }
            _ => None,
        };

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
            details,
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

    pub fn init_config(&self, sink: &dyn ProgressSink) -> Result<InitResult, KiraError> {
        sink.event(ProgressEvent {
            message: "phase=Resolve; scanning project store".to_string(),
            elapsed: None,
        });

        let metadata = Store::list_metadata(self.store.project_root())?;
        let mut proteins = Vec::new();
        let mut genomes = Vec::new();
        let mut srr = Vec::new();
        let mut uniprot = Vec::new();
        let mut doi = Vec::new();

        for entry in metadata {
            match entry.dataset_type.as_str() {
                "protein" => {
                    let format = entry.format.as_deref().and_then(parse_protein_format);
                    if matches!(format, Some(ProteinFormat::Cif) | None) {
                        proteins.push(ProteinEntry::Shorthand(entry.id.clone()));
                    } else if let Some(format) = format {
                        proteins.push(ProteinEntry::Detailed(crate::config::ProteinEntryObject {
                            id: entry.id.clone(),
                            format: Some(format),
                        }));
                    } else {
                        proteins.push(ProteinEntry::Shorthand(entry.id.clone()));
                    }
                }
                "genome" => {
                    genomes.push(GenomeEntry::Shorthand(entry.id.clone()));
                }
                "srr" => {
                    let (format, paired) = load_srr_settings(&entry.resolved_path);
                    if matches!(format, Some(SrrFormat::Fastq) | None) && !paired.unwrap_or(false) {
                        srr.push(SrrEntry::Shorthand(entry.id.clone()));
                    } else {
                        srr.push(SrrEntry::Detailed(crate::config::SrrEntryObject {
                            id: entry.id.clone(),
                            format,
                            paired,
                        }));
                    }
                }
                "uniprot" => {
                    uniprot.push(UniprotEntry::Shorthand(entry.id.clone()));
                }
                "doi" => {
                    doi.push(DoiEntry::Shorthand(entry.id.clone()));
                }
                _ => {}
            }
        }

        let config = Config {
            schema_version: Some(1),
            proteins,
            genomes,
            srr,
            uniprot,
            doi,
        };

        sink.event(ProgressEvent {
            message: "phase=Store; writing kira-bm.json".to_string(),
            elapsed: None,
        });

        let path = std::env::current_dir()
            .map_err(|err| KiraError::Filesystem(err.to_string()))?
            .join("kira-bm.json");
        write_config_atomic(&path, &config)?;

        Ok(InitResult {
            path: path.to_string_lossy().to_string(),
            written: true,
            counts: InitCounts {
                proteins: config.proteins.len(),
                genomes: config.genomes.len(),
                srr: config.srr.len(),
                uniprot: config.uniprot.len(),
                doi: config.doi.len(),
            },
        })
    }

    fn fetch_single(
        &self,
        specifier: DatasetSpecifier,
        overrides: FetchOverrides,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchItemResult, KiraError> {
        if !options.dry_run {
            self.store.ensure_project_root()?;
            self.store.ensure_cache_root()?;
        }

        let registry = specifier.resolve_registry(overrides.protein_format);
        match (specifier, registry) {
            (DatasetSpecifier::Protein(id), Registry::Rcsb) => {
                self.fetch_protein(id, overrides.protein_format, options, sink)
            }
            (DatasetSpecifier::Protein(id), Registry::Ncbi) => {
                self.fetch_protein(id, overrides.protein_format, options, sink)
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
            (DatasetSpecifier::Srr(id), Registry::Ncbi) => self.fetch_srr(
                id,
                overrides.srr_format.unwrap_or(SrrFormat::Fastq),
                overrides.srr_paired.unwrap_or(false),
                options,
                sink,
            ),
            (DatasetSpecifier::Uniprot(id), Registry::Uniprot) => {
                self.fetch_uniprot(id, options, sink)
            }
            (DatasetSpecifier::Doi(_), Registry::Doi) => Err(KiraError::DoiResolution(
                "doi resolution must be invoked from the top-level fetch".to_string(),
            )),
            (DatasetSpecifier::Expression(acc), Registry::Geo) => {
                self.fetch_expression(acc, options, sink)
            }
            (DatasetSpecifier::Expression10x(acc), Registry::Geo) => {
                self.fetch_expression10x(acc, options, sink)
            }
            (DatasetSpecifier::Go, Registry::Go) => self.fetch_go(options, sink),
            (DatasetSpecifier::Kegg, Registry::Kegg) => self.fetch_kegg(options, sink),
            (DatasetSpecifier::Reactome, Registry::Reactome) => self.fetch_reactome(options, sink),
            _ => Err(KiraError::InvalidFormat(
                "unsupported registry for dataset type".to_string(),
            )),
        }
    }

    fn fetch_doi(
        &self,
        doi: Doi,
        overrides: FetchOverrides,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchResult, KiraError> {
        sink.event(ProgressEvent {
            message: format!("phase=Resolve; doi {}", doi.as_str()),
            elapsed: None,
        });

        if !options.dry_run {
            self.store.ensure_project_root()?;
        }

        let resolver = DoiResolver::new()?;
        let resolution_path = self.store.project_doi_resolution_path(&doi);
        let resolution = if !options.force && resolution_path.as_std_path().exists() {
            read_doi_resolution(&resolution_path)?
        } else {
            sink.event(ProgressEvent {
                message: "phase=Prepare; preparing DOI resolution".to_string(),
                elapsed: None,
            });
            sink.event(ProgressEvent {
                message: "phase=Fetch; resolving Crossref metadata".to_string(),
                elapsed: None,
            });
            sink.event(ProgressEvent {
                message: "crossref.request".to_string(),
                elapsed: None,
            });
            let result = resolver.resolve_with_progress(&doi, |msg| {
                sink.event(ProgressEvent {
                    message: msg.to_string(),
                    elapsed: None,
                });
            })?;
            sink.event(ProgressEvent {
                message: "phase=Verify; validating identifiers".to_string(),
                elapsed: None,
            });
            result
        };

        if !options.dry_run {
            let dir = self.store.project_doi_dir(&doi);
            std::fs::create_dir_all(dir.as_std_path())
                .map_err(|err| KiraError::Filesystem(err.to_string()))?;
            sink.event(ProgressEvent {
                message: "phase=Store; writing provenance".to_string(),
                elapsed: None,
            });
            write_doi_resolution(&resolution_path, &resolution)?;
            let meta = self.build_metadata("crossref", "doi", doi.as_str(), None, dir.as_str());
            Store::write_metadata(&self.store.project_doi_metadata_path(&doi), &meta)?;
        }

        let counts = resolution
            .extracted
            .counts()
            .into_iter()
            .map(|(id_type, count)| IdCount { id_type, count })
            .collect::<Vec<_>>();
        let resolved_specifiers = resolution.resolved_specifiers()?;
        let mut items = Vec::new();

        sink.event(ProgressEvent {
            message: format!(
                "doi.resolved ids={} targets={}",
                counts.iter().map(|c| c.count).sum::<usize>(),
                resolved_specifiers.len()
            ),
            elapsed: None,
        });

        for spec in resolved_specifiers {
            items.push(self.fetch_single(spec, overrides.clone(), options.clone(), sink)?);
        }

        Ok(FetchResult {
            items,
            summary: Some(FetchSummary {
                kind: "doi".to_string(),
                doi: Some(doi.as_str().to_string()),
                id_counts: counts,
                resolved_targets: resolution.resolved_targets.len(),
                unresolved: resolution.unresolved.len(),
            }),
        })
    }

    fn fetch_expression(
        &self,
        accession: GeoSeriesAccession,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchItemResult, KiraError> {
        sink.event(ProgressEvent {
            message: format!("phase=Resolve; expression {}", accession.as_str()),
            elapsed: None,
        });
        if !options.dry_run {
            self.store.ensure_project_root()?;
            self.store.ensure_cache_root()?;
        }

        let project_dir = self.store.project_expression_dir(&accession);
        let cache_dir = self.store.cache_expression_dir(&accession);

        if !options.force && self.store.project_exists(&project_dir) {
            return Ok(FetchItemResult {
                dataset_type: "expression".to_string(),
                id: accession.as_str().to_string(),
                format: None,
                source: "geo".to_string(),
                action: "project".to_string(),
                project_path: Some(project_dir.to_string()),
                cache_path: cache_dir
                    .as_std_path()
                    .exists()
                    .then(|| cache_dir.to_string()),
            });
        }

        if !options.force && self.store.cache_exists(&cache_dir) {
            if !options.dry_run {
                Store::copy_dir_atomic(&cache_dir, &project_dir)?;
                let meta = self.build_metadata(
                    "geo",
                    "expression",
                    accession.as_str(),
                    None,
                    project_dir.as_str(),
                );
                Store::write_metadata(
                    &self
                        .store
                        .project_metadata_path("expression", accession.as_str()),
                    &meta,
                )?;
            }
            return Ok(FetchItemResult {
                dataset_type: "expression".to_string(),
                id: accession.as_str().to_string(),
                format: None,
                source: "geo".to_string(),
                action: "cache".to_string(),
                project_path: Some(project_dir.to_string()),
                cache_path: Some(cache_dir.to_string()),
            });
        }

        let soft_text = self.geo.fetch_soft_text(&accession)?;
        let urls = extract_supplementary_urls(&soft_text);
        if urls.is_empty() {
            return Err(KiraError::GeoResolution(
                "GEO series contains no supplementary files".to_string(),
            ));
        }

        if options.dry_run {
            return Ok(FetchItemResult {
                dataset_type: "expression".to_string(),
                id: accession.as_str().to_string(),
                format: None,
                source: "geo".to_string(),
                action: "dry-run".to_string(),
                project_path: Some(project_dir.to_string()),
                cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
            });
        }

        let target_dir = if options.no_cache {
            &project_dir
        } else {
            &cache_dir
        };
        let parent = target_dir
            .parent()
            .ok_or_else(|| KiraError::Filesystem("invalid target dir".to_string()))?;
        let temp_dir = tempfile::Builder::new()
            .prefix("kira-bm-geo")
            .tempdir_in(parent.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let temp_path = Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf())
            .map_err(|_| KiraError::Filesystem("invalid temp dir".to_string()))?;

        let metadata_dir = temp_path.join("metadata");
        fs::create_dir_all(metadata_dir.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::write(
            metadata_dir.join("geo_soft.txt").as_std_path(),
            soft_text.as_bytes(),
        )
        .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        let mut files = Vec::new();
        for url in &urls {
            let rel = geo_relative_path(url);
            let dest = temp_path.join(&rel);
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent.as_std_path())
                    .map_err(|err| KiraError::Filesystem(err.to_string()))?;
            }
            self.geo.download_url(url, dest.as_std_path())?;
            if let Some(name) = dest.file_name() {
                files.push(name.to_string());
            }
        }

        let meta = ExpressionMetadataFile {
            registry: "geo".to_string(),
            dataset_type: "expression".to_string(),
            accession: accession.as_str().to_string(),
            organism: extract_organism(&soft_text),
            bundle_format: None,
            n_bundles: None,
            files: files.clone(),
            downloaded_at: iso_timestamp(),
        };
        let meta_path = metadata_dir.join("metadata.json");
        let meta_bytes = serde_json::to_vec_pretty(&meta)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::write(meta_path.as_std_path(), meta_bytes)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        atomic_rename_dir(temp_path.as_std_path(), target_dir.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        if !options.no_cache {
            Store::copy_dir_atomic(&cache_dir, &project_dir)?;
        }

        let project_meta = self.build_metadata(
            "geo",
            "expression",
            accession.as_str(),
            None,
            project_dir.as_str(),
        );
        Store::write_metadata(
            &self
                .store
                .project_metadata_path("expression", accession.as_str()),
            &project_meta,
        )?;

        if !options.no_cache {
            let cache_meta = self.build_metadata(
                "geo",
                "expression",
                accession.as_str(),
                None,
                cache_dir.as_str(),
            );
            Store::write_metadata(
                &self
                    .store
                    .cache_metadata_path("expression", accession.as_str()),
                &cache_meta,
            )?;
        }

        Ok(FetchItemResult {
            dataset_type: "expression".to_string(),
            id: accession.as_str().to_string(),
            format: None,
            source: "geo".to_string(),
            action: "download".to_string(),
            project_path: Some(project_dir.to_string()),
            cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
        })
    }

    fn fetch_expression10x(
        &self,
        accession: GeoSeriesAccession,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchItemResult, KiraError> {
        sink.event(ProgressEvent {
            message: format!("phase=Resolve; expression10x {}", accession.as_str()),
            elapsed: None,
        });
        if !options.dry_run {
            self.store.ensure_project_root()?;
            self.store.ensure_cache_root()?;
        }

        let project_dir = self.store.project_expression10x_dir(&accession);
        let cache_dir = self.store.cache_expression10x_dir(&accession);

        if !options.force && self.store.project_exists(&project_dir) {
            return Ok(FetchItemResult {
                dataset_type: "expression10x".to_string(),
                id: accession.as_str().to_string(),
                format: None,
                source: "geo".to_string(),
                action: "project".to_string(),
                project_path: Some(project_dir.to_string()),
                cache_path: cache_dir
                    .as_std_path()
                    .exists()
                    .then(|| cache_dir.to_string()),
            });
        }

        if !options.force && self.store.cache_exists(&cache_dir) {
            if !options.dry_run {
                Store::copy_dir_atomic(&cache_dir, &project_dir)?;
                let meta = self.build_metadata(
                    "geo",
                    "expression10x",
                    accession.as_str(),
                    None,
                    project_dir.as_str(),
                );
                Store::write_metadata(
                    &self
                        .store
                        .project_metadata_path("expression10x", accession.as_str()),
                    &meta,
                )?;
            }
            return Ok(FetchItemResult {
                dataset_type: "expression10x".to_string(),
                id: accession.as_str().to_string(),
                format: None,
                source: "geo".to_string(),
                action: "cache".to_string(),
                project_path: Some(project_dir.to_string()),
                cache_path: Some(cache_dir.to_string()),
            });
        }

        let soft_text = self.geo.fetch_soft_text(&accession)?;
        let urls = extract_supplementary_urls(&soft_text);
        let bundles = detect_10x_bundles(&urls);
        if bundles.is_empty() {
            return Err(KiraError::GeoResolution(
                "no 10x bundle found in GEO supplementary files".to_string(),
            ));
        }

        if options.dry_run {
            return Ok(FetchItemResult {
                dataset_type: "expression10x".to_string(),
                id: accession.as_str().to_string(),
                format: None,
                source: "geo".to_string(),
                action: "dry-run".to_string(),
                project_path: Some(project_dir.to_string()),
                cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
            });
        }

        let target_dir = if options.no_cache {
            &project_dir
        } else {
            &cache_dir
        };
        let parent = target_dir
            .parent()
            .ok_or_else(|| KiraError::Filesystem("invalid target dir".to_string()))?;
        let temp_dir = tempfile::Builder::new()
            .prefix("kira-bm-geo")
            .tempdir_in(parent.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let temp_path = Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf())
            .map_err(|_| KiraError::Filesystem("invalid temp dir".to_string()))?;

        let metadata_dir = temp_path.join("metadata");
        fs::create_dir_all(metadata_dir.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::write(
            metadata_dir.join("geo_soft.txt").as_std_path(),
            soft_text.as_bytes(),
        )
        .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        let mut file_names = Vec::new();
        for bundle in &bundles {
            for url in &bundle.urls {
                let rel = geo_relative_path(url);
                let dest = temp_path.join(&rel);
                if let Some(parent) = dest.parent() {
                    fs::create_dir_all(parent.as_std_path())
                        .map_err(|err| KiraError::Filesystem(err.to_string()))?;
                }
                self.geo.download_url(url, dest.as_std_path())?;
                if let Some(name) = dest.file_name() {
                    file_names.push(name.to_string());
                }
            }
        }

        let meta = ExpressionMetadataFile {
            registry: "geo".to_string(),
            dataset_type: "expression10x".to_string(),
            accession: accession.as_str().to_string(),
            organism: extract_organism(&soft_text),
            bundle_format: Some("10x".to_string()),
            n_bundles: Some(bundles.len()),
            files: unique_sorted(file_names),
            downloaded_at: iso_timestamp(),
        };
        let meta_path = metadata_dir.join("metadata.json");
        let meta_bytes = serde_json::to_vec_pretty(&meta)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::write(meta_path.as_std_path(), meta_bytes)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        atomic_rename_dir(temp_path.as_std_path(), target_dir.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        if !options.no_cache {
            Store::copy_dir_atomic(&cache_dir, &project_dir)?;
        }

        let project_meta = self.build_metadata(
            "geo",
            "expression10x",
            accession.as_str(),
            None,
            project_dir.as_str(),
        );
        Store::write_metadata(
            &self
                .store
                .project_metadata_path("expression10x", accession.as_str()),
            &project_meta,
        )?;

        if !options.no_cache {
            let cache_meta = self.build_metadata(
                "geo",
                "expression10x",
                accession.as_str(),
                None,
                cache_dir.as_str(),
            );
            Store::write_metadata(
                &self
                    .store
                    .cache_metadata_path("expression10x", accession.as_str()),
                &cache_meta,
            )?;
        }

        Ok(FetchItemResult {
            dataset_type: "expression10x".to_string(),
            id: accession.as_str().to_string(),
            format: None,
            source: "geo".to_string(),
            action: "download".to_string(),
            project_path: Some(project_dir.to_string()),
            cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
        })
    }

    fn fetch_go(
        &self,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchItemResult, KiraError> {
        sink.event(ProgressEvent {
            message: "phase=Resolve; go".to_string(),
            elapsed: None,
        });
        if !options.dry_run {
            if options.no_cache {
                self.store.ensure_project_root()?;
            } else {
                self.store.ensure_cache_root()?;
            }
        }
        let cache_dir = self.store.cache_kb_dir("go");
        let project_dir = self.store.project_kb_dir("go");
        if !options.force && self.store.cache_exists(&cache_dir) && !options.no_cache {
            return Ok(FetchItemResult {
                dataset_type: "go".to_string(),
                id: "go".to_string(),
                format: None,
                source: "go".to_string(),
                action: "cache".to_string(),
                project_path: None,
                cache_path: Some(cache_dir.to_string()),
            });
        }
        if options.dry_run {
            return Ok(FetchItemResult {
                dataset_type: "go".to_string(),
                id: "go".to_string(),
                format: None,
                source: "go".to_string(),
                action: "dry-run".to_string(),
                project_path: options.no_cache.then(|| project_dir.to_string()),
                cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
            });
        }

        let target_dir = if options.no_cache {
            &project_dir
        } else {
            &cache_dir
        };
        let parent = target_dir
            .parent()
            .ok_or_else(|| KiraError::Filesystem("invalid cache dir".to_string()))?;
        let temp_dir = tempfile::Builder::new()
            .prefix("kira-bm-go")
            .tempdir_in(parent.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let temp_path = Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf())
            .map_err(|_| KiraError::Filesystem("invalid temp dir".to_string()))?;

        let obo_path = temp_path.join("go-basic.obo");
        let obo_bytes = self.knowledge.download_go(obo_path.as_std_path())?;
        let (version, release_date) = parse_go_header(&obo_bytes);
        let meta = KnowledgeMetadataFile {
            registry: "go".to_string(),
            dataset_type: "go".to_string(),
            version,
            release_date,
            source_urls: vec!["http://purl.obolibrary.org/obo/go/go-basic.obo".to_string()],
            downloaded_at: iso_timestamp(),
        };
        let meta_path = temp_path.join("metadata.json");
        let meta_bytes = serde_json::to_vec_pretty(&meta)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::write(meta_path.as_std_path(), meta_bytes)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        atomic_rename_dir(temp_path.as_std_path(), target_dir.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        if options.no_cache {
            let project_meta = self.build_metadata("go", "go", "go", None, project_dir.as_str());
            Store::write_metadata(&self.store.project_metadata_path("go", "go"), &project_meta)?;
        } else {
            let cache_meta = self.build_metadata("go", "go", "go", None, cache_dir.as_str());
            Store::write_metadata(&self.store.cache_metadata_path("go", "go"), &cache_meta)?;
        }

        Ok(FetchItemResult {
            dataset_type: "go".to_string(),
            id: "go".to_string(),
            format: None,
            source: "go".to_string(),
            action: "download".to_string(),
            project_path: options.no_cache.then(|| project_dir.to_string()),
            cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
        })
    }

    fn fetch_kegg(
        &self,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchItemResult, KiraError> {
        sink.event(ProgressEvent {
            message: "phase=Resolve; kegg".to_string(),
            elapsed: None,
        });
        if !options.dry_run {
            if options.no_cache {
                self.store.ensure_project_root()?;
            } else {
                self.store.ensure_cache_root()?;
            }
        }
        let cache_dir = self.store.cache_kb_dir("kegg");
        let project_dir = self.store.project_kb_dir("kegg");
        if !options.force && self.store.cache_exists(&cache_dir) && !options.no_cache {
            return Ok(FetchItemResult {
                dataset_type: "kegg".to_string(),
                id: "kegg".to_string(),
                format: None,
                source: "kegg".to_string(),
                action: "cache".to_string(),
                project_path: None,
                cache_path: Some(cache_dir.to_string()),
            });
        }
        if options.dry_run {
            return Ok(FetchItemResult {
                dataset_type: "kegg".to_string(),
                id: "kegg".to_string(),
                format: None,
                source: "kegg".to_string(),
                action: "dry-run".to_string(),
                project_path: options.no_cache.then(|| project_dir.to_string()),
                cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
            });
        }

        let target_dir = if options.no_cache {
            &project_dir
        } else {
            &cache_dir
        };
        let parent = target_dir
            .parent()
            .ok_or_else(|| KiraError::Filesystem("invalid cache dir".to_string()))?;
        let temp_dir = tempfile::Builder::new()
            .prefix("kira-bm-kegg")
            .tempdir_in(parent.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let temp_path = Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf())
            .map_err(|_| KiraError::Filesystem("invalid temp dir".to_string()))?;

        let list_path = temp_path.join("pathway_list.txt");
        let link_path = temp_path.join("pathway_ko.txt");
        self.knowledge
            .download_kegg_pathways(list_path.as_std_path())?;
        self.knowledge
            .download_kegg_pathway_links(link_path.as_std_path())?;
        let meta = KnowledgeMetadataFile {
            registry: "kegg".to_string(),
            dataset_type: "kegg".to_string(),
            version: None,
            release_date: None,
            source_urls: vec![
                "https://rest.kegg.jp/list/pathway".to_string(),
                "https://rest.kegg.jp/link/pathway/ko".to_string(),
            ],
            downloaded_at: iso_timestamp(),
        };
        let meta_path = temp_path.join("metadata.json");
        let meta_bytes = serde_json::to_vec_pretty(&meta)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::write(meta_path.as_std_path(), meta_bytes)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        atomic_rename_dir(temp_path.as_std_path(), target_dir.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        if options.no_cache {
            let project_meta =
                self.build_metadata("kegg", "kegg", "kegg", None, project_dir.as_str());
            Store::write_metadata(
                &self.store.project_metadata_path("kegg", "kegg"),
                &project_meta,
            )?;
        } else {
            let cache_meta = self.build_metadata("kegg", "kegg", "kegg", None, cache_dir.as_str());
            Store::write_metadata(&self.store.cache_metadata_path("kegg", "kegg"), &cache_meta)?;
        }

        Ok(FetchItemResult {
            dataset_type: "kegg".to_string(),
            id: "kegg".to_string(),
            format: None,
            source: "kegg".to_string(),
            action: "download".to_string(),
            project_path: options.no_cache.then(|| project_dir.to_string()),
            cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
        })
    }

    fn fetch_reactome(
        &self,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchItemResult, KiraError> {
        sink.event(ProgressEvent {
            message: "phase=Resolve; reactome".to_string(),
            elapsed: None,
        });
        if !options.dry_run {
            if options.no_cache {
                self.store.ensure_project_root()?;
            } else {
                self.store.ensure_cache_root()?;
            }
        }
        let cache_dir = self.store.cache_kb_dir("reactome");
        let project_dir = self.store.project_kb_dir("reactome");
        if !options.force && self.store.cache_exists(&cache_dir) && !options.no_cache {
            return Ok(FetchItemResult {
                dataset_type: "reactome".to_string(),
                id: "reactome".to_string(),
                format: None,
                source: "reactome".to_string(),
                action: "cache".to_string(),
                project_path: None,
                cache_path: Some(cache_dir.to_string()),
            });
        }
        if options.dry_run {
            return Ok(FetchItemResult {
                dataset_type: "reactome".to_string(),
                id: "reactome".to_string(),
                format: None,
                source: "reactome".to_string(),
                action: "dry-run".to_string(),
                project_path: options.no_cache.then(|| project_dir.to_string()),
                cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
            });
        }

        let target_dir = if options.no_cache {
            &project_dir
        } else {
            &cache_dir
        };
        let parent = target_dir
            .parent()
            .ok_or_else(|| KiraError::Filesystem("invalid cache dir".to_string()))?;
        let temp_dir = tempfile::Builder::new()
            .prefix("kira-bm-reactome")
            .tempdir_in(parent.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let temp_path = Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf())
            .map_err(|_| KiraError::Filesystem("invalid temp dir".to_string()))?;

        let pathways_path = temp_path.join("ReactomePathways.txt");
        let mapping_path = temp_path.join("UniProt2Reactome.txt");
        self.knowledge
            .download_reactome_pathways(pathways_path.as_std_path())?;
        self.knowledge
            .download_reactome_mappings(mapping_path.as_std_path())?;
        let meta = KnowledgeMetadataFile {
            registry: "reactome".to_string(),
            dataset_type: "reactome".to_string(),
            version: None,
            release_date: None,
            source_urls: vec![
                "https://reactome.org/download/current/ReactomePathways.txt".to_string(),
                "https://reactome.org/download/current/UniProt2Reactome.txt".to_string(),
            ],
            downloaded_at: iso_timestamp(),
        };
        let meta_path = temp_path.join("metadata.json");
        let meta_bytes = serde_json::to_vec_pretty(&meta)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::write(meta_path.as_std_path(), meta_bytes)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        atomic_rename_dir(temp_path.as_std_path(), target_dir.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        if options.no_cache {
            let project_meta = self.build_metadata(
                "reactome",
                "reactome",
                "reactome",
                None,
                project_dir.as_str(),
            );
            Store::write_metadata(
                &self.store.project_metadata_path("reactome", "reactome"),
                &project_meta,
            )?;
        } else {
            let cache_meta =
                self.build_metadata("reactome", "reactome", "reactome", None, cache_dir.as_str());
            Store::write_metadata(
                &self.store.cache_metadata_path("reactome", "reactome"),
                &cache_meta,
            )?;
        }

        Ok(FetchItemResult {
            dataset_type: "reactome".to_string(),
            id: "reactome".to_string(),
            format: None,
            source: "reactome".to_string(),
            action: "download".to_string(),
            project_path: options.no_cache.then(|| project_dir.to_string()),
            cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
        })
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

    fn fetch_srr(
        &self,
        id: SrrId,
        format: SrrFormat,
        paired: bool,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchItemResult, KiraError> {
        sink.event(ProgressEvent {
            message: format!("phase=Resolve; srr {}", id.as_str()),
            elapsed: None,
        });
        if !options.dry_run {
            self.store.ensure_project_root()?;
            self.store.ensure_cache_root()?;
        }

        let project_dir = self.store.project_srr_dir(&id);
        let cache_dir = self.store.cache_srr_dir(&id);

        if !options.force && self.store.project_exists(&project_dir) {
            sink.event(ProgressEvent {
                message: "phase=Store; already in project store".to_string(),
                elapsed: None,
            });
            return Ok(FetchItemResult {
                dataset_type: "srr".to_string(),
                id: id.as_str().to_string(),
                format: Some(format.to_string()),
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
                    "srr",
                    id.as_str(),
                    Some(format.to_string()),
                    project_dir.as_str(),
                );
                Store::write_metadata(
                    &self.store.project_metadata_path("srr", id.as_str()),
                    &meta,
                )?;
            }
            return Ok(FetchItemResult {
                dataset_type: "srr".to_string(),
                id: id.as_str().to_string(),
                format: Some(format.to_string()),
                source: "ncbi".to_string(),
                action: "cache".to_string(),
                project_path: Some(project_dir.to_string()),
                cache_path: Some(cache_dir.to_string()),
            });
        }

        if options.dry_run {
            return Ok(FetchItemResult {
                dataset_type: "srr".to_string(),
                id: id.as_str().to_string(),
                format: Some(format.to_string()),
                source: "ncbi".to_string(),
                action: "download".to_string(),
                project_path: Some(project_dir.to_string()),
                cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
            });
        }

        let temp_dir = tempfile::Builder::new()
            .prefix("kira-bm-srr")
            .tempdir_in(self.store.project_root().as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let staging_dir = temp_dir.path().join("staging");
        fs::create_dir_all(&staging_dir).map_err(|err| KiraError::Filesystem(err.to_string()))?;

        sink.event(ProgressEvent {
            message: "phase=Prepare; preparing download".to_string(),
            elapsed: None,
        });
        sink.event(ProgressEvent {
            message: "ncbi.request".to_string(),
            elapsed: None,
        });
        let start = std::time::Instant::now();
        let fastq_files = self.srr.download_fastq(&id, paired, &staging_dir)?;
        let detected_paired = !paired && detect_paired_fastq(&fastq_files);
        let paired = paired || detected_paired;
        let latency = start.elapsed().as_millis();
        sink.event(ProgressEvent {
            message: format!("ncbi.response latency_ms={latency}"),
            elapsed: None,
        });

        sink.event(ProgressEvent {
            message: "phase=Verify; validating package".to_string(),
            elapsed: None,
        });

        let normalized_dir = temp_dir.path().join("normalized");
        fs::create_dir_all(&normalized_dir)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let normalized_fastq = normalize_fastq_files(&fastq_files, paired, &normalized_dir)?;

        let output_files = match format {
            SrrFormat::Fastq => normalized_fastq.clone(),
            SrrFormat::Fasta => {
                let mut fasta_files = Vec::new();
                for fastq in &normalized_fastq {
                    let fasta = replace_ext(fastq, "fasta")?;
                    fastq_to_fasta(fastq, &fasta)?;
                    fasta_files.push(fasta);
                }
                fasta_files
            }
        };

        sink.event(ProgressEvent {
            message: "phase=Store; writing files".to_string(),
            elapsed: None,
        });

        for file in &output_files {
            let utf8 = Utf8PathBuf::from_path_buf(file.to_path_buf())
                .map_err(|_| KiraError::Filesystem("non-utf8 file path in dataset".to_string()))?;
            let target = project_dir.join(
                file.file_name()
                    .ok_or_else(|| KiraError::Filesystem("invalid SRR output file".to_string()))?
                    .to_string_lossy()
                    .to_string(),
            );
            Store::copy_file_atomic(&utf8, &target)?;
        }

        let tools = self.srr.tool_info();
        let metadata = SrrMetadataFile {
            registry: "ncbi".to_string(),
            dataset_type: "srr".to_string(),
            accession: id.as_str().to_string(),
            format: format.to_string(),
            paired,
            downloaded_at: iso_timestamp(),
            tools,
            source_fastq: if format == SrrFormat::Fasta {
                Some(
                    normalized_fastq
                        .iter()
                        .filter_map(|path| {
                            path.file_name().map(|n| n.to_string_lossy().to_string())
                        })
                        .collect(),
                )
            } else {
                None
            },
            conversion: if format == SrrFormat::Fasta {
                Some("fastq_to_fasta".to_string())
            } else {
                None
            },
        };
        write_srr_metadata(&project_dir, &metadata)?;

        let meta = self.build_metadata(
            "ncbi",
            "srr",
            id.as_str(),
            Some(format.to_string()),
            project_dir.as_str(),
        );
        Store::write_metadata(&self.store.project_metadata_path("srr", id.as_str()), &meta)?;

        if !options.no_cache {
            Store::copy_dir_atomic(&project_dir, &cache_dir)?;
            write_srr_metadata(&cache_dir, &metadata)?;
            let meta = self.build_metadata(
                "ncbi",
                "srr",
                id.as_str(),
                Some(format.to_string()),
                cache_dir.as_str(),
            );
            Store::write_metadata(&self.store.cache_metadata_path("srr", id.as_str()), &meta)?;
        }

        Ok(FetchItemResult {
            dataset_type: "srr".to_string(),
            id: id.as_str().to_string(),
            format: Some(format.to_string()),
            source: "ncbi".to_string(),
            action: "download".to_string(),
            project_path: Some(project_dir.to_string()),
            cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
        })
    }

    fn fetch_uniprot(
        &self,
        id: UniprotId,
        options: FetchOptions,
        sink: &dyn ProgressSink,
    ) -> Result<FetchItemResult, KiraError> {
        sink.event(ProgressEvent {
            message: format!("phase=Resolve; uniprot {}", id.as_str()),
            elapsed: None,
        });
        if !options.dry_run {
            self.store.ensure_project_root()?;
            self.store.ensure_cache_root()?;
        }

        let project_dir = self.store.project_uniprot_dir(&id);
        let cache_dir = self.store.cache_uniprot_dir(&id);

        if !options.force && self.store.project_exists(&project_dir) {
            sink.event(ProgressEvent {
                message: "phase=Store; already in project store".to_string(),
                elapsed: None,
            });
            return Ok(FetchItemResult {
                dataset_type: "uniprot".to_string(),
                id: id.as_str().to_string(),
                format: None,
                source: "uniprot".to_string(),
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
                    "uniprot",
                    "uniprot",
                    id.as_str(),
                    None,
                    project_dir.as_str(),
                );
                Store::write_metadata(
                    &self.store.project_metadata_path("uniprot", id.as_str()),
                    &meta,
                )?;
            }
            return Ok(FetchItemResult {
                dataset_type: "uniprot".to_string(),
                id: id.as_str().to_string(),
                format: None,
                source: "uniprot".to_string(),
                action: "cache".to_string(),
                project_path: Some(project_dir.to_string()),
                cache_path: Some(cache_dir.to_string()),
            });
        }

        if options.dry_run {
            return Ok(FetchItemResult {
                dataset_type: "uniprot".to_string(),
                id: id.as_str().to_string(),
                format: None,
                source: "uniprot".to_string(),
                action: "download".to_string(),
                project_path: Some(project_dir.to_string()),
                cache_path: (!options.no_cache).then(|| cache_dir.to_string()),
            });
        }

        let temp_dir = tempfile::Builder::new()
            .prefix("kira-bm-uniprot")
            .tempdir_in(self.store.project_root().as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        let staging_dir = temp_dir.path().join("dataset");
        fs::create_dir_all(&staging_dir).map_err(|err| KiraError::Filesystem(err.to_string()))?;

        sink.event(ProgressEvent {
            message: "phase=Prepare; preparing download".to_string(),
            elapsed: None,
        });
        sink.event(ProgressEvent {
            message: "uniprot.request".to_string(),
            elapsed: None,
        });
        let start = std::time::Instant::now();
        let record = self.uniprot.fetch(&id)?;
        let latency = start.elapsed().as_millis();
        sink.event(ProgressEvent {
            message: format!("uniprot.response latency_ms={latency}"),
            elapsed: None,
        });

        sink.event(ProgressEvent {
            message: "phase=Store; writing files".to_string(),
            elapsed: None,
        });

        let fasta_path = staging_dir.join(format!("{}.fasta", id.as_str()));
        fs::write(&fasta_path, record.fasta.as_bytes())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        let meta_path = staging_dir.join("metadata.json");
        let meta_bytes = serde_json::to_vec_pretty(&record.metadata)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::write(&meta_path, &meta_bytes).map_err(|err| KiraError::Filesystem(err.to_string()))?;

        let raw_path = staging_dir.join("raw.json");
        let raw_bytes = serde_json::to_vec_pretty(&record.raw_json)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::write(&raw_path, &raw_bytes).map_err(|err| KiraError::Filesystem(err.to_string()))?;

        atomic_rename_dir(&staging_dir, project_dir.as_std_path())
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;

        let meta = self.build_metadata(
            "uniprot",
            "uniprot",
            id.as_str(),
            None,
            project_dir.as_str(),
        );
        Store::write_metadata(
            &self.store.project_metadata_path("uniprot", id.as_str()),
            &meta,
        )?;

        if !options.no_cache {
            Store::copy_dir_atomic(&project_dir, &cache_dir)?;
            let meta =
                self.build_metadata("uniprot", "uniprot", id.as_str(), None, cache_dir.as_str());
            Store::write_metadata(
                &self.store.cache_metadata_path("uniprot", id.as_str()),
                &meta,
            )?;
        }

        Ok(FetchItemResult {
            dataset_type: "uniprot".to_string(),
            id: id.as_str().to_string(),
            format: None,
            source: "uniprot".to_string(),
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

#[derive(Debug, Serialize)]
struct ExpressionMetadataFile {
    registry: String,
    #[serde(rename = "type")]
    dataset_type: String,
    accession: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    organism: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bundle_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    n_bundles: Option<usize>,
    files: Vec<String>,
    downloaded_at: String,
}

#[derive(Debug, Serialize)]
struct KnowledgeMetadataFile {
    registry: String,
    #[serde(rename = "type")]
    dataset_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    release_date: Option<String>,
    source_urls: Vec<String>,
    downloaded_at: String,
}

#[derive(Debug)]
struct Bundle {
    urls: Vec<String>,
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

#[derive(Debug, Serialize)]
struct SrrMetadataFile {
    registry: String,
    dataset_type: String,
    accession: String,
    format: String,
    paired: bool,
    downloaded_at: String,
    tools: ToolInfo,
    source_fastq: Option<Vec<String>>,
    conversion: Option<String>,
}

fn write_srr_metadata(dir: &Utf8PathBuf, metadata: &SrrMetadataFile) -> Result<(), KiraError> {
    let path = dir.join("metadata.json");
    let content = serde_json::to_vec_pretty(metadata)
        .map_err(|err| KiraError::Filesystem(err.to_string()))?;
    Store::write_bytes_atomic(&path, &content)
}

fn normalize_fastq_files(
    files: &[std::path::PathBuf],
    paired: bool,
    out_dir: &std::path::Path,
) -> Result<Vec<std::path::PathBuf>, KiraError> {
    if files.is_empty() {
        return Err(KiraError::Filesystem(
            "no FASTQ files produced for SRR dataset".to_string(),
        ));
    }
    let mut sorted = files.to_vec();
    sorted.sort();

    let mut outputs = Vec::new();
    if paired {
        let first = sorted
            .get(0)
            .ok_or_else(|| KiraError::Filesystem("missing paired FASTQ file 1".to_string()))?;
        let second = sorted
            .get(1)
            .ok_or_else(|| KiraError::Filesystem("missing paired FASTQ file 2".to_string()))?;
        let out1 = out_dir.join("reads_1.fastq");
        let out2 = out_dir.join("reads_2.fastq");
        fs::copy(first, &out1).map_err(|err| KiraError::Filesystem(err.to_string()))?;
        fs::copy(second, &out2).map_err(|err| KiraError::Filesystem(err.to_string()))?;
        outputs.push(out1);
        outputs.push(out2);
    } else {
        let first = sorted
            .get(0)
            .ok_or_else(|| KiraError::Filesystem("missing FASTQ file".to_string()))?;
        let out = out_dir.join("reads.fastq");
        fs::copy(first, &out).map_err(|err| KiraError::Filesystem(err.to_string()))?;
        outputs.push(out);
    }
    Ok(outputs)
}

fn detect_paired_fastq(files: &[std::path::PathBuf]) -> bool {
    let mut has_1 = false;
    let mut has_2 = false;
    for file in files {
        if let Some(name) = file.file_name().and_then(|n| n.to_str()) {
            if name.contains("_1") {
                has_1 = true;
            }
            if name.contains("_2") {
                has_2 = true;
            }
        }
    }
    has_1 && has_2
}

fn replace_ext(path: &std::path::Path, ext: &str) -> Result<std::path::PathBuf, KiraError> {
    let mut new = path.to_path_buf();
    new.set_extension(ext);
    Ok(new)
}

fn load_uniprot_details(project: Option<&Metadata>, cache: Option<&Metadata>) -> Option<Value> {
    let paths = [
        project.and_then(|meta| Some(meta.resolved_path.clone())),
        cache.and_then(|meta| Some(meta.resolved_path.clone())),
    ];
    for path in paths.into_iter().flatten() {
        let meta_path = std::path::Path::new(&path).join("metadata.json");
        if let Ok(content) = std::fs::read_to_string(&meta_path) {
            if let Ok(value) = serde_json::from_str::<Value>(&content) {
                return Some(value);
            }
        }
    }
    None
}

fn load_doi_details(project: Option<&Metadata>, cache: Option<&Metadata>) -> Option<Value> {
    let paths = [
        project.and_then(|meta| Some(meta.resolved_path.clone())),
        cache.and_then(|meta| Some(meta.resolved_path.clone())),
    ];
    for path in paths.into_iter().flatten() {
        let meta_path = std::path::Path::new(&path).join("doi_resolution.json");
        if let Ok(content) = std::fs::read_to_string(&meta_path) {
            if let Ok(value) = serde_json::from_str::<Value>(&content) {
                return Some(value);
            }
        }
    }
    None
}

fn load_expression_details(project: Option<&Metadata>, cache: Option<&Metadata>) -> Option<Value> {
    let paths = [
        project.and_then(|meta| Some(meta.resolved_path.clone())),
        cache.and_then(|meta| Some(meta.resolved_path.clone())),
    ];
    for path in paths.into_iter().flatten() {
        let meta_path = std::path::Path::new(&path)
            .join("metadata")
            .join("metadata.json");
        if let Ok(content) = std::fs::read_to_string(&meta_path) {
            if let Ok(value) = serde_json::from_str::<Value>(&content) {
                return Some(value);
            }
        }
    }
    None
}

fn load_kb_details(project: Option<&Metadata>, cache: Option<&Metadata>) -> Option<Value> {
    let paths = [
        project.and_then(|meta| Some(meta.resolved_path.clone())),
        cache.and_then(|meta| Some(meta.resolved_path.clone())),
    ];
    for path in paths.into_iter().flatten() {
        let meta_path = std::path::Path::new(&path).join("metadata.json");
        if let Ok(content) = std::fs::read_to_string(&meta_path) {
            if let Ok(value) = serde_json::from_str::<Value>(&content) {
                return Some(value);
            }
        }
    }
    None
}

fn geo_relative_path(url: &str) -> String {
    let without_query = url.split('?').next().unwrap_or(url);
    if let Some(idx) = without_query.find("/suppl/") {
        return without_query[(idx + "/suppl/".len())..].to_string();
    }
    without_query
        .rsplit('/')
        .next()
        .unwrap_or(without_query)
        .to_string()
}

fn detect_10x_bundles(urls: &[String]) -> Vec<Bundle> {
    let mut map: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    for url in urls {
        let rel = geo_relative_path(url);
        let file_name = rel.rsplit('/').next().unwrap_or(&rel);
        if !matches!(
            file_name,
            "matrix.mtx.gz" | "barcodes.tsv.gz" | "features.tsv.gz" | "genes.tsv.gz"
        ) {
            continue;
        }
        let key = rel
            .rsplit_once('/')
            .map(|(dir, _)| dir.to_string())
            .unwrap_or_else(|| "root".to_string());
        map.entry(key).or_default().push(url.clone());
    }

    let mut bundles = Vec::new();
    for (_key, bundle_urls) in map {
        let mut has_matrix = false;
        let mut has_barcodes = false;
        let mut has_features = false;
        for url in &bundle_urls {
            let rel = geo_relative_path(url);
            let file_name = rel.rsplit('/').next().unwrap_or(&rel);
            match file_name {
                "matrix.mtx.gz" => has_matrix = true,
                "barcodes.tsv.gz" => has_barcodes = true,
                "features.tsv.gz" | "genes.tsv.gz" => has_features = true,
                _ => {}
            }
        }
        if has_matrix && has_barcodes && has_features {
            bundles.push(Bundle { urls: bundle_urls });
        }
    }
    bundles
}

fn unique_sorted(mut items: Vec<String>) -> Vec<String> {
    items.sort();
    items.dedup();
    items
}

fn read_doi_resolution(path: &Utf8PathBuf) -> Result<DoiResolution, KiraError> {
    let content = std::fs::read_to_string(path.as_std_path())
        .map_err(|err| KiraError::Filesystem(err.to_string()))?;
    serde_json::from_str(&content).map_err(|err| KiraError::Filesystem(err.to_string()))
}

fn write_doi_resolution(path: &Utf8PathBuf, value: &DoiResolution) -> Result<(), KiraError> {
    let bytes =
        serde_json::to_vec_pretty(value).map_err(|err| KiraError::Filesystem(err.to_string()))?;
    Store::write_bytes_atomic(path, &bytes)
}

fn parse_protein_format(value: &str) -> Option<ProteinFormat> {
    match value.to_lowercase().as_str() {
        "cif" => Some(ProteinFormat::Cif),
        "pdb" => Some(ProteinFormat::Pdb),
        "bcif" => Some(ProteinFormat::Bcif),
        _ => None,
    }
}

fn load_srr_settings(path: &str) -> (Option<SrrFormat>, Option<bool>) {
    #[derive(serde::Deserialize)]
    struct SrrMeta {
        format: Option<SrrFormat>,
        paired: Option<bool>,
    }
    let meta_path = std::path::Path::new(path).join("metadata.json");
    let content = match std::fs::read_to_string(&meta_path) {
        Ok(value) => value,
        Err(_) => return (None, None),
    };
    let parsed: SrrMeta = match serde_json::from_str(&content) {
        Ok(value) => value,
        Err(_) => return (None, None),
    };
    (parsed.format, parsed.paired)
}

fn write_config_atomic(path: &std::path::Path, config: &Config) -> Result<(), KiraError> {
    let payload =
        serde_json::to_vec_pretty(config).map_err(|err| KiraError::Filesystem(err.to_string()))?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &payload).map_err(|err| KiraError::Filesystem(err.to_string()))?;
    std::fs::rename(&tmp, path).map_err(|err| KiraError::Filesystem(err.to_string()))?;
    Ok(())
}

fn fastq_to_fasta(input: &std::path::Path, output: &std::path::Path) -> Result<(), KiraError> {
    let content =
        std::fs::read_to_string(input).map_err(|err| KiraError::Filesystem(err.to_string()))?;
    let mut out = String::new();
    let mut lines = content.lines();
    loop {
        let header = match lines.next() {
            Some(value) => value,
            None => break,
        };
        let seq = lines.next().ok_or_else(|| {
            KiraError::SrrConversion("invalid FASTQ: missing sequence".to_string())
        })?;
        let _plus = lines.next().ok_or_else(|| {
            KiraError::SrrConversion("invalid FASTQ: missing plus line".to_string())
        })?;
        let _qual = lines.next().ok_or_else(|| {
            KiraError::SrrConversion("invalid FASTQ: missing quality".to_string())
        })?;

        let header = header.strip_prefix('@').unwrap_or(header);
        out.push('>');
        out.push_str(header);
        out.push('\n');
        out.push_str(seq);
        out.push('\n');
    }
    std::fs::write(output, out).map_err(|err| KiraError::Filesystem(err.to_string()))?;
    Ok(())
}
