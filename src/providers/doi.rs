use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use regex::Regex;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::domain::{DatasetSpecifier, Doi};
use crate::error::KiraError;

const CROSSREF_BASE: &str = "https://api.crossref.org";
const EUTILS_BASE: &str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils";
const RCSB_BASE: &str = "https://data.rcsb.org/rest/v1/core/entry";
const UNIPROT_BASE: &str = "https://rest.uniprot.org/uniprotkb";
const NCBI_DATASETS_BASE: &str = "https://api.ncbi.nlm.nih.gov/datasets/v2";
const ENA_PORTAL_BASE: &str = "https://www.ebi.ac.uk/ena/portal/api";
const GEO_TEXT_BASE: &str = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi";

#[derive(Debug, Clone)]
pub struct DoiResolver {
    client: Client,
}

impl DoiResolver {
    pub fn new() -> Result<Self, KiraError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(format!("kira-bm/{}", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        Ok(Self { client })
    }

    pub fn resolve(&self, doi: &Doi) -> Result<DoiResolution, KiraError> {
        self.resolve_with_progress(doi, |_| {})
    }

    pub fn resolve_with_progress<F>(
        &self,
        doi: &Doi,
        mut progress: F,
    ) -> Result<DoiResolution, KiraError>
    where
        F: FnMut(&str),
    {
        progress("doi.crossref.start");
        let crossref = self.fetch_crossref(doi)?;
        progress("doi.crossref.done");
        let (source, texts) = collect_source(&crossref);
        let extracted = extract_ids(&texts);
        progress(&format!(
            "doi.extract gse={} gsm={} srr={} err={} bioproject={} ena_project={} assembly={} pdb={} uniprot={}",
            extracted.geo_series.len(),
            extracted.geo_samples.len(),
            extracted.sra_runs.len(),
            extracted.ena_runs.len(),
            extracted.bioprojects.len(),
            extracted.ena_projects.len(),
            extracted.assemblies.len(),
            extracted.pdb.len(),
            extracted.uniprot.len()
        ));

        if extracted.is_empty() {
            return Err(KiraError::DoiResolution(
                "DOI resolved successfully, but no supported public dataset identifiers were found"
                    .to_string(),
            ));
        }

        let mut validation = ValidationSummary::default();
        let mut hydrated = HydratedSummary::default();
        let mut resolved_targets = BTreeSet::new();
        let mut unresolved = Vec::new();
        progress("doi.validate.pdb");
        for id in &extracted.pdb {
            let exists = self.validate_pdb(id)?;
            validation.pdb.push(IdStatus::new("pdb", id, exists, None));
            if exists {
                resolved_targets.insert(ResolvedTarget::new("protein", id));
            } else {
                unresolved.push(UnresolvedId::new("pdb", id, "not found"));
            }
        }

        progress("doi.validate.uniprot");
        for id in &extracted.uniprot {
            let exists = self.validate_uniprot(id)?;
            validation
                .uniprot
                .push(IdStatus::new("uniprot", id, exists, None));
            if exists {
                resolved_targets.insert(ResolvedTarget::new("uniprot", id));
            } else {
                unresolved.push(UnresolvedId::new("uniprot", id, "not found"));
            }
        }

        progress("doi.validate.assembly");
        for id in &extracted.assemblies {
            let exists = self.validate_assembly(id)?;
            validation
                .assemblies
                .push(IdStatus::new("assembly", id, exists, None));
            if exists {
                resolved_targets.insert(ResolvedTarget::new("genome", id));
            } else {
                unresolved.push(UnresolvedId::new("assembly", id, "not found"));
            }
        }

        progress("doi.validate.srr");
        for id in &extracted.sra_runs {
            let exists = self.validate_sra_run(id)?;
            validation
                .sra_runs
                .push(IdStatus::new("srr", id, exists, None));
            if exists {
                resolved_targets.insert(ResolvedTarget::new("srr", id));
            } else {
                unresolved.push(UnresolvedId::new("srr", id, "not found"));
            }
        }

        progress("doi.validate.err");
        for id in &extracted.ena_runs {
            let exists = self.validate_sra_run(id)?;
            validation
                .ena_runs
                .push(IdStatus::new("err", id, exists, None));
            if exists {
                resolved_targets.insert(ResolvedTarget::new("srr", id));
            } else {
                unresolved.push(UnresolvedId::new("err", id, "not found"));
            }
        }

        progress("doi.hydrate.geo_series");
        for gse in &extracted.geo_series {
            let exists = self.validate_geo(gse)?;
            validation
                .geo_series
                .push(IdStatus::new("gse", gse, exists, None));
            if !exists {
                unresolved.push(UnresolvedId::new("gse", gse, "not found"));
                continue;
            }

            match self.hydrate_geo_series(gse) {
                Ok(geo) => {
                    for gsm in &geo.gsm {
                        validation
                            .geo_samples
                            .push(IdStatus::new("gsm", gsm, true, None));
                    }
                    for run in &geo.srr {
                        let exists = self.validate_sra_run(run)?;
                        validation.sra_runs.push(IdStatus::new(
                            "srr",
                            run,
                            exists,
                            Some("from gse".to_string()),
                        ));
                        if exists {
                            resolved_targets.insert(ResolvedTarget::new("srr", run));
                        } else {
                            unresolved.push(UnresolvedId::new("srr", run, "not found"));
                        }
                    }
                    hydrated.geo.push(geo);
                }
                Err(err) => {
                    unresolved.push(UnresolvedId::new(
                        "gse",
                        gse,
                        &format!("hydration failed: {}", err),
                    ));
                }
            }
        }

        progress("doi.hydrate.geo_samples");
        for gsm in &extracted.geo_samples {
            let exists = self.validate_geo(gsm)?;
            validation
                .geo_samples
                .push(IdStatus::new("gsm", gsm, exists, None));
            if !exists {
                unresolved.push(UnresolvedId::new("gsm", gsm, "not found"));
                continue;
            }
            match self.hydrate_geo_sample(gsm) {
                Ok(runs) => {
                    for run in runs {
                        let exists = self.validate_sra_run(&run)?;
                        validation.sra_runs.push(IdStatus::new(
                            "srr",
                            &run,
                            exists,
                            Some("from gsm".to_string()),
                        ));
                        if exists {
                            resolved_targets.insert(ResolvedTarget::new("srr", &run));
                        } else {
                            unresolved.push(UnresolvedId::new("srr", &run, "not found"));
                        }
                    }
                }
                Err(err) => {
                    unresolved.push(UnresolvedId::new(
                        "gsm",
                        gsm,
                        &format!("hydration failed: {}", err),
                    ));
                }
            }
        }

        progress("doi.hydrate.bioproject");
        for project in &extracted.bioprojects {
            let ids = self.esearch_ids("bioproject", &format!("{project}[Accession]"))?;
            if ids.is_empty() {
                validation
                    .bioprojects
                    .push(IdStatus::new("bioproject", project, false, None));
                unresolved.push(UnresolvedId::new("bioproject", project, "not found"));
                continue;
            }
            validation
                .bioprojects
                .push(IdStatus::new("bioproject", project, true, None));
            match self.hydrate_bioproject(project, &ids) {
                Ok(item) => {
                    for run in &item.srr {
                        let exists = self.validate_sra_run(run)?;
                        validation.sra_runs.push(IdStatus::new(
                            "srr",
                            run,
                            exists,
                            Some("from bioproject".to_string()),
                        ));
                        if exists {
                            resolved_targets.insert(ResolvedTarget::new("srr", run));
                        } else {
                            unresolved.push(UnresolvedId::new("srr", run, "not found"));
                        }
                    }
                    for acc in &item.assemblies {
                        let exists = self.validate_assembly(acc)?;
                        validation.assemblies.push(IdStatus::new(
                            "assembly",
                            acc,
                            exists,
                            Some("from bioproject".to_string()),
                        ));
                        if exists {
                            resolved_targets.insert(ResolvedTarget::new("genome", acc));
                        } else {
                            unresolved.push(UnresolvedId::new("assembly", acc, "not found"));
                        }
                    }
                    hydrated.bioprojects.push(item);
                }
                Err(err) => {
                    unresolved.push(UnresolvedId::new(
                        "bioproject",
                        project,
                        &format!("hydration failed: {}", err),
                    ));
                }
            }
        }

        progress("doi.hydrate.ena_project");
        for project in &extracted.ena_projects {
            match self.hydrate_ena_project(project) {
                Ok(item) => {
                    if item.runs.is_empty() {
                        validation.ena_projects.push(IdStatus::new(
                            "ena_project",
                            project,
                            false,
                            None,
                        ));
                        unresolved.push(UnresolvedId::new("ena_project", project, "no runs found"));
                        continue;
                    }
                    validation
                        .ena_projects
                        .push(IdStatus::new("ena_project", project, true, None));
                    for run in &item.runs {
                        let exists = self.validate_sra_run(run)?;
                        validation.ena_runs.push(IdStatus::new(
                            "err",
                            run,
                            exists,
                            Some("from ena_project".to_string()),
                        ));
                        if exists {
                            resolved_targets.insert(ResolvedTarget::new("srr", run));
                        } else {
                            unresolved.push(UnresolvedId::new("err", run, "not found"));
                        }
                    }
                    hydrated.ena_projects.push(item);
                }
                Err(err) => {
                    unresolved.push(UnresolvedId::new(
                        "ena_project",
                        project,
                        &format!("hydration failed: {}", err),
                    ));
                }
            }
        }

        progress("doi.done");
        Ok(DoiResolution {
            doi: doi.as_str().to_string(),
            source,
            extracted,
            validation,
            hydrated,
            resolved_targets: resolved_targets.into_iter().collect(),
            unresolved,
        })
    }
    fn fetch_crossref(&self, doi: &Doi) -> Result<CrossrefMessage, KiraError> {
        let url = format!(
            "{}/works/{}",
            CROSSREF_BASE,
            encode_url_component(doi.as_str())
        );
        let response = self
            .client
            .get(&url)
            .send()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let message = response
                .text()
                .unwrap_or_else(|_| "Crossref request failed".to_string());
            return Err(KiraError::CrossrefStatus { status, message });
        }
        let payload: CrossrefResponse = response
            .json()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        Ok(payload.message)
    }

    fn validate_pdb(&self, id: &str) -> Result<bool, KiraError> {
        let url = format!("{}/{}", RCSB_BASE, id);
        let response = self
            .client
            .get(&url)
            .send()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        Ok(response.status().is_success())
    }

    fn validate_uniprot(&self, id: &str) -> Result<bool, KiraError> {
        let url = format!("{}/{}.json", UNIPROT_BASE, id);
        let response = self
            .client
            .get(&url)
            .send()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        Ok(response.status().is_success())
    }

    fn validate_assembly(&self, acc: &str) -> Result<bool, KiraError> {
        let url = format!(
            "{}/genome/accession/{}/dataset_report",
            NCBI_DATASETS_BASE, acc
        );
        let response = self
            .client
            .get(&url)
            .send()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        Ok(response.status().is_success())
    }

    fn validate_sra_run(&self, acc: &str) -> Result<bool, KiraError> {
        let ids = self.esearch_ids("sra", &format!("{acc}[Accession]"))?;
        Ok(!ids.is_empty())
    }

    fn validate_geo(&self, acc: &str) -> Result<bool, KiraError> {
        let ids = self.esearch_ids("gds", &format!("{acc}[Accession]"))?;
        Ok(!ids.is_empty())
    }

    fn hydrate_geo_series(&self, gse: &str) -> Result<HydratedGeo, KiraError> {
        let text = self.fetch_geo_text(gse)?;
        let gsm = extract_matches(&text, &Regex::new(r"GSM\d+").unwrap());
        let mut runs = BTreeSet::new();
        for sample in &gsm {
            for run in self.hydrate_geo_sample(sample)? {
                runs.insert(run);
            }
        }

        if runs.is_empty() {
            if let Ok(runs_from_links) = self.sra_runs_from_gds(gse) {
                for run in runs_from_links {
                    runs.insert(run);
                }
            }
        }

        Ok(HydratedGeo {
            gse: gse.to_string(),
            gsm,
            srr: runs.into_iter().collect(),
        })
    }

    fn hydrate_geo_sample(&self, gsm: &str) -> Result<Vec<String>, KiraError> {
        let text = self.fetch_geo_text(gsm)?;
        let mut runs = extract_matches(&text, &Regex::new(r"(SRR\d+|ERR\d+)").unwrap());
        let srx = extract_matches(&text, &Regex::new(r"SRX\d+").unwrap());
        for accession in srx {
            for run in self.sra_runs_from_srx(&accession)? {
                runs.push(run);
            }
        }
        runs.sort();
        runs.dedup();
        Ok(runs)
    }

    fn sra_runs_from_gds(&self, gse: &str) -> Result<Vec<String>, KiraError> {
        let ids = self.esearch_ids("gds", &format!("{gse}[Accession]"))?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let sra_ids = self.elink_ids("gds", "sra", &ids)?;
        self.esummary_sra_runs(&sra_ids)
    }

    fn sra_runs_from_srx(&self, srx: &str) -> Result<Vec<String>, KiraError> {
        let ids = self.esearch_ids("sra", &format!("{srx}[Accession]"))?;
        self.esummary_sra_runs(&ids)
    }

    fn hydrate_bioproject(
        &self,
        acc: &str,
        ids: &[String],
    ) -> Result<HydratedBioProject, KiraError> {
        let sra_ids = self.elink_ids("bioproject", "sra", ids)?;
        let assembly_ids = self.elink_ids("bioproject", "assembly", ids)?;
        let srr = self.esummary_sra_runs(&sra_ids)?;
        let assemblies = self.esummary_assembly_accessions(&assembly_ids)?;
        Ok(HydratedBioProject {
            bioproject: acc.to_string(),
            srr,
            assemblies,
        })
    }

    fn hydrate_ena_project(&self, acc: &str) -> Result<HydratedEnaProject, KiraError> {
        let response = self
            .client
            .get(&build_query_url(
                &format!("{ENA_PORTAL_BASE}/filereport"),
                &[
                    ("accession", acc),
                    ("result", "read_run"),
                    ("fields", "run_accession"),
                    ("format", "tsv"),
                ],
            ))
            .send()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        if !response.status().is_success() {
            return Err(KiraError::CrossrefHttp(format!(
                "ENA portal returned status {}",
                response.status().as_u16()
            )));
        }
        let text = response
            .text()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        let mut runs = Vec::new();
        for line in text.lines().skip(1) {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            runs.push(trimmed.to_string());
        }
        runs.sort();
        runs.dedup();
        Ok(HydratedEnaProject {
            ena_project: acc.to_string(),
            runs,
        })
    }

    fn fetch_geo_text(&self, acc: &str) -> Result<String, KiraError> {
        let response = self
            .client
            .get(&build_query_url(
                GEO_TEXT_BASE,
                &[
                    ("acc", acc),
                    ("targ", "self"),
                    ("form", "text"),
                    ("view", "quick"),
                ],
            ))
            .send()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        if !response.status().is_success() {
            return Err(KiraError::CrossrefHttp(format!(
                "GEO returned status {}",
                response.status().as_u16()
            )));
        }
        response
            .text()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))
    }

    fn esearch_ids(&self, db: &str, term: &str) -> Result<Vec<String>, KiraError> {
        let response = self
            .client
            .get(&build_query_url(
                &format!("{EUTILS_BASE}/esearch.fcgi"),
                &[("db", db), ("term", term), ("retmode", "json")],
            ))
            .send()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        if !response.status().is_success() {
            return Ok(Vec::new());
        }
        let payload: Value = response
            .json()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        let ids = payload["esearchresult"]["idlist"]
            .as_array()
            .map(|list| {
                list.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        Ok(ids)
    }

    fn elink_ids(&self, dbfrom: &str, db: &str, ids: &[String]) -> Result<Vec<String>, KiraError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let id_list = ids.join(",");
        let response = self
            .client
            .get(&build_query_url(
                &format!("{EUTILS_BASE}/elink.fcgi"),
                &[
                    ("dbfrom", dbfrom),
                    ("db", db),
                    ("id", id_list.as_str()),
                    ("retmode", "json"),
                ],
            ))
            .send()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        if !response.status().is_success() {
            return Ok(Vec::new());
        }
        let payload: Value = response
            .json()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        let mut output = Vec::new();
        if let Some(linksets) = payload["linksets"].as_array() {
            for linkset in linksets {
                if let Some(dbs) = linkset["linksetdbs"].as_array() {
                    for db in dbs {
                        if let Some(links) = db["links"].as_array() {
                            for link in links {
                                if let Some(value) = link.as_str() {
                                    output.push(value.to_string());
                                } else if let Some(num) = link.as_u64() {
                                    output.push(num.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
        output.sort();
        output.dedup();
        Ok(output)
    }

    fn esummary_sra_runs(&self, ids: &[String]) -> Result<Vec<String>, KiraError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let id_list = ids.join(",");
        let response = self
            .client
            .get(&build_query_url(
                &format!("{EUTILS_BASE}/esummary.fcgi"),
                &[("db", "sra"), ("id", id_list.as_str()), ("retmode", "json")],
            ))
            .send()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        if !response.status().is_success() {
            return Ok(Vec::new());
        }
        let payload: Value = response
            .json()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        let mut runs = Vec::new();
        let run_re = Regex::new(r#"acc=\"(SRR\d+|ERR\d+)\""#).unwrap();
        if let Some(uids) = payload["result"]["uids"].as_array() {
            for uid in uids {
                if let Some(uid) = uid.as_str() {
                    if let Some(runs_xml) = payload["result"][uid]["runs"].as_str() {
                        for cap in run_re.captures_iter(runs_xml) {
                            if let Some(m) = cap.get(1) {
                                runs.push(m.as_str().to_string());
                            }
                        }
                    }
                }
            }
        }
        runs.sort();
        runs.dedup();
        Ok(runs)
    }

    fn esummary_assembly_accessions(&self, ids: &[String]) -> Result<Vec<String>, KiraError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let id_list = ids.join(",");
        let response = self
            .client
            .get(&build_query_url(
                &format!("{EUTILS_BASE}/esummary.fcgi"),
                &[
                    ("db", "assembly"),
                    ("id", id_list.as_str()),
                    ("retmode", "json"),
                ],
            ))
            .send()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        if !response.status().is_success() {
            return Ok(Vec::new());
        }
        let payload: Value = response
            .json()
            .map_err(|err| KiraError::CrossrefHttp(err.to_string()))?;
        let mut accs = Vec::new();
        if let Some(uids) = payload["result"]["uids"].as_array() {
            for uid in uids {
                if let Some(uid) = uid.as_str() {
                    if let Some(acc) = payload["result"][uid]["assemblyaccession"].as_str() {
                        accs.push(acc.to_string());
                    }
                }
            }
        }
        accs.sort();
        accs.dedup();
        Ok(accs)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoiResolution {
    pub doi: String,
    pub source: DoiSourceMetadata,
    pub extracted: ExtractedIds,
    pub validation: ValidationSummary,
    pub hydrated: HydratedSummary,
    pub resolved_targets: Vec<ResolvedTarget>,
    pub unresolved: Vec<UnresolvedId>,
}

impl DoiResolution {
    pub fn resolved_specifiers(&self) -> Result<Vec<DatasetSpecifier>, KiraError> {
        let mut output = Vec::new();
        for target in &self.resolved_targets {
            let spec = format!("{}:{}", target.dataset_type, target.id);
            output.push(spec.parse()?);
        }
        Ok(output)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DoiSourceMetadata {
    pub title: Option<String>,
    pub abstract_text: Option<String>,
    pub references: Vec<String>,
    pub links: Vec<String>,
    pub data_availability: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExtractedIds {
    pub geo_series: Vec<String>,
    pub geo_samples: Vec<String>,
    pub sra_runs: Vec<String>,
    pub ena_runs: Vec<String>,
    pub bioprojects: Vec<String>,
    pub ena_projects: Vec<String>,
    pub assemblies: Vec<String>,
    pub pdb: Vec<String>,
    pub uniprot: Vec<String>,
}

impl ExtractedIds {
    pub fn is_empty(&self) -> bool {
        self.geo_series.is_empty()
            && self.geo_samples.is_empty()
            && self.sra_runs.is_empty()
            && self.ena_runs.is_empty()
            && self.bioprojects.is_empty()
            && self.ena_projects.is_empty()
            && self.assemblies.is_empty()
            && self.pdb.is_empty()
            && self.uniprot.is_empty()
    }

    pub fn counts(&self) -> BTreeMap<String, usize> {
        let mut map = BTreeMap::new();
        map.insert("gse".to_string(), self.geo_series.len());
        map.insert("gsm".to_string(), self.geo_samples.len());
        map.insert("srr".to_string(), self.sra_runs.len());
        map.insert("err".to_string(), self.ena_runs.len());
        map.insert("bioproject".to_string(), self.bioprojects.len());
        map.insert("ena_project".to_string(), self.ena_projects.len());
        map.insert("assembly".to_string(), self.assemblies.len());
        map.insert("pdb".to_string(), self.pdb.len());
        map.insert("uniprot".to_string(), self.uniprot.len());
        map
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidationSummary {
    pub geo_series: Vec<IdStatus>,
    pub geo_samples: Vec<IdStatus>,
    pub sra_runs: Vec<IdStatus>,
    pub ena_runs: Vec<IdStatus>,
    pub bioprojects: Vec<IdStatus>,
    pub ena_projects: Vec<IdStatus>,
    pub assemblies: Vec<IdStatus>,
    pub pdb: Vec<IdStatus>,
    pub uniprot: Vec<IdStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HydratedSummary {
    pub geo: Vec<HydratedGeo>,
    pub bioprojects: Vec<HydratedBioProject>,
    pub ena_projects: Vec<HydratedEnaProject>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdStatus {
    pub id_type: String,
    pub id: String,
    pub exists: bool,
    pub source: Option<String>,
}

impl IdStatus {
    fn new(id_type: &str, id: &str, exists: bool, source: Option<String>) -> Self {
        Self {
            id_type: id_type.to_string(),
            id: id.to_string(),
            exists,
            source,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydratedGeo {
    pub gse: String,
    pub gsm: Vec<String>,
    pub srr: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydratedBioProject {
    pub bioproject: String,
    pub srr: Vec<String>,
    pub assemblies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydratedEnaProject {
    pub ena_project: String,
    pub runs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ResolvedTarget {
    pub dataset_type: String,
    pub id: String,
}

impl ResolvedTarget {
    fn new(dataset_type: &str, id: &str) -> Self {
        Self {
            dataset_type: dataset_type.to_string(),
            id: id.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnresolvedId {
    pub id_type: String,
    pub id: String,
    pub reason: String,
}

impl UnresolvedId {
    fn new(id_type: &str, id: &str, reason: &str) -> Self {
        Self {
            id_type: id_type.to_string(),
            id: id.to_string(),
            reason: reason.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct CrossrefResponse {
    message: CrossrefMessage,
}

#[derive(Debug, Deserialize)]
struct CrossrefMessage {
    title: Option<Vec<String>>,
    #[serde(rename = "abstract")]
    abstract_text: Option<String>,
    reference: Option<Vec<CrossrefReference>>,
    link: Option<Vec<CrossrefLink>>,
    resource: Option<CrossrefResource>,
    assertion: Option<Vec<CrossrefAssertion>>,
}

#[derive(Debug, Deserialize)]
struct CrossrefReference {
    #[serde(rename = "DOI")]
    doi: Option<String>,
    unstructured: Option<String>,
    #[serde(rename = "article-title")]
    article_title: Option<String>,
    #[serde(rename = "series-title")]
    series_title: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CrossrefLink {
    #[serde(rename = "URL")]
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CrossrefResource {
    primary: Option<CrossrefResourcePrimary>,
}

#[derive(Debug, Deserialize)]
struct CrossrefResourcePrimary {
    #[serde(rename = "URL")]
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CrossrefAssertion {
    label: Option<String>,
    name: Option<String>,
    value: Option<String>,
}

fn collect_source(message: &CrossrefMessage) -> (DoiSourceMetadata, Vec<String>) {
    let title = message
        .title
        .as_ref()
        .and_then(|titles| titles.first().cloned());
    let abstract_text = message.abstract_text.clone();

    let mut references = Vec::new();
    if let Some(items) = &message.reference {
        for item in items {
            if let Some(doi) = &item.doi {
                references.push(doi.clone());
            }
            if let Some(text) = &item.unstructured {
                references.push(text.clone());
            }
            if let Some(text) = &item.article_title {
                references.push(text.clone());
            }
            if let Some(text) = &item.series_title {
                references.push(text.clone());
            }
        }
    }

    let mut links = Vec::new();
    if let Some(items) = &message.link {
        for item in items {
            if let Some(url) = &item.url {
                links.push(url.clone());
            }
        }
    }
    if let Some(resource) = &message.resource {
        if let Some(primary) = &resource.primary {
            if let Some(url) = &primary.url {
                links.push(url.clone());
            }
        }
    }

    let mut data_availability = Vec::new();
    if let Some(assertions) = &message.assertion {
        for assertion in assertions {
            let label = assertion
                .label
                .as_ref()
                .or_else(|| assertion.name.as_ref())
                .map(|value| value.to_lowercase());
            if let Some(label) = label {
                if label.contains("data availability") || label.contains("data") {
                    if let Some(value) = &assertion.value {
                        data_availability.push(value.clone());
                    }
                }
            }
        }
    }

    let mut texts = Vec::new();
    if let Some(value) = &title {
        texts.push(value.clone());
    }
    if let Some(value) = &abstract_text {
        texts.push(value.clone());
    }
    texts.extend(references.clone());
    texts.extend(links.clone());
    texts.extend(data_availability.clone());

    (
        DoiSourceMetadata {
            title,
            abstract_text,
            references,
            links,
            data_availability,
        },
        texts,
    )
}

pub fn extract_ids(texts: &[String]) -> ExtractedIds {
    let re_gse = Regex::new(r"\bGSE\d+\b").unwrap();
    let re_gsm = Regex::new(r"\bGSM\d+\b").unwrap();
    let re_srr = Regex::new(r"\bSRR\d+\b").unwrap();
    let re_err = Regex::new(r"\bERR\d+\b").unwrap();
    let re_bioproject = Regex::new(r"\bPRJ[EN]A\d+\b").unwrap();
    let re_ena_project = Regex::new(r"\bERP\d+\b").unwrap();
    let re_gca = Regex::new(r"\bGCA_\d+\.\d+\b").unwrap();
    let re_gcf = Regex::new(r"\bGCF_\d+\.\d+\b").unwrap();
    let re_pdb = Regex::new(r"\b[0-9][A-Z0-9]{3}\b").unwrap();
    let re_uniprot = Regex::new(r"\b[OPQ][0-9][A-Z0-9]{3}[0-9]\b").unwrap();

    let mut geo_series = BTreeSet::new();
    let mut geo_samples = BTreeSet::new();
    let mut sra_runs = BTreeSet::new();
    let mut ena_runs = BTreeSet::new();
    let mut bioprojects = BTreeSet::new();
    let mut ena_projects = BTreeSet::new();
    let mut assemblies = BTreeSet::new();
    let mut pdb = BTreeSet::new();
    let mut uniprot = BTreeSet::new();

    for text in texts {
        let upper = text.to_uppercase();
        for value in re_gse.find_iter(&upper) {
            geo_series.insert(value.as_str().to_string());
        }
        for value in re_gsm.find_iter(&upper) {
            geo_samples.insert(value.as_str().to_string());
        }
        for value in re_srr.find_iter(&upper) {
            sra_runs.insert(value.as_str().to_string());
        }
        for value in re_err.find_iter(&upper) {
            ena_runs.insert(value.as_str().to_string());
        }
        for value in re_bioproject.find_iter(&upper) {
            bioprojects.insert(value.as_str().to_string());
        }
        for value in re_ena_project.find_iter(&upper) {
            ena_projects.insert(value.as_str().to_string());
        }
        for value in re_gca.find_iter(&upper) {
            assemblies.insert(value.as_str().to_string());
        }
        for value in re_gcf.find_iter(&upper) {
            assemblies.insert(value.as_str().to_string());
        }
        for value in re_pdb.find_iter(&upper) {
            pdb.insert(value.as_str().to_string());
        }
        for value in re_uniprot.find_iter(&upper) {
            uniprot.insert(value.as_str().to_string());
        }
    }

    ExtractedIds {
        geo_series: geo_series.into_iter().collect(),
        geo_samples: geo_samples.into_iter().collect(),
        sra_runs: sra_runs.into_iter().collect(),
        ena_runs: ena_runs.into_iter().collect(),
        bioprojects: bioprojects.into_iter().collect(),
        ena_projects: ena_projects.into_iter().collect(),
        assemblies: assemblies.into_iter().collect(),
        pdb: pdb.into_iter().collect(),
        uniprot: uniprot.into_iter().collect(),
    }
}

fn extract_matches(text: &str, regex: &Regex) -> Vec<String> {
    let mut output = Vec::new();
    for value in regex.find_iter(text) {
        output.push(value.as_str().to_string());
    }
    output.sort();
    output.dedup();
    output
}

fn encode_url_component(value: &str) -> String {
    let mut out = String::new();
    for byte in value.as_bytes() {
        let ch = *byte as char;
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' || ch == '~' {
            out.push(ch);
        } else {
            out.push_str(&format!("%{:02X}", byte));
        }
    }
    out
}

fn build_query_url(base: &str, params: &[(&str, &str)]) -> String {
    if params.is_empty() {
        return base.to_string();
    }
    let mut out = String::from(base);
    out.push('?');
    for (idx, (key, value)) in params.iter().enumerate() {
        if idx > 0 {
            out.push('&');
        }
        out.push_str(&encode_url_component(key));
        out.push('=');
        out.push_str(&encode_url_component(value));
    }
    out
}
