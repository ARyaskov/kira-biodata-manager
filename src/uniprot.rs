use std::time::Duration;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use serde::Serialize;
use serde_json::Value;

use crate::domain::UniprotId;
use crate::error::KiraError;

#[derive(Debug, Clone)]
pub struct UniprotRecord {
    pub raw_json: Value,
    pub fasta: String,
    pub metadata: UniprotMetadata,
}

#[derive(Debug, Clone, Serialize)]
pub struct UniprotMetadata {
    pub registry: String,
    pub accession: String,
    pub protein_name: Option<String>,
    pub gene_names: Vec<String>,
    pub organism: Option<String>,
    pub sequence_length: Option<u64>,
    pub canonical_isoform: bool,
    pub isoforms: Vec<String>,
    pub features: UniprotFeatures,
    pub functions: Vec<String>,
    pub diseases: Vec<String>,
    pub cross_references: UniprotCrossRefs,
    pub downloaded_at: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct UniprotFeatures {
    pub domains: Vec<FeatureItem>,
    pub active_sites: Vec<FeatureItem>,
    pub binding_sites: Vec<FeatureItem>,
    pub ptm: Vec<FeatureItem>,
    pub variants: Vec<FeatureItem>,
    pub regions: Vec<FeatureItem>,
    pub repeats: Vec<FeatureItem>,
    pub motifs: Vec<FeatureItem>,
    pub signal_peptides: Vec<FeatureItem>,
    pub transmembrane: Vec<FeatureItem>,
    pub topological_domains: Vec<FeatureItem>,
    pub helices: Vec<FeatureItem>,
    pub coiled_coils: Vec<FeatureItem>,
    pub zinc_fingers: Vec<FeatureItem>,
    pub turns: Vec<FeatureItem>,
    pub strands: Vec<FeatureItem>,
    pub beta_strands: Vec<FeatureItem>,
    pub disordered_regions: Vec<FeatureItem>,
    pub low_complexity_regions: Vec<FeatureItem>,
    pub signal_anchors: Vec<FeatureItem>,
    pub transit_peptides: Vec<FeatureItem>,
    pub beta_helices: Vec<FeatureItem>,
    pub propeptides: Vec<FeatureItem>,
    pub initiator_methionines: Vec<FeatureItem>,
    pub chains: Vec<FeatureItem>,
    pub peptides: Vec<FeatureItem>,
    pub mature_chains: Vec<FeatureItem>,
    pub propeptide_chains: Vec<FeatureItem>,
    pub mature_peptides: Vec<FeatureItem>,
    pub propeptide_peptides: Vec<FeatureItem>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeatureItem {
    pub name: String,
    pub start: Option<u64>,
    pub end: Option<u64>,
    pub description: Option<String>,
    pub qualifier: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct UniprotCrossRefs {
    pub pdb: Vec<String>,
    pub ncbi: Vec<String>,
}

pub trait UniprotClient: Send + Sync {
    fn fetch(&self, id: &UniprotId) -> Result<UniprotRecord, KiraError>;
}

#[derive(Clone)]
pub struct UniprotHttpClient {
    client: Client,
}

impl UniprotHttpClient {
    pub fn new() -> Result<Self, KiraError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&format!("kira-bm/{}", env!("CARGO_PKG_VERSION")))
                .map_err(|err| KiraError::Filesystem(err.to_string()))?,
        );
        let client = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|err| KiraError::UniprotHttp(err.to_string()))?;
        Ok(Self { client })
    }

    fn send_with_retries<F>(
        &self,
        mut make_req: F,
    ) -> Result<reqwest::blocking::Response, KiraError>
    where
        F: FnMut() -> reqwest::blocking::RequestBuilder,
    {
        const MAX_RETRIES: usize = 3;
        const BASE_DELAY_MS: u64 = 200;
        let mut attempt = 0usize;
        loop {
            let response = make_req().send();
            match response {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    if attempt < MAX_RETRIES && is_retryable_status(status) {
                        let delay = BASE_DELAY_MS * (attempt as u64 + 1);
                        std::thread::sleep(Duration::from_millis(delay));
                        attempt += 1;
                        continue;
                    }
                    return Ok(resp);
                }
                Err(err) => {
                    if attempt < MAX_RETRIES && is_retryable_error(&err) {
                        let delay = BASE_DELAY_MS * (attempt as u64 + 1);
                        std::thread::sleep(Duration::from_millis(delay));
                        attempt += 1;
                        continue;
                    }
                    return Err(KiraError::UniprotHttp(err.to_string()));
                }
            }
        }
    }

    fn handle_status(
        response: reqwest::blocking::Response,
    ) -> Result<reqwest::blocking::Response, KiraError> {
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status().as_u16();
        let message = response
            .text()
            .unwrap_or_else(|_| "UniProt request failed".to_string());
        Err(KiraError::UniprotStatus { status, message })
    }

    fn metadata_url(id: &UniprotId) -> String {
        format!("https://rest.uniprot.org/uniprotkb/{}.json", id.as_str())
    }

    fn fasta_url(id: &UniprotId) -> String {
        format!("https://rest.uniprot.org/uniprotkb/{}.fasta", id.as_str())
    }
}

impl UniprotClient for UniprotHttpClient {
    fn fetch(&self, id: &UniprotId) -> Result<UniprotRecord, KiraError> {
        let metadata_url = Self::metadata_url(id);
        let fasta_url = Self::fasta_url(id);

        let response = self.send_with_retries(|| self.client.get(&metadata_url))?;
        let response = Self::handle_status(response)?;
        let raw_json: Value = response
            .json()
            .map_err(|err| KiraError::UniprotHttp(err.to_string()))?;

        let response = self.send_with_retries(|| self.client.get(&fasta_url))?;
        let response = Self::handle_status(response)?;
        let fasta = response
            .text()
            .map_err(|err| KiraError::UniprotHttp(err.to_string()))?;

        let metadata = extract_metadata(&raw_json)?;

        Ok(UniprotRecord {
            raw_json,
            fasta,
            metadata,
        })
    }
}

pub fn extract_metadata(raw: &Value) -> Result<UniprotMetadata, KiraError> {
    let accession = raw
        .get("primaryAccession")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let protein_name = raw
        .get("proteinDescription")
        .and_then(|v| v.get("recommendedName"))
        .and_then(|v| v.get("fullName"))
        .and_then(|v| v.get("value"))
        .and_then(|v| v.as_str())
        .map(|v| v.to_string())
        .or_else(|| {
            raw.get("proteinDescription")
                .and_then(|v| v.get("submissionNames"))
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.get("fullName"))
                .and_then(|v| v.get("value"))
                .and_then(|v| v.as_str())
                .map(|v| v.to_string())
        });
    let mut gene_names = Vec::new();
    if let Some(genes) = raw.get("genes").and_then(|v| v.as_array()) {
        for gene in genes {
            if let Some(name) = gene
                .get("geneName")
                .and_then(|v| v.get("value"))
                .and_then(|v| v.as_str())
            {
                gene_names.push(name.to_string());
            }
            if let Some(syns) = gene.get("synonyms").and_then(|v| v.as_array()) {
                for syn in syns {
                    if let Some(name) = syn.get("value").and_then(|v| v.as_str()) {
                        gene_names.push(name.to_string());
                    }
                }
            }
        }
    }
    gene_names.sort();
    gene_names.dedup();

    let organism = raw
        .get("organism")
        .and_then(|v| v.get("scientificName"))
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    let sequence_length = raw
        .get("sequence")
        .and_then(|v| v.get("length"))
        .and_then(|v| v.as_u64());

    let mut isoforms = Vec::new();
    let mut canonical = true;
    if let Some(comments) = raw.get("comments").and_then(|v| v.as_array()) {
        for comment in comments {
            if comment.get("commentType").and_then(|v| v.as_str()) == Some("ALTERNATIVE_PRODUCTS") {
                if let Some(iso) = comment.get("isoforms").and_then(|v| v.as_array()) {
                    for item in iso {
                        if let Some(iso_ids) = item.get("isoformIds").and_then(|v| v.as_array()) {
                            for iso_id in iso_ids {
                                if let Some(id) = iso_id.as_str() {
                                    isoforms.push(id.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if !isoforms.is_empty() {
        canonical = isoforms.iter().any(|id| id.ends_with("-1"));
    }

    let mut features = UniprotFeatures::default();
    if let Some(items) = raw.get("features").and_then(|v| v.as_array()) {
        for item in items {
            let ftype = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
            let description = item
                .get("description")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string());
            let start = item
                .get("location")
                .and_then(|v| v.get("start"))
                .and_then(|v| v.get("value"))
                .and_then(|v| v.as_u64());
            let end = item
                .get("location")
                .and_then(|v| v.get("end"))
                .and_then(|v| v.get("value"))
                .and_then(|v| v.as_u64());
            let entry = FeatureItem {
                name: ftype.to_string(),
                start,
                end,
                description,
                qualifier: item
                    .get("featureCrossReferences")
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|v| v.get("id"))
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string()),
            };
            match ftype {
                "Domain" => features.domains.push(entry),
                "Active site" => features.active_sites.push(entry),
                "Binding site" => features.binding_sites.push(entry),
                "Modified residue"
                | "Glycosylation"
                | "Lipidation"
                | "Disulfide bond"
                | "Cross-link"
                | "Modified residue (PTM)" => features.ptm.push(entry),
                "Natural variant" | "Sequence variant" => features.variants.push(entry),
                "Region" => features.regions.push(entry),
                "Repeat" => features.repeats.push(entry),
                "Motif" => features.motifs.push(entry),
                "Signal peptide" => features.signal_peptides.push(entry),
                "Transmembrane" => features.transmembrane.push(entry),
                "Topological domain" => features.topological_domains.push(entry),
                "Helix" => features.helices.push(entry),
                "Coiled coil" => features.coiled_coils.push(entry),
                "Zinc finger" => features.zinc_fingers.push(entry),
                "Turn" => features.turns.push(entry),
                "Strand" => features.strands.push(entry),
                "Beta strand" => features.beta_strands.push(entry),
                "Intrinsically disordered region" | "Disordered" => {
                    features.disordered_regions.push(entry)
                }
                "Low complexity" => features.low_complexity_regions.push(entry),
                "Signal anchor" => features.signal_anchors.push(entry),
                "Transit peptide" => features.transit_peptides.push(entry),
                "Beta helix" => features.beta_helices.push(entry),
                "Propeptide" => features.propeptides.push(entry),
                "Initiator methionine" => features.initiator_methionines.push(entry),
                "Chain" => {
                    if is_mature_feature(item, &entry) {
                        features.mature_chains.push(entry);
                    } else if is_propeptide_feature(item, &entry) {
                        features.propeptide_chains.push(entry);
                    } else {
                        features.chains.push(entry);
                    }
                }
                "Peptide" => {
                    if is_mature_feature(item, &entry) {
                        features.mature_peptides.push(entry);
                    } else if is_propeptide_feature(item, &entry) {
                        features.propeptide_peptides.push(entry);
                    } else {
                        features.peptides.push(entry);
                    }
                }
                _ => {}
            }
        }
    }

    let mut functions = Vec::new();
    let mut diseases = Vec::new();
    if let Some(comments) = raw.get("comments").and_then(|v| v.as_array()) {
        for comment in comments {
            match comment.get("commentType").and_then(|v| v.as_str()) {
                Some("FUNCTION") => {
                    if let Some(texts) = comment.get("texts").and_then(|v| v.as_array()) {
                        for text in texts {
                            if let Some(value) = text.get("value").and_then(|v| v.as_str()) {
                                functions.push(value.to_string());
                            }
                        }
                    }
                }
                Some("CATALYTIC_ACTIVITY") => {
                    if let Some(reaction) = comment
                        .get("reaction")
                        .and_then(|v| v.get("name"))
                        .and_then(|v| v.as_str())
                    {
                        functions.push(reaction.to_string());
                    }
                }
                Some("DISEASE") => {
                    if let Some(disease) = comment.get("disease") {
                        let name = disease.get("diseaseId").and_then(|v| v.as_str());
                        let desc = disease.get("description").and_then(|v| v.as_str());
                        let value = match (name, desc) {
                            (Some(n), Some(d)) => format!("{n}: {d}"),
                            (Some(n), None) => n.to_string(),
                            (None, Some(d)) => d.to_string(),
                            _ => continue,
                        };
                        diseases.push(value);
                    }
                }
                _ => {}
            }
        }
    }

    let mut cross_refs = UniprotCrossRefs::default();
    if let Some(xrefs) = raw
        .get("uniProtKBCrossReferences")
        .and_then(|v| v.as_array())
    {
        for xref in xrefs {
            let db = xref.get("database").and_then(|v| v.as_str()).unwrap_or("");
            let id = xref.get("id").and_then(|v| v.as_str());
            match (db, id) {
                ("PDB", Some(value)) => cross_refs.pdb.push(value.to_string()),
                ("RefSeq", Some(value)) => cross_refs.ncbi.push(value.to_string()),
                ("GeneID", Some(value)) => cross_refs.ncbi.push(value.to_string()),
                _ => {}
            }
        }
    }

    Ok(UniprotMetadata {
        registry: "uniprot".to_string(),
        accession,
        protein_name,
        gene_names,
        organism,
        sequence_length,
        canonical_isoform: canonical,
        isoforms,
        features,
        functions,
        diseases,
        cross_references: cross_refs,
        downloaded_at: chrono::Utc::now().to_rfc3339(),
    })
}

fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

fn is_retryable_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request()
}

fn is_mature_feature(item: &Value, entry: &FeatureItem) -> bool {
    if has_note_keyword(item, "mature") {
        return true;
    }
    if has_feature_ref_keyword(item, "mature") {
        return true;
    }
    if let Some(desc) = entry.description.as_ref() {
        return desc.to_lowercase().contains("mature");
    }
    false
}

fn is_propeptide_feature(item: &Value, entry: &FeatureItem) -> bool {
    if has_note_keyword(item, "propeptide") {
        return true;
    }
    if has_feature_ref_keyword(item, "propeptide") {
        return true;
    }
    if let Some(desc) = entry.description.as_ref() {
        return desc.to_lowercase().contains("propeptide");
    }
    false
}

fn has_note_keyword(item: &Value, keyword: &str) -> bool {
    if let Some(note) = item.get("note") {
        if let Some(texts) = note.get("texts").and_then(|v| v.as_array()) {
            for text in texts {
                if let Some(value) = text.get("value").and_then(|v| v.as_str()) {
                    if value.to_lowercase().contains(keyword) {
                        return true;
                    }
                }
            }
        }
        if let Some(value) = note.get("value").and_then(|v| v.as_str()) {
            if value.to_lowercase().contains(keyword) {
                return true;
            }
        }
    }
    false
}

fn has_feature_ref_keyword(item: &Value, keyword: &str) -> bool {
    if let Some(refs) = item
        .get("featureCrossReferences")
        .and_then(|v| v.as_array())
    {
        for entry in refs {
            if let Some(id) = entry.get("id").and_then(|v| v.as_str()) {
                if id.to_lowercase().contains(keyword) {
                    return true;
                }
            }
            if let Some(props) = entry.get("properties").and_then(|v| v.as_array()) {
                for prop in props {
                    let value = prop.get("value").and_then(|v| v.as_str());
                    if let Some(value) = value {
                        if value.to_lowercase().contains(keyword) {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}
