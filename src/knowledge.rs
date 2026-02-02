use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};

use crate::error::KiraError;

pub trait KnowledgeClient: Send + Sync {
    fn download_go(&self, destination: &Path) -> Result<Vec<u8>, KiraError>;
    fn download_kegg_pathways(&self, destination: &Path) -> Result<(), KiraError>;
    fn download_kegg_pathway_links(&self, destination: &Path) -> Result<(), KiraError>;
    fn download_reactome_pathways(&self, destination: &Path) -> Result<(), KiraError>;
    fn download_reactome_mappings(&self, destination: &Path) -> Result<(), KiraError>;
}

#[derive(Clone)]
pub struct KnowledgeHttpClient {
    client: Client,
}

impl KnowledgeHttpClient {
    pub fn new() -> Result<Self, KiraError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&format!("kira-bm/{}", env!("CARGO_PKG_VERSION")))
                .map_err(|err| KiraError::Filesystem(err.to_string()))?,
        );
        let client = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|err| KiraError::KnowledgeHttp(err.to_string()))?;
        Ok(Self { client })
    }

    fn download(&self, url: &str, destination: &Path) -> Result<Vec<u8>, KiraError> {
        let response = self
            .client
            .get(url)
            .send()
            .map_err(|err| KiraError::KnowledgeHttp(err.to_string()))?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let message = response
                .text()
                .unwrap_or_else(|_| "knowledge base request failed".to_string());
            return Err(KiraError::KnowledgeStatus { status, message });
        }
        let bytes = response
            .bytes()
            .map_err(|err| KiraError::KnowledgeHttp(err.to_string()))?;
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        }
        let mut file =
            File::create(destination).map_err(|err| KiraError::Filesystem(err.to_string()))?;
        file.write_all(&bytes)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        Ok(bytes.to_vec())
    }
}

impl KnowledgeClient for KnowledgeHttpClient {
    fn download_go(&self, destination: &Path) -> Result<Vec<u8>, KiraError> {
        self.download(
            "http://purl.obolibrary.org/obo/go/go-basic.obo",
            destination,
        )
    }

    fn download_kegg_pathways(&self, destination: &Path) -> Result<(), KiraError> {
        let _ = self.download("https://rest.kegg.jp/list/pathway", destination)?;
        Ok(())
    }

    fn download_kegg_pathway_links(&self, destination: &Path) -> Result<(), KiraError> {
        let _ = self.download("https://rest.kegg.jp/link/pathway/ko", destination)?;
        Ok(())
    }

    fn download_reactome_pathways(&self, destination: &Path) -> Result<(), KiraError> {
        let _ = self.download(
            "https://reactome.org/download/current/ReactomePathways.txt",
            destination,
        )?;
        Ok(())
    }

    fn download_reactome_mappings(&self, destination: &Path) -> Result<(), KiraError> {
        let _ = self.download(
            "https://reactome.org/download/current/UniProt2Reactome.txt",
            destination,
        )?;
        Ok(())
    }
}

pub fn parse_go_header(content: &[u8]) -> (Option<String>, Option<String>) {
    let mut version = None;
    let mut date = None;
    let text = String::from_utf8_lossy(content);
    for line in text.lines().take(50) {
        if let Some(value) = line.strip_prefix("data-version:") {
            version = Some(value.trim().to_string());
        }
        if let Some(value) = line.strip_prefix("date:") {
            date = Some(value.trim().to_string());
        }
    }
    (version, date)
}
