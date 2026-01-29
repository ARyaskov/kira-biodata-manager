use std::fs::File;
use std::path::Path;
use std::thread;
use std::time::Duration;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use serde::Serialize;
use serde_json::Value;

use crate::domain::{ProteinFormat, ProteinId};
use crate::error::KiraError;

#[derive(Debug, Clone, Serialize)]
pub struct RcsbMetadata {
    pub registry: String,
    pub pdb_id: String,
    pub title: Option<String>,
    pub experimental_method: Option<String>,
    pub resolution: Option<f64>,
    pub deposition_date: Option<String>,
    pub release_date: Option<String>,
    pub source_structure_url: String,
    pub source_metadata_url: String,
    pub raw_json: Value,
}

pub trait RcsbClient: Send + Sync {
    fn download_structure(
        &self,
        id: &ProteinId,
        format: ProteinFormat,
        destination: &Path,
    ) -> Result<(), KiraError>;
    fn fetch_metadata(&self, id: &ProteinId) -> Result<RcsbMetadata, KiraError>;
}

#[derive(Clone)]
pub struct RcsbHttpClient {
    client: Client,
}

impl RcsbHttpClient {
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
            .map_err(|err| KiraError::RcsbHttp(err.to_string()))?;
        Ok(Self { client })
    }

    pub fn structure_url(id: &ProteinId, format: ProteinFormat) -> String {
        let ext = match format {
            ProteinFormat::Cif => "cif",
            ProteinFormat::Pdb => "pdb",
            ProteinFormat::Bcif => "bcif",
        };
        format!("https://files.rcsb.org/download/{}.{}", id.as_str(), ext)
    }

    fn metadata_url(id: &ProteinId) -> String {
        format!("https://data.rcsb.org/rest/v1/core/entry/{}", id.as_str())
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
            .unwrap_or_else(|_| "RCSB request failed".to_string());
        Err(KiraError::RcsbStatus { status, message })
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
                        thread::sleep(Duration::from_millis(delay));
                        attempt += 1;
                        continue;
                    }
                    return Ok(resp);
                }
                Err(err) => {
                    if attempt < MAX_RETRIES && is_retryable_error(&err) {
                        let delay = BASE_DELAY_MS * (attempt as u64 + 1);
                        thread::sleep(Duration::from_millis(delay));
                        attempt += 1;
                        continue;
                    }
                    return Err(KiraError::RcsbHttp(err.to_string()));
                }
            }
        }
    }
}

impl RcsbClient for RcsbHttpClient {
    fn download_structure(
        &self,
        id: &ProteinId,
        format: ProteinFormat,
        destination: &Path,
    ) -> Result<(), KiraError> {
        let url = Self::structure_url(id, format);
        let response = self.send_with_retries(|| self.client.get(&url))?;
        let mut response = Self::handle_status(response)?;
        let mut file =
            File::create(destination).map_err(|err| KiraError::Filesystem(err.to_string()))?;
        std::io::copy(&mut response, &mut file)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        Ok(())
    }

    fn fetch_metadata(&self, id: &ProteinId) -> Result<RcsbMetadata, KiraError> {
        let url = Self::metadata_url(id);
        let response = self.send_with_retries(|| self.client.get(&url))?;
        let response = Self::handle_status(response)?;
        let raw_json: Value = response
            .json()
            .map_err(|err| KiraError::RcsbHttp(err.to_string()))?;

        let title = raw_json
            .get("struct")
            .and_then(|value| value.get("title"))
            .and_then(|value| value.as_str())
            .map(|value| value.to_string());
        let experimental_method = raw_json
            .get("exptl")
            .and_then(|value| value.as_array())
            .and_then(|array| array.first())
            .and_then(|value| value.get("method"))
            .and_then(|value| value.as_str())
            .map(|value| value.to_string());
        let resolution = raw_json
            .get("rcsb_entry_info")
            .and_then(|value| value.get("resolution_combined"))
            .and_then(|value| value.as_array())
            .and_then(|array| array.first())
            .and_then(|value| value.as_f64());
        let deposition_date = raw_json
            .get("rcsb_accession_info")
            .and_then(|value| value.get("deposit_date"))
            .and_then(|value| value.as_str())
            .map(|value| value.to_string());
        let release_date = raw_json
            .get("rcsb_accession_info")
            .and_then(|value| value.get("initial_release_date"))
            .and_then(|value| value.as_str())
            .map(|value| value.to_string());

        Ok(RcsbMetadata {
            registry: "rcsb".to_string(),
            pdb_id: id.as_str().to_string(),
            title,
            experimental_method,
            resolution,
            deposition_date,
            release_date,
            source_structure_url: Self::structure_url(id, ProteinFormat::Cif),
            source_metadata_url: url,
            raw_json,
        })
    }
}

fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

fn is_retryable_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request()
}
