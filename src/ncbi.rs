use std::fs::File;
use std::path::Path;
use std::thread;
use std::time::Duration;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};

use crate::domain::{GenomeAccession, ProteinFormat, ProteinId};
use crate::error::KiraError;

#[derive(Debug, Clone, Copy)]
pub struct DownloadInfo {
    pub is_zip: bool,
}

pub trait NcbiClient: Send + Sync {
    fn download_protein(
        &self,
        id: &ProteinId,
        format: ProteinFormat,
        destination: &Path,
    ) -> Result<DownloadInfo, KiraError>;
    fn download_genome(
        &self,
        accession: &GenomeAccession,
        include: &[String],
        destination: &Path,
    ) -> Result<DownloadInfo, KiraError>;
}

#[derive(Clone)]
pub struct NcbiHttpClient {
    client: Client,
    base_url: String,
}

impl NcbiHttpClient {
    pub fn new() -> Result<Self, KiraError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&format!("kira-bm/{}", env!("CARGO_PKG_VERSION")))
                .map_err(|err| KiraError::Filesystem(err.to_string()))?,
        );
        headers.insert("X-Datasets-Client", HeaderValue::from_static("kira-bm"));
        headers.insert(
            "X-Datasets-Client-Version",
            HeaderValue::from_str(env!("CARGO_PKG_VERSION"))
                .map_err(|err| KiraError::Filesystem(err.to_string()))?,
        );
        headers.insert(
            "X-Datasets-Client-OS",
            HeaderValue::from_str(std::env::consts::OS)
                .map_err(|err| KiraError::Filesystem(err.to_string()))?,
        );
        headers.insert(
            "X-Datasets-Client-Arch",
            HeaderValue::from_str(std::env::consts::ARCH)
                .map_err(|err| KiraError::Filesystem(err.to_string()))?,
        );

        if let Ok(api_key) = std::env::var("NCBI_API_KEY") {
            if !api_key.trim().is_empty() {
                headers.insert(
                    "api-key",
                    HeaderValue::from_str(api_key.trim())
                        .map_err(|err| KiraError::Filesystem(err.to_string()))?,
                );
            }
        }

        let client = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|err| KiraError::NcbiHttp(err.to_string()))?;

        Ok(Self {
            client,
            base_url: "https://api.ncbi.nlm.nih.gov/datasets/v2".to_string(),
        })
    }

    fn write_response_to_file(
        &self,
        mut response: reqwest::blocking::Response,
        destination: &Path,
    ) -> Result<DownloadInfo, KiraError> {
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let message = response
                .text()
                .unwrap_or_else(|_| "NCBI request failed".to_string());
            return Err(KiraError::NcbiStatus { status, message });
        }
        let is_zip = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.contains("zip"))
            .unwrap_or(false);

        let mut file =
            File::create(destination).map_err(|err| KiraError::Filesystem(err.to_string()))?;
        std::io::copy(&mut response, &mut file)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        Ok(DownloadInfo { is_zip })
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
                    return Err(KiraError::NcbiHttp(err.to_string()));
                }
            }
        }
    }
}

impl NcbiClient for NcbiHttpClient {
    fn download_protein(
        &self,
        _id: &ProteinId,
        _format: ProteinFormat,
        _destination: &Path,
    ) -> Result<DownloadInfo, KiraError> {
        Err(KiraError::UnsupportedProteinFormat(
            "protein structures are routed to RCSB in this release".to_string(),
        ))
    }

    fn download_genome(
        &self,
        accession: &GenomeAccession,
        include: &[String],
        destination: &Path,
    ) -> Result<DownloadInfo, KiraError> {
        let include_params = map_genome_include(include)?;
        let url = format!(
            "{}/genome/accession/{}/download",
            self.base_url,
            accession.as_str()
        );
        let response = self.send_with_retries(|| {
            let mut request = self.client.get(&url);
            for value in &include_params {
                request = request.query(&[("include_annotation_type", value.as_str())]);
            }
            request
        })?;
        self.write_response_to_file(response, destination)
    }
}

fn map_genome_include(include: &[String]) -> Result<Vec<String>, KiraError> {
    if include.is_empty() {
        return Ok(Vec::new());
    }
    let mut mapped = Vec::new();
    for item in include {
        let value = match item.as_str() {
            "genome" => "GENOME_FASTA",
            "gff3" => "GENOME_GFF",
            "gbff" => "GENOME_GBFF",
            "gtf" => "GENOME_GTF",
            "rna" => "RNA_FASTA",
            "protein" => "PROT_FASTA",
            "cds" => "CDS_FASTA",
            "seq-report" => "SEQUENCE_REPORT",
            "default" => "DEFAULT",
            other => {
                return Err(KiraError::InvalidInclude(other.to_string()));
            }
        };
        mapped.push(value.to_string());
    }
    Ok(mapped)
}

fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

fn is_retryable_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_include_values() {
        let include = vec![
            "genome".to_string(),
            "gff3".to_string(),
            "protein".to_string(),
        ];
        let mapped = map_genome_include(&include).unwrap();
        assert_eq!(mapped, vec!["GENOME_FASTA", "GENOME_GFF", "PROT_FASTA"]);
    }
}
