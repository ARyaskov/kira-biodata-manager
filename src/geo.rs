use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

use flate2::read::GzDecoder;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};

use crate::domain::GeoSeriesAccession;
use crate::error::KiraError;

pub trait GeoClient: Send + Sync {
    fn fetch_soft_text(&self, accession: &GeoSeriesAccession) -> Result<String, KiraError>;
    fn download_url(&self, url: &str, destination: &Path) -> Result<(), KiraError>;
}

#[derive(Clone)]
pub struct GeoHttpClient {
    client: Client,
}

impl GeoHttpClient {
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
            .map_err(|err| KiraError::GeoHttp(err.to_string()))?;
        Ok(Self { client })
    }

    fn soft_url(accession: &GeoSeriesAccession) -> String {
        let prefix = geo_series_prefix(accession);
        format!(
            "https://ftp.ncbi.nlm.nih.gov/geo/series/{prefix}/{acc}/soft/{acc}_family.soft.gz",
            acc = accession.as_str()
        )
    }

    fn normalize_url(url: &str) -> String {
        if let Some(rest) = url.strip_prefix("ftp://ftp.ncbi.nlm.nih.gov/") {
            return format!("https://ftp.ncbi.nlm.nih.gov/{}", rest);
        }
        url.to_string()
    }

    fn write_response_to_file(
        &self,
        mut response: reqwest::blocking::Response,
        destination: &Path,
    ) -> Result<(), KiraError> {
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let message = response
                .text()
                .unwrap_or_else(|_| "GEO request failed".to_string());
            return Err(KiraError::GeoStatus { status, message });
        }
        let mut file =
            File::create(destination).map_err(|err| KiraError::Filesystem(err.to_string()))?;
        std::io::copy(&mut response, &mut file)
            .map_err(|err| KiraError::Filesystem(err.to_string()))?;
        Ok(())
    }
}

impl GeoClient for GeoHttpClient {
    fn fetch_soft_text(&self, accession: &GeoSeriesAccession) -> Result<String, KiraError> {
        let url = Self::soft_url(accession);
        let response = self
            .client
            .get(url)
            .send()
            .map_err(|err| KiraError::GeoHttp(err.to_string()))?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let message = response
                .text()
                .unwrap_or_else(|_| "GEO request failed".to_string());
            return Err(KiraError::GeoStatus { status, message });
        }
        let bytes = response
            .bytes()
            .map_err(|err| KiraError::GeoHttp(err.to_string()))?;
        let mut decoder = GzDecoder::new(bytes.as_ref());
        let mut text = String::new();
        decoder
            .read_to_string(&mut text)
            .map_err(|err| KiraError::GeoHttp(err.to_string()))?;
        Ok(text)
    }

    fn download_url(&self, url: &str, destination: &Path) -> Result<(), KiraError> {
        let url = Self::normalize_url(url);
        let response = self
            .client
            .get(url)
            .send()
            .map_err(|err| KiraError::GeoHttp(err.to_string()))?;
        self.write_response_to_file(response, destination)
    }
}

pub fn extract_supplementary_urls(soft_text: &str) -> Vec<String> {
    let mut urls = Vec::new();
    for line in soft_text.lines() {
        if !line.contains("supplementary_file") {
            continue;
        }
        if let Some((_, value)) = line.split_once('=') {
            let url = value.trim();
            if !url.is_empty() {
                urls.push(url.to_string());
            }
        }
    }
    urls
}

pub fn extract_organism(soft_text: &str) -> Option<String> {
    for line in soft_text.lines() {
        if line.starts_with("!Series_organism_ch1")
            || line.starts_with("!Series_organism")
            || line.starts_with("!Sample_organism_ch1")
        {
            if let Some((_, value)) = line.split_once('=') {
                let value = value.trim();
                if !value.is_empty() {
                    return Some(value.to_string());
                }
            }
        }
    }
    None
}

pub fn geo_series_prefix(accession: &GeoSeriesAccession) -> String {
    let digits = accession.as_str().trim_start_matches("GSE");
    if digits.len() <= 3 {
        return "GSEnnn".to_string();
    }
    let head = &digits[..digits.len() - 3];
    format!("GSE{}nnn", head)
}
