use kira_biodata_manager::domain::Doi;
use kira_biodata_manager::providers::doi::DoiResolver;

#[test]
#[ignore]
fn resolve_real_doi_with_geo_or_sra() {
    let resolver = DoiResolver::new().unwrap();
    let doi: Doi = "10.1038/s41586-020-2649-2".parse().unwrap();
    let result = resolver.resolve(&doi).unwrap();

    let geo_found =
        !result.extracted.geo_series.is_empty() || !result.extracted.geo_samples.is_empty();
    let sra_found = !result.extracted.sra_runs.is_empty() || !result.extracted.ena_runs.is_empty();

    assert!(geo_found || sra_found);
}
