use assert_matches::assert_matches;

use kira_biodata_manager::domain::{
    DatasetSpecifier, Doi, GenomeAccession, GeoSeriesAccession, ProteinFormat, ProteinId, Registry,
    SrrId, UniprotId,
};
use kira_biodata_manager::error::KiraError;

#[test]
fn parse_protein_id_valid() {
    let id: ProteinId = "1lyz".parse().unwrap();
    assert_eq!(id.as_str(), "1LYZ");
}

#[test]
fn parse_protein_id_invalid() {
    let err = "XYZ".parse::<ProteinId>().unwrap_err();
    assert_matches!(err, KiraError::InvalidProteinId(_));
}

#[test]
fn parse_genome_accession_valid() {
    let acc: GenomeAccession = "GCF_000005845.2".parse().unwrap();
    assert_eq!(acc.as_str(), "GCF_000005845.2");
}

#[test]
fn parse_genome_accession_invalid() {
    let err = "ABC_123".parse::<GenomeAccession>().unwrap_err();
    assert_matches!(err, KiraError::InvalidGenomeAccession(_));
}

#[test]
fn parse_dataset_specifier() {
    let spec: DatasetSpecifier = "protein:1LYZ".parse().unwrap();
    assert_matches!(spec, DatasetSpecifier::Protein(_));
}

#[test]
fn registry_routing() {
    let protein: DatasetSpecifier = "protein:1LYZ".parse().unwrap();
    assert_eq!(protein.resolve_registry(None), Registry::Rcsb);
    assert_eq!(
        protein.resolve_registry(Some(ProteinFormat::Bcif)),
        Registry::Rcsb
    );

    let genome: DatasetSpecifier = "genome:GCF_000005845.2".parse().unwrap();
    assert_eq!(genome.resolve_registry(None), Registry::Ncbi);

    let srr: DatasetSpecifier = "srr:SRR014966".parse().unwrap();
    assert_eq!(srr.resolve_registry(None), Registry::Ncbi);

    let uniprot: DatasetSpecifier = "uniprot:P69905".parse().unwrap();
    assert_eq!(uniprot.resolve_registry(None), Registry::Uniprot);

    let doi: DatasetSpecifier = "doi:10.1038/s41586-020-2649-2".parse().unwrap();
    assert_eq!(doi.resolve_registry(None), Registry::Doi);

    let expression: DatasetSpecifier = "expression:GSE102902".parse().unwrap();
    assert_eq!(expression.resolve_registry(None), Registry::Geo);

    let expression10x: DatasetSpecifier = "expression10x:GSE115978".parse().unwrap();
    assert_eq!(expression10x.resolve_registry(None), Registry::Geo);

    let go: DatasetSpecifier = "go".parse().unwrap();
    assert_eq!(go.resolve_registry(None), Registry::Go);

    let kegg: DatasetSpecifier = "kegg".parse().unwrap();
    assert_eq!(kegg.resolve_registry(None), Registry::Kegg);

    let reactome: DatasetSpecifier = "reactome".parse().unwrap();
    assert_eq!(reactome.resolve_registry(None), Registry::Reactome);
}

#[test]
fn parse_srr_id_valid() {
    let id: SrrId = "srr014966".parse().unwrap();
    assert_eq!(id.as_str(), "SRR014966");
}

#[test]
fn parse_err_id_valid() {
    let id: SrrId = "err123456".parse().unwrap();
    assert_eq!(id.as_str(), "ERR123456");
}

#[test]
fn parse_uniprot_id_valid() {
    let id: UniprotId = "p69905".parse().unwrap();
    assert_eq!(id.as_str(), "P69905");
}

#[test]
fn parse_doi_valid() {
    let doi: Doi = "10.1038/s41586-020-2649-2".parse().unwrap();
    assert_eq!(doi.as_str(), "10.1038/s41586-020-2649-2");
}

#[test]
fn parse_expression_valid() {
    let acc: GeoSeriesAccession = "GSE102902".parse().unwrap();
    assert_eq!(acc.as_str(), "GSE102902");
}
