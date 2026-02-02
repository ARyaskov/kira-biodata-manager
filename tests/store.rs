use kira_biodata_manager::domain::{
    Doi, GenomeAccession, GeoSeriesAccession, ProteinFormat, ProteinId, SrrId, UniprotId,
};
use kira_biodata_manager::store::Store;

#[test]
fn layout_paths() {
    let store = Store::new().unwrap();
    let id: ProteinId = "1LYZ".parse().unwrap();
    let acc: GenomeAccession = "GCF_000005845.2".parse().unwrap();
    let srr: SrrId = "SRR014966".parse().unwrap();
    let uniprot: UniprotId = "P69905".parse().unwrap();
    let doi: Doi = "10.1038/s41586-020-2649-2".parse().unwrap();
    let gse: GeoSeriesAccession = "GSE102902".parse().unwrap();

    let protein_path = store.project_protein_path(&id, ProteinFormat::Pdb);
    assert!(protein_path.ends_with("proteins/1LYZ/1LYZ.pdb"));

    let genome_path = store.cache_genome_dir(&acc);
    assert!(genome_path.ends_with("genomes/GCF_000005845.2"));

    let srr_path = store.project_srr_dir(&srr);
    assert!(srr_path.ends_with("srr/SRR014966"));

    let uniprot_path = store.project_uniprot_dir(&uniprot);
    assert!(uniprot_path.ends_with("uniprot/P69905"));

    let doi_path = store.project_doi_dir(&doi);
    assert!(doi_path.starts_with(store.project_root()));
    let doi_meta = store.project_doi_metadata_path(&doi);
    assert!(doi_meta.to_string().contains("metadata/doi/"));

    let expr_path = store.project_expression_dir(&gse);
    assert!(expr_path.ends_with("expression/GSE102902"));

    let expr10x_path = store.project_expression10x_dir(&gse);
    assert!(expr10x_path.ends_with("expression10x/GSE102902"));

    let go_cache = store.cache_kb_dir("go");
    assert!(go_cache.to_string().contains("metadata/go"));
}
