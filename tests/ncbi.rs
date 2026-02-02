use kira_biodata_manager::ncbi::map_genome_include;

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
