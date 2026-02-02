use kira_biodata_manager::providers::doi::extract_ids;

#[test]
fn extract_ids_from_text() {
    let texts = vec![
        "GSE12345 and GSM67890 with SRR014966".to_string(),
        "BioProject PRJNA123456 and ERP012345".to_string(),
        "Assembly GCF_000005845.2 and GCA_000005845.1".to_string(),
        "PDB 1LYZ and UniProt P69905".to_string(),
        "ENA run ERR123456".to_string(),
    ];

    let extracted = extract_ids(&texts);
    assert!(extracted.geo_series.contains(&"GSE12345".to_string()));
    assert!(extracted.geo_samples.contains(&"GSM67890".to_string()));
    assert!(extracted.sra_runs.contains(&"SRR014966".to_string()));
    assert!(extracted.bioprojects.contains(&"PRJNA123456".to_string()));
    assert!(extracted.ena_projects.contains(&"ERP012345".to_string()));
    assert!(
        extracted
            .assemblies
            .contains(&"GCF_000005845.2".to_string())
    );
    assert!(
        extracted
            .assemblies
            .contains(&"GCA_000005845.1".to_string())
    );
    assert!(extracted.pdb.contains(&"1LYZ".to_string()));
    assert!(extracted.uniprot.contains(&"P69905".to_string()));
    assert!(extracted.ena_runs.contains(&"ERR123456".to_string()));
}
