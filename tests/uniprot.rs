use std::fs;

use kira_biodata_manager::uniprot::extract_metadata;

#[test]
fn extract_uniprot_metadata() {
    let raw = fs::read_to_string("tests/fixtures/uniprot_P69905.json").unwrap();
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let meta = extract_metadata(&value).unwrap();

    assert_eq!(meta.accession, "P69905");
    assert_eq!(
        meta.protein_name.as_deref(),
        Some("Hemoglobin subunit alpha")
    );
    assert!(meta.gene_names.contains(&"HBA1".to_string()));
    assert_eq!(meta.organism.as_deref(), Some("Homo sapiens"));
    assert_eq!(meta.sequence_length, Some(141));
    assert!(meta.cross_references.pdb.contains(&"1A3N".to_string()));
    assert!(meta.functions.iter().any(|f| f.contains("Oxygen")));
    assert!(!meta.features.domains.is_empty());
    assert!(
        meta.features
            .variants
            .iter()
            .any(|f| f.name == "Natural variant")
    );
    assert!(meta.features.regions.iter().any(|f| f.name == "Region"));
    assert!(meta.features.repeats.iter().any(|f| f.name == "Repeat"));
    assert!(meta.features.motifs.iter().any(|f| f.name == "Motif"));
    assert!(
        meta.features
            .signal_peptides
            .iter()
            .any(|f| f.name == "Signal peptide")
    );
    assert!(
        meta.features
            .transmembrane
            .iter()
            .any(|f| f.name == "Transmembrane")
    );
    assert!(
        meta.features
            .topological_domains
            .iter()
            .any(|f| f.name == "Topological domain")
    );
    assert!(meta.features.helices.iter().any(|f| f.name == "Helix"));
    assert!(
        meta.features
            .coiled_coils
            .iter()
            .any(|f| f.name == "Coiled coil")
    );
    assert!(
        meta.features
            .zinc_fingers
            .iter()
            .any(|f| f.name == "Zinc finger")
    );
    assert!(meta.features.turns.iter().any(|f| f.name == "Turn"));
    assert!(meta.features.strands.iter().any(|f| f.name == "Strand"));
    assert!(
        meta.features
            .beta_strands
            .iter()
            .any(|f| f.name == "Beta strand")
    );
    assert!(
        meta.features
            .disordered_regions
            .iter()
            .any(|f| f.name == "Intrinsically disordered region")
    );
    assert!(
        meta.features
            .low_complexity_regions
            .iter()
            .any(|f| f.name == "Low complexity")
    );
    assert!(
        meta.features
            .signal_anchors
            .iter()
            .any(|f| f.name == "Signal anchor")
    );
    assert!(
        meta.features
            .transit_peptides
            .iter()
            .any(|f| f.name == "Transit peptide")
    );
    assert!(
        meta.features
            .beta_helices
            .iter()
            .any(|f| f.name == "Beta helix")
    );
    assert!(
        meta.features
            .propeptides
            .iter()
            .any(|f| f.name == "Propeptide")
    );
    assert!(
        meta.features
            .initiator_methionines
            .iter()
            .any(|f| f.name == "Initiator methionine")
    );
    assert!(
        meta.features
            .mature_chains
            .iter()
            .any(|f| f.name == "Chain")
    );
    assert!(
        meta.features
            .mature_peptides
            .iter()
            .any(|f| f.name == "Peptide")
    );
    assert!(
        meta.features
            .propeptide_peptides
            .iter()
            .any(|f| f.name == "Peptide")
    );
}
