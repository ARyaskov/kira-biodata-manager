use kira_biodata_manager::config::{
    Config, ConfigLoader, DoiEntry, GenomeEntry, ProteinEntry, SrrEntry, UniprotEntry,
    default_genome_include,
};
use kira_biodata_manager::domain::{Doi, ProteinFormat, SrrFormat, UniprotId};
use std::str::FromStr;

#[test]
fn parse_config_shorthand() {
    let config = Config {
        schema_version: None,
        proteins: vec![ProteinEntry::Shorthand("1LYZ".to_string())],
        genomes: vec![GenomeEntry::Shorthand("GCF_000005845.2".to_string())],
        srr: vec![SrrEntry::Shorthand("SRR014966".to_string())],
        uniprot: vec![UniprotEntry::Shorthand("P69905".to_string())],
        doi: vec![DoiEntry::Shorthand("10.1038/s41586-020-2649-2".to_string())],
    };

    let resolved = ConfigLoader::resolve_config(config).unwrap();
    assert_eq!(resolved.schema_version, 1);
    assert_eq!(resolved.proteins.len(), 1);
    assert_eq!(resolved.genomes.len(), 1);
    assert_eq!(resolved.srr.len(), 1);
    assert_eq!(resolved.uniprot.len(), 1);
    assert_eq!(resolved.doi.len(), 1);
    assert_eq!(resolved.proteins[0].format, ProteinFormat::Cif);
    assert_eq!(resolved.genomes[0].include, default_genome_include());
    assert_eq!(resolved.srr[0].format, SrrFormat::Fastq);
    assert_eq!(
        resolved.uniprot[0].id,
        UniprotId::from_str("P69905").unwrap()
    );
    assert_eq!(
        resolved.doi[0].id,
        Doi::from_str("10.1038/s41586-020-2649-2").unwrap()
    );
}
