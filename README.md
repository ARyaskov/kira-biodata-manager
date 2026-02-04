# kira-biodata-manager (kira-bm)

Reproducible bio-data manager with a project-local store and a shared global cache.
`kira-bm` it's like npm/cargo/pip for bioinformatics.

## Prerequisites

- Rust 1.91+ (Edition 2024)
- Network access to NCBI GEO/Datasets, RCSB PDB, UniProt, GO, KEGG, Reactome

## Installation

Install from crates.io (Rust 1.91+ / Windows / Linux / MacOS):

```bash
cargo install kira-biodata-manager
```

Or

Build from source (Rust 1.91+):

```bash
cargo build --release
```

## Config (`kira-bm.json`)

It's like `package.json`/`Cargo.toml`

```json
{
  "schema_version": 1,
  "proteins": [
    "1LYZ",
    { "id": "4V6X", "format": "pdb" }
  ],
  "genomes": [
    {
      "accession": "GCF_000005845.2",
      "include": ["genome", "gff3", "protein", "seq-report"]
    }
  ],
  "srr": [
    "SRR014966",
    { "id": "SRR123456", "format": "fasta", "paired": true }
  ],
  "uniprot": [
    "P69905",
    { "id": "Q9Y263" }
  ],
  "doi": [
    "10.1038/s41586-020-2649-2"
  ],
  "expression": [
    "GSE102902"
  ],
  "expression10x": [
    "GSE115978"
  ]
}
```

Notes:
- `schema_version` defaults to `1` if omitted.
- Protein format defaults to `cif`. Supported: `cif`, `pdb`, `bcif`.
- Genome `include` defaults to `["genome","gff3","protein","seq-report"]`.
- SRR format defaults to `fastq`. Supported: `fastq`, `fasta`.
- UniProt accepts accessions like `P69905` or `Q9Y263`.
- DOI accepts full DOI strings like `10.1038/s41586-020-2649-2`.
- GEO expression datasets accept `GSE` accessions (`expression`, `expression10x`).
- Knowledge bases are available as singletons: `go`, `kegg`, `reactome` (use CLI fetch).
- SRR datasets require NCBI SRA Toolkit (`prefetch` + `fasterq-dump`). See `kira-bm tools install-sra`.

## Usage

Fetch from config (`kira-bm.json`) in the current directory:

```bash
kira-bm fetch
or just
kira-bm
```

In case you have no `kira-bm.json` file in project and run `kira-bm` -- you'll see an interactive terminal user interface.

![Screenshot 1](./docs/scr1.jpg)
Fetch a specific dataset (add dataset to project's dataset directory):

```bash
kira-bm fetch protein:1LYZ
kira-bm fetch genome:GCF_000005845.2
kira-bm fetch srr:SRR014966
kira-bm fetch uniprot:P69905
kira-bm fetch expression:GSE102902
kira-bm fetch expression10x:GSE115978
kira-bm fetch go
kira-bm fetch kegg
kira-bm fetch reactome
```

Routing:
- Protein structures (`protein:<PDB_ID>`) are fetched from RCSB PDB.
- Genomes and SRR runs are fetched from NCBI.
- UniProt accessions (`uniprot:<ACCESSION>`) are fetched from UniProt.
- DOI-based discovery (`doi:<DOI>`) resolves metadata via Crossref and hydrates public dataset IDs.
- GEO expression datasets (`expression:<GSE>`, `expression10x:<GSE>`) are fetched from NCBI GEO.
- Knowledge bases (`go`, `kegg`, `reactome`) are fetched from their official sources.

![Screenshot 2](./docs/scr2.jpg)

List datasets (JSON in non-interactive mode):

```bash
kira-bm list --non-interactive
```

Show dataset info:

```bash
kira-bm info protein:1LYZ --non-interactive
```

Clear project store:

```bash
kira-bm clear
```

## DOI-driven dataset discovery

`kira-bm` can resolve a DOI into public repository identifiers (GEO/SRA/BioProject/Assembly/PDB/UniProt)
using structured metadata from Crossref, then hydrate and download the resolved datasets.

What it does:
- Resolves Crossref metadata (title/abstract/references/links).
- Extracts known identifiers via strict regex matching.
- Validates identifiers using public APIs.
- Hydrates hierarchies (e.g. GSE -> GSM -> SRR, BioProject -> SRR/assemblies).
- Writes `doi_resolution.json` provenance to the project store.

What it does NOT do:
- No PDF parsing.
- No publisher HTML scraping.
- No fuzzy matching or probabilistic inference.

Example:

```bash
kira-bm fetch doi:10.1038/s41586-020-2649-2
```

## Storage layout

Project store:

```
./.kira-bm/
  proteins/<ID>/<ID>.<ext>
  proteins/<ID>/metadata.json
  proteins/<ID>/metadata.raw.json
  genomes/<ACCESSION>/...
  srr/<SRR_ID>/reads.fastq
  srr/<SRR_ID>/reads_1.fastq
  srr/<SRR_ID>/reads_2.fastq
  srr/<SRR_ID>/metadata.json
  uniprot/<ACCESSION>/<ACCESSION>.fasta
  uniprot/<ACCESSION>/metadata.json
  uniprot/<ACCESSION>/raw.json
  doi/<ENCODED_DOI>/doi_resolution.json
  expression/<GSE>/...
  expression/<GSE>/metadata/metadata.json
  expression10x/<GSE>/... (10x bundles preserved)
  expression10x/<GSE>/metadata/metadata.json
  metadata/<TYPE>/<ID>.json
  metadata/go/go-basic.obo
  metadata/go/metadata.json
  metadata/kegg/...
  metadata/kegg/metadata.json
  metadata/reactome/...
  metadata/reactome/metadata.json
```

Global cache:

```
~/.cache/kira-biodata-manager/
  proteins/<ID>/<ID>.<ext>
  proteins/<ID>/metadata.json
  proteins/<ID>/metadata.raw.json
  genomes/<ACCESSION>/...
  srr/<SRR_ID>/reads.fastq
  srr/<SRR_ID>/reads_1.fastq
  srr/<SRR_ID>/reads_2.fastq
  srr/<SRR_ID>/metadata.json
  uniprot/<ACCESSION>/<ACCESSION>.fasta
  uniprot/<ACCESSION>/metadata.json
  uniprot/<ACCESSION>/raw.json
  expression/<GSE>/...
  expression/<GSE>/metadata/metadata.json
  expression10x/<GSE>/... (10x bundles preserved)
  expression10x/<GSE>/metadata/metadata.json
  metadata/go/go-basic.obo
  metadata/go/metadata.json
  metadata/kegg/...
  metadata/kegg/metadata.json
  metadata/reactome/...
  metadata/reactome/metadata.json
  metadata/<TYPE>/<ID>.json
```

## Output contracts

`--non-interactive` mode:
- `list` and `info` output JSON to stdout.
- `fetch` and `clear` output JSON summaries.
- Errors go to stderr with non-zero exit codes.



## Optional external tools

kira-bm may optionally invoke externally installed third-party tools
(e.g. NCBI SRA Toolkit https://github.com/ncbi/sra-tools ).
These tools are not bundled, not distributed, and are subject
to their own licenses.
Users are responsible for installing and complying with those licenses.
