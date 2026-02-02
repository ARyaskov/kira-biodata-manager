# KIRA-BM CLI Reference

All commands are for the `kira-bm` binary.

## Global options

- `--non-interactive` — disables TUI, outputs JSON for list/info and JSON summary for fetch/clear/init
- `-h, --help`
- `-V, --version`

## Command groups

```
kira-bm data <subcommand>
```

Aliases:
- `kira-bm data` → `kira-bm data fetch`
- `kira-bm data add` → `kira-bm data fetch`

## data fetch

```
kira-bm data fetch [SPECIFIER] [--config PATH] [--format <fmt>] [--paired] [--force] [--no-cache] [--dry-run] [--non-interactive]
```

Notes:
- If `SPECIFIER` is omitted, the tool loads `kira-bm.json` from the current directory.
- If `--config PATH` is set, only that file is used.
- `--format` is only valid for `protein` and `srr` datasets.
- `--paired` is only valid for `srr` datasets.
- `--no-cache` writes only to the project store.
- `--force` re-downloads even if cache/project already has the dataset.

### Supported specifiers

- `protein:<PDB_ID>` — e.g. `protein:1LYZ`
  - formats: `cif` (default), `pdb`, `bcif`
- `genome:<ASSEMBLY>` — e.g. `genome:GCF_000005845.2`
- `srr:<SRR_ID>` — e.g. `srr:SRR014966`
  - formats: `fastq` (default), `fasta`
  - `--paired` enables paired-end output
- `uniprot:<ACCESSION>` — e.g. `uniprot:P69905`
- `doi:<DOI>` — e.g. `doi:10.1038/s41586-020-2649-2`
- `expression:<GSE>` — e.g. `expression:GSE102902`
- `expression10x:<GSE>` — e.g. `expression10x:GSE115978`
- `go`
- `kegg`
- `reactome`

### Examples

```
kira-bm data fetch protein:1LYZ
kira-bm data fetch protein:1LYZ --format pdb
kira-bm data fetch genome:GCF_000005845.2
kira-bm data fetch srr:SRR014966 --format fastq --paired
kira-bm data fetch uniprot:P69905
kira-bm data fetch doi:10.1038/s41586-020-2649-2
kira-bm data fetch expression:GSE102902
kira-bm data fetch expression10x:GSE115978
kira-bm data fetch go
kira-bm data fetch kegg
kira-bm data fetch reactome
```

## data list

```
kira-bm data list [--non-interactive]
```

Lists datasets available in the project store and global cache.

## data info

```
kira-bm data info <SPECIFIER> [--non-interactive]
```

Prints metadata and resolved paths for a dataset.

## data clear

```
kira-bm data clear [--non-interactive]
```

Clears only the project-local store (`./.kira-bm/`).

## data init

```
kira-bm data init [--non-interactive]
```

Generates `kira-bm.json` from datasets already present in the project store.
