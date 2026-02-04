# KIRA-BM CLI Reference

All commands are for the `kira-bm` binary.

## Global options

- `--non-interactive` — disables TUI, outputs JSON for list/info and JSON summary for fetch/clear/init
- `-h, --help`
- `-V, --version`

## Command groups

```
kira-bm <command>
kira-bm tools <subcommand>
```

## fetch

```
kira-bm fetch [SPECIFIER] [--config PATH] [--format <fmt>] [--paired] [--force] [--no-cache] [--dry-run] [--non-interactive]
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
kira-bm fetch protein:1LYZ
kira-bm fetch protein:1LYZ --format pdb
kira-bm fetch genome:GCF_000005845.2
kira-bm fetch srr:SRR014966 --format fastq --paired
kira-bm fetch uniprot:P69905
kira-bm fetch doi:10.1038/s41586-020-2649-2
kira-bm fetch expression:GSE102902
kira-bm fetch expression10x:GSE115978
kira-bm fetch go
kira-bm fetch kegg
kira-bm fetch reactome
```

## add

```
kira-bm add <SPECIFIER> [--config PATH] [--format <fmt>] [--paired] [--force] [--no-cache] [--dry-run] [--non-interactive]
```

Alias of `fetch`.

## list

```
kira-bm list [--non-interactive]
```

Lists datasets available in the project store and global cache.

## info

```
kira-bm info <SPECIFIER> [--non-interactive]
```

Prints metadata and resolved paths for a dataset.

## clear

```
kira-bm clear [--non-interactive]
```

Clears only the project-local store (`./.kira-bm/`).

## init

```
kira-bm init [--non-interactive]
```

Generates `kira-bm.json` from datasets already present in the project store.

## tools install-sra

```
kira-bm tools install-sra
```

Prints official SRA Toolkit install instructions (prefetch/fasterq-dump).
