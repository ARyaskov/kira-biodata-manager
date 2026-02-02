# KIRA-BM Pipelines

This document describes the internal data flow for major operations in kira-bm.

## DOI resolution and dataset hydration

**Input:** `doi:<DOI>`

**Outputs:**
- `doi_resolution.json` written to the project store.
- Downloaded datasets for resolved targets (if any).

**Steps:**
1. Resolve DOI metadata via Crossref REST API.
2. Extract public repository identifiers using strict regex matching.
3. Validate identifiers via public APIs (NCBI, RCSB, UniProt, ENA).
4. Hydrate hierarchies (GSE -> GSM -> SRR; BioProject -> SRR / assemblies).
5. Resolve final dataset targets and dispatch to existing providers.

**Guarantees:**
- Deterministic extraction using explicit regex patterns only.
- No PDF parsing, HTML scraping, or probabilistic inference.
- Provenance captured in `doi_resolution.json`.

**Failure modes:**
- If Crossref is unavailable, DOI resolution fails with an explicit error.
- If no supported IDs are found, the command exits with:
  `DOI resolved successfully, but no supported public dataset identifiers were found`.
- If IDs are present but unavailable, they are marked as unresolved in provenance.

## GEO expression download (expression / expression10x)

**Input:** `expression:<GSE>` or `expression10x:<GSE>`

**Outputs:**
- GEO SOFT text captured in `.kira-bm/<type>/<GSE>/metadata/geo_soft.txt`.
- `metadata.json` with organism/bundle summary.
- Downloaded supplementary files (preserved, no recompression).

**Steps:**
1. Fetch GEO SOFT metadata for the series.
2. Extract supplementary file URLs from structured SOFT fields.
3. For `expression10x`, detect 10x bundles by filename patterns:
   `matrix.mtx.gz`, `barcodes.tsv.gz`, `features.tsv.gz`/`genes.tsv.gz`.
4. Download files into a temp dir, then atomically rename into place.

**Guarantees:**
- No HTML scraping or transformation.
- Deterministic selection based on filenames only.
- Atomic writes to avoid partial datasets.

**Failure modes:**
- If no supplementary files exist, an explicit GEO error is returned.
- If `expression10x` has no valid bundle, the command fails with a clear message.

## Knowledge base fetch (GO / KEGG / Reactome)

**Input:** `go`, `kegg`, `reactome`

**Outputs:**
- Official release files in `.kira-bm/metadata/<name>/`.
- `metadata.json` with source URLs and release metadata.

**Steps:**
1. Download official release artifacts (OBO/flat files).
2. Store once in the global cache (project duplication only with `--no-cache`).

**Guarantees:**
- No scraping or proprietary access.
- Cached and reusable across projects.

**Failure modes:**
- Upstream network/HTTP errors are surfaced as knowledge base errors.
