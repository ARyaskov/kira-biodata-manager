# Data Formats

## doi_resolution.json

Generated during `doi:<DOI>` resolution. Stored at:

```
.kira-bm/doi/<ENCODED_DOI>/doi_resolution.json
```

### Fields

- `doi` — the resolved DOI (lowercased).
- `source` — Crossref metadata fields used for extraction.
  - `title`
  - `abstract_text`
  - `references`
  - `links`
  - `data_availability`
- `extracted` — identifiers extracted via regex.
- `validation` — per-identifier validation status.
- `hydrated` — expansions from GEO/BioProject/ENA hierarchies.
- `resolved_targets` — final dataset targets to download.
- `unresolved` — identifiers that could not be validated or hydrated.

## expression metadata.json

Generated during `expression:<GSE>` or `expression10x:<GSE>`. Stored at:

```
.kira-bm/expression/<GSE>/metadata/metadata.json
.kira-bm/expression10x/<GSE>/metadata/metadata.json
```

### Fields

- `registry` — `geo`.
- `type` — `expression` or `expression10x`.
- `accession` — GEO series accession.
- `organism` — series organism if available.
- `bundle_format` — `10x` for expression10x.
- `n_bundles` — number of detected 10x bundles.
- `files` — filenames included in the dataset.
- `downloaded_at` — ISO-8601 timestamp.

## knowledge metadata.json

Generated for `go`, `kegg`, `reactome`. Stored at:

```
.kira-bm/metadata/go/metadata.json
.kira-bm/metadata/kegg/metadata.json
.kira-bm/metadata/reactome/metadata.json
```

### Fields

- `registry` — `go` / `kegg` / `reactome`.
- `type` — dataset name.
- `version` — release identifier if available.
- `release_date` — release date if available.
- `source_urls` — official source endpoints.
- `downloaded_at` — ISO-8601 timestamp.
