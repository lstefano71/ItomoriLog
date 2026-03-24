# Portable Log/Browser

## 2) System & Technical Specification

### 2.1 Architecture Overview

* **App**: Avalonia AOT desktop (.NET 10 / C# 14).
* **Core services**:
  * **Session Manager**: session metadata, rules/templates, resume.
  * **Detection Engine**: pluggable detectors (Text/CSV/NDJSON) → `DetectionResult{BoundarySpec, TimestampExtractor, Confidence}`.
  * **Ingestion Orchestrator**: parallel per‑file read → batches → **DuckLake/ DuckDB** insert (ACID).
  * **Skip Logger**: emits skip records.
  * **Query Planner**: filters, keyset pagination, TICK rewrite → SQL + parameters.
  * **Visualization**: timeline binning queries; facet counts.
* **Storage**:
  * **DuckLake database** (local): catalog (schemas, snapshots, file pointers), **Parquet for data files**. Use DuckDB’s DuckLake extension. [\[notepad.onghu.com\]](https://notepad.onghu.com/2025/exploring-duckdb-part2/), [\[github.com\]](https://github.com/duckdb/duckdb/issues/600)
  * **Tables**: `logs`, `skips`, `segments`, `rules` (optional).
* **UI**:
  * Left rail: **Timeline + Facets + Skips**.
  * Main: **Virtualized Grid** + **Side Detail** (tabs: Overview, Structured, Raw).
  * Top: **Query box** (DSL + TICK), Live Tail (later).

### 2.2 Data Model

**Table: logs** (DuckLake)

* `timestamp_utc TIMESTAMP NOT NULL`
* `timestamp_basis VARCHAR` // Local|Utc|FixedOffset|Zone
* `timestamp_effective_offset_minutes INT`
* `timestamp_original VARCHAR NULL`
* `logical_source_id VARCHAR NOT NULL`  // from templates or derived
* `source_path VARCHAR NOT NULL`
* `physical_file_id VARCHAR NOT NULL`
* `segment_id VARCHAR NOT NULL`  // file × epoch
* `ingest_run_id VARCHAR NOT NULL`
* `record_index BIGINT`
* `level VARCHAR NULL`
* `message VARCHAR NULL`
* `fields JSON NULL`  // dynamic attributes from CSV/JSON

**Table: skips**

* `session_id VARCHAR NOT NULL`
* `logical_source_id VARCHAR NOT NULL`
* `physical_file_id VARCHAR NOT NULL`
* `segment_id VARCHAR NOT NULL`
* `segment_seq BIGINT NOT NULL`
* `start_line BIGINT NULL`, `end_line BIGINT NULL`
* `start_offset BIGINT NULL`, `end_offset BIGINT NULL`
* `reason_code VARCHAR`, `reason_detail VARCHAR`
* `sample_prefix BLOB NULL`
* `detector_profile_id VARCHAR`
* `utc_logged_at TIMESTAMP`

**Table: segments**

* `segment_id VARCHAR PRIMARY KEY`
* `logical_source_id VARCHAR NOT NULL`
* `physical_file_id VARCHAR NOT NULL`
* `min_ts_utc TIMESTAMP`, `max_ts_utc TIMESTAMP`
* `row_count BIGINT`
* `last_ingest_run_id VARCHAR`
* `active BOOLEAN DEFAULT TRUE`

> DuckLake uses **SQL tables for catalog & metadata** (schemas, file pointers, snapshots) and **Parquet** for data files, with **ACID** semantics and versioning; the DuckDB extension reads/writes these tables natively. [\[duckdb.org\]](https://duckdb.org/docs/stable/sql/functions/date), [\[notepad.onghu.com\]](https://notepad.onghu.com/2025/exploring-duckdb-part2/), [\[github.com\]](https://github.com/duckdb/duckdb/issues/600)

### 2.3 Ingestion

**Per file**:

1. Sniff first 64–256 KB or \~2k records.
2. Try detectors:
    * **CSV**: delimiter/header/column stability → candidates for timestamp columns and **composites** (e.g., `Date + Time + ms`).
    * **NDJSON**: per‑line JSON; multi‑line JSON if needed.
    * **Text**: SoR (Start‑of‑Record) regex library (source‑generated) + multiline continuation.
3. Score candidates: **parse rate** ≥95%, **monotonicity**, **continuity**, **stability**.
4. Cache profile; start ingest; **sliding validator** can trigger re‑detect mid‑file (rare).
5. **Write batches** into `logs` (DuckDB bulk insert / COPY), in a transaction.
6. Log **skips** with offsets/lines and reasons.

**Re‑ingest**

* For overrides (timezone policy change, detector tweak), reprocess just that **segment\_id**:

    ```sql
    BEGIN;
    DELETE FROM logs WHERE segment_id = $1;
    INSERT INTO logs (...) VALUES ...;   -- bulk
    UPDATE segments SET ... WHERE segment_id = $1;
    COMMIT;
    ```

    ACID makes this safe; DuckLake handles snapshots/catalog transparently. [\[duckdb.org\]](https://duckdb.org/docs/stable/sql/functions/date)

### 2.4 Querying & Pagination

* **Keyset pagination** using `(timestamp_utc, segment_id, record_index)` for smooth infinite scroll (no `OFFSET`).
* Always push down **time window** and **source includes/excludes** first.
* **Timeline**: server‑side binning with `date_trunc` or `time_bucket` (DuckDB) per zoom level. [\[questdb.com\]](https://questdb.com/blog/questdb-9-3-2-release/), [\[py-questdb...thedocs.io\]](https://py-questdb-client.readthedocs.io/en/latest/examples.html)
* **Facets**: tri‑state include/exclude; compute top‑K with current predicates.

### 2.5 TICK‑Style Periods

* **Syntax** (subset): dates, bracket ranges `[a..b]`, `T…` time, `@timezone`, `;duration`, `$now/$today` with +/‑ arithmetic (e.g., `'$now-1h..$now'`). [\[cynkra.github.io\]](https://cynkra.github.io/dd/reference/date_trunc.html)
* **Compiler**: C# rewriter converts to merged UTC `[start,end)` intervals, **DST‑aware** using `TimeZoneInfo` (or NodaTime).
* **Emission**:
  * For ≤64 intervals: parameterized OR chain.
  * For larger sets: temp table `_q_intervals(start_ts,end_ts)` + `EXISTS`.

QuestDB documents the behavior of `WHERE ts IN 'TICK'`, multiple ranges, business‑day filters, and merging of overlaps; we mimic the semantics while executing on DuckDB. [\[cynkra.github.io\]](https://cynkra.github.io/dd/reference/date_trunc.html), [\[newreleases.io\]](https://newreleases.io/project/github/questdb/questdb/release/9.3.2)

### 2.6 Damage Tolerance & Skips

* Open skip segment on windowed failure (e.g., CSV column mismatch runs) and **resync** (e.g., find K consecutive good rows).
* Reasons: `DecodeError`, `CsvColumnMismatch`, `JsonMalformed`, `RegexDrift`, `Oversize`, `TimeParse`, `ZipEntryCorrupt`, `UserSkip`.
* UI exposes **Skips panel** (grouped by file/segment), previews sample, and jumps to nearest good record.

### 2.7 Timezone Policy for Bare Timestamps

* **Default**: Assume **Local** (no `Z`/offset) → convert to UTC; store `timestamp_basis`/`effective_offset`.
* Per‑file override: **Local | UTC | FixedOffset | Zone**; apply retroactively to file; optional **templated rule** (“remember for similar files”).
* Store: `timestamp_utc`, `timestamp_basis`, `timestamp_effective_offset_minutes`, and optionally `timestamp_original`.

### 2.8 Performance Targets

* Ingest: ≥ 50–150k records/s on modern laptop for text/CSV (batching enabled).
* UI grid: perceivable 60fps feel; page fetch within 50–120 ms typical.
* Timeline: ≤ 150 ms first paint at coarse bins; progressive refine ≤ 400 ms.

### 2.9 Security & Privacy

* Local‑only by default; no background network calls.
* Session paths and file contents are not exfiltrated.
* Export features (Parquet/CSV) behind explicit actions.

### 2.10 Packaging

* **Single‑file publish**, **AOT** where possible (Avalonia supported AOT).
* Bundle DuckDB + DuckLake extension natives.
