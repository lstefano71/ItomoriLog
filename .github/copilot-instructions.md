# ItomoriLog — Copilot Instructions

> **Status:** Pre-implementation. The repo currently contains design documents only. The code described below is the target implementation.

## Project Overview

ItomoriLog is a cross-platform desktop log ingestion and browsing app — fast, local-first, built on **.NET 10 / C# 14**, **Avalonia** (AOT-compatible), and **DuckDB + DuckLake** storage (ACID, Parquet-backed).

## Target Assembly Structure

```
ItomoriLog.App        // Avalonia shell, startup, Brand constants
ItomoriLog.UI         // Views, ViewModels, controls
ItomoriLog.Ingest     // Detectors, record readers, timestamp extractors
ItomoriLog.DuckLake   // DuckDB/DuckLake integration, session store
ItomoriLog.Query      // Query planner, keyset pagination, TICK DSL rewriter
ItomoriLog.Model      // Shared records and contracts
ItomoriLog.Tests      // Unit, property, and integration tests
```

## Architecture

### Session-per-DB Model
Each session is a self-contained folder with a single `ItomoriLog.duckdb` file:
```
~/ItomoriLog/Sessions/
  2026-03-24_09-47-02_ItomoriLog/
    ItomoriLog.duckdb   ← DuckLake catalog + Parquet data + session metadata
    exports/
```
There is no global catalog DB. "Recent sessions" uses a lightweight `RecentSessions.json` cache that is validated by reading the `session` table from each DB on access.

### Core DuckLake Tables
- **`session`** — title, description, default timezone, app version (source of truth for session identity)
- **`logs`** — `(timestamp_utc, timestamp_basis, timestamp_effective_offset_minutes, timestamp_original, logical_source_id, source_path, physical_file_id, segment_id, ingest_run_id, record_index, level, message, fields JSON)`
- **`segments`** — per-file ingest units; `segment_id` is the key for re-ingest operations
- **`skips`** — damaged/unparseable records with `start_offset`/`end_offset`, `reason_code`, `sample_prefix`

### Ingestion Pipeline
1. Sniff first 64–256 KB / ~2k records
2. Parallel detector probing: `IFormatDetector.Probe(Stream, string) → DetectionResult`
3. Score candidates on parse rate (≥95%), monotonicity, continuity
4. Stream records → bulk INSERT into DuckLake in batches
5. Log skips with offsets and `reason_code`

**Re-ingest** is always scoped to a `segment_id` — never a full rebuild:
```sql
BEGIN;
DELETE FROM logs WHERE segment_id = $1;
INSERT INTO logs (...) VALUES ...;
UPDATE segments SET ... WHERE segment_id = $1;
COMMIT;
```

### Query & Pagination
- **Keyset pagination only** — never use `OFFSET`. Cursor: `(timestamp_utc, segment_id, record_index)`
- Always push down time window and source filters first
- Timeline binning uses DuckDB `date_trunc` / `time_bucket` server-side

### TICK Period DSL
The query box supports `timestamp IN 'TICK'` expressions modeled after QuestDB:
```
'$now-1h..$now'
'$today'
'2026-03-[24..26]T09:30@Europe/Rome;1h'
```
The `TickCompiler` class in `ItomoriLog.Query` parses TICK → AST → merged UTC `[start,end)` intervals (DST-aware via `TimeZoneInfo` or NodaTime). Emission: ≤64 intervals → OR-chain; >64 → temp table `_q_intervals(start_ts,end_ts)` + `EXISTS`.

## Key Conventions

### Interfaces and sealed records
Core ingest contracts use interfaces + sealed records, not abstract classes:
```csharp
public interface IFormatDetector { DetectionResult Probe(Stream sample, string sourceName); }
public sealed record DetectionResult(double Confidence, RecordBoundarySpec Boundary, ITimestampExtractor Extractor, string? Notes = null);
public interface ITimestampExtractor { bool TryExtract(RawRecord raw, out DateTimeOffset ts); }
```

### Timezone policy
Bare timestamps (no offset) default to **Local** and are converted to UTC at ingest time. Always store `timestamp_basis` (`Local|Utc|FixedOffset|Zone`), `timestamp_effective_offset_minutes`, and optionally `timestamp_original`.

### Skip reason codes
`DecodeError`, `CsvColumnMismatch`, `JsonMalformed`, `RegexDrift`, `Oversize`, `TimeParse`, `ZipEntryCorrupt`, `UserSkip`, `NestedArchive`, `NotRecognized`, `Abandoned`, `IOError`

### Damage resync
On a streak of failures, open a skip segment; resync after K consecutive good rows. Text: scan ahead for next valid SoR regex match.

### UI performance
- Fixed row heights in the virtualized grid (Avalonia); detail in a side drawer
- Hot-path avoids heavy bindings — project to simple DTOs, hydrate `fields` JSON only on expand
- Timeline canvas uses batched `DrawRects`; progressive bin refinement (coarse → fine)

## Avalonia Theme
Custom resource dictionary at `Themes/ItomoriLogTheme.axaml`, merged after `FluentTheme Mode="Dark"`:
```xml
<Application.Styles>
  <FluentTheme Mode="Dark"/>
  <StyleInclude Source="avares://ItomoriLog/Styles/ItomoriLogTheme.axaml"/>
</Application.Styles>
```

Brand color keys: `Itomori.SakuraPink` (#FF6DAE), `Itomori.NeonCyan` (#0EA5E9), `Itomori.Onyx` (#111418), `Itomori.Slate` (#1F2430), `Itomori.Mist` (#D6E2F0).  
Severity brush keys: `Brush.Info`, `Brush.Warn`, `Brush.Error`, `Brush.Debug`.

## Additional Tables

### `rules` — detection profiles, timezone overrides, field extraction patterns
```sql
CREATE TABLE rules (
  rule_id VARCHAR PRIMARY KEY,
  segment_id VARCHAR,           -- NULL = session-wide default
  rule_type VARCHAR NOT NULL,   -- 'timezone', 'detection', 'field_extraction', 'template'
  config JSON NOT NULL,
  created_utc TIMESTAMP,
  source VARCHAR                -- 'auto', 'user', 'fkb_suggestion'
);
```

### `ingest_runs` — crash recovery tracking
```sql
CREATE TABLE ingest_runs (
  run_id VARCHAR PRIMARY KEY,
  started_utc TIMESTAMP,
  completed_utc TIMESTAMP NULL,  -- NULL = interrupted
  status VARCHAR                 -- 'running', 'completed', 'interrupted'
);
```

### Global storage
All global state (recent sessions, FKB rules, query history, preferences) lives in `~/.itomorilog/global.duckdb` — not JSON files. This ensures ACID safety across multiple app instances.

## Field Synthesis
After timestamp extraction, an `IFieldExtractor` applies a regex with named groups to extract structured fields from text log lines. Well-known field names with special UI behavior: `level`, `subsource`, `task_id`, `username`, `message`. All other fields are custom (stored in `fields` JSON, searchable, facetable). Field names are user-assignable in the detection wizard.

## Identity Generation
- `segment_id` = `SHA256(physical_file_id + ":" + format_region_index)[0:16hex]` — deterministic, idempotent
- `physical_file_id` = `SHA256(canonical_path + ":" + file_size + ":" + last_modified_utc)[0:16hex]`
- ZIP entries: `source_path` uses `!` separator (`/path/to/archive.zip!/entry/name.log`)

## Performance Targets
- Ingest: ≥50–150k records/s (text/CSV with batching), batch size 50K records, ~1 GB peak memory OK
- Grid fetch: 50–120 ms typical
- Timeline first paint: ≤150 ms (coarse bins); progressive refine ≤400 ms
- MVP platform: Windows x64 only, bundled DuckDB + DuckLake extension

## TICK DSL Variables
- `$now` = current wall clock UTC
- `$today` = current day wall clock
- `$first` = min timestamp_utc in session (session-relative)
- `$latest` = max timestamp_utc in session (session-relative)

## Query Box Grammar
Implicit AND between terms. Explicit `OR` keyword. Parentheses for grouping. TICK clause (`timestamp IN '...'`) always ANDed. Facet selections also ANDed. Timeline selection is a separate implicit filter (not injected into query box). Free text → `ILIKE` (case-insensitive by default).

## Session Model
Sessions are **living investigative workspaces** — not one-shot imports. They persist across runs and can be extended with new files via file picker, folder picker, or drag-and-drop. Session access is exclusive per instance (DuckDB single-writer).

## Testing Priorities
- Unit + property tests for each `IFormatDetector` and the `TickCompiler` (focus on DST edge cases)
- Integration tests for re-ingest atomicity and skip logging
- Use property-based testing for timestamp parsing (varied formats, monotonicity checks)
