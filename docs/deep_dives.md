# Portable Log Browser

## 3) Technical Deep Dives

### 3.1 Ingestion & Detection (Multiline + CSV splits + Re‑detect)

**Core types**

```csharp
public interface IFormatDetector
{
    DetectionResult Probe(Stream sample, string sourceName);
}

public sealed record DetectionResult(
    double Confidence,
    RecordBoundarySpec Boundary,
    ITimestampExtractor Extractor,
    string? Notes = null);

public abstract record RecordBoundarySpec; // CsvBoundary, JsonNdBoundary, TextSoRBoundary(Regex re, bool anchored)

public interface IRecordReader : IDisposable
{
    bool TryReadNext(out RawRecord raw); // RawRecord: first-line + full text or field bag
}

public interface ITimestampExtractor
{
    bool TryExtract(RawRecord raw, out DateTimeOffset ts);
    string Description { get; }
}
```

**Multiline text**

* **SoR regex** (anchored preferred). Non‑matching lines are **continuations**.
* State machine: find start, accumulate continuation until next start or EOF.
* Sliding validator: low SoR hit‑rate and high parse‑fail → **re‑detect** window.

**CSV with split timestamps**

* Detect delimiter/header; sample rows; collect candidates:
  * parseable date/time fields; numeric epoch columns; name hints (`date`,`time`,`ms`).
* Try composites: `Date + Time[ + fractional] [ + tz/offset]`.
* Score by parse rate, monotonicity, continuity; prefer simpler composites.

**Damage & resync**

* CSV: on malformed row streak, open skip; resync after K good rows; log segment.
* NDJSON: single line failure tolerable; streak opens skip.
* Text: if SoR detection collapses, resync by scanning ahead for next valid SoR.

**Skip logging**

```csharp
public sealed class SkipLogger { /* Begin(...) → SkipSegment : IDisposable; Close(...) emits a row */ }
```

Acceptance

* ≥95% successful timestamp extraction on sniff sample under varied formats.
* Skip segments persisted with start/end offsets or lines and `reason_code`.

***

### 3.2 TICK‑Style Period Compiler & SQL Emission

**Grammar subset**

    TICK_EXPR = DATE [TIME] [@TZ] [#FILTER] [;DUR]
    DATE     = 'YYYY-MM-DD' | '[YYYY-MM-[DD..DD]]' | '$today' | '$now +/- N(u)'
    TIME     = 'T'HH:MM[:SS[.fff]]
    @TZ      = '@IANA' | '@UTC' | '@+02:00'
    DUR      = ';' duration like '6h30m'
    RANGE    = DATE..DATE (relative supported, e.g., '$now-2h..$now')

(Modeled after QuestDB TICK docs) [\[cynkra.github.io\]](https://cynkra.github.io/dd/reference/date_trunc.html), [\[newreleases.io\]](https://newreleases.io/project/github/questdb/questdb/release/9.3.2)

**Compiler outline**

```csharp
public sealed record TickCompileResult(
    IReadOnlyList<UtcInterval> Intervals,
    string? NormalizedTick,
    string? Warning);

public sealed class TickCompiler : ITickCompiler
{
    public TickCompileResult Compile(string tick, TickContext ctx)
    {
        // 1) Parse → AST
        // 2) Expand dates/times/brackets in target TZ (TimeZoneInfo/NodaTime)
        // 3) For each anchor, create [start,end) with duration or day window
        // 4) Convert to UTC, resolve DST ambiguity/invalids
        // 5) Merge intervals
        // 6) Return intervals + normalized text
    }
}
```

**SQL emission on DuckDB**

* Small N: OR chain with parameters.
* Large N: temp interval table + `EXISTS` (fast to plan/run).
* **Timeline bins**: `SELECT date_trunc($bin, timestamp_utc) AS bucket, COUNT(*) ...` or `time_bucket(...)`. [\[questdb.com\]](https://questdb.com/blog/questdb-9-3-2-release/), [\[py-questdb...thedocs.io\]](https://py-questdb-client.readthedocs.io/en/latest/examples.html)

**DST policy**

* Ambiguous local times → choose post‑transition by default (document).
* Skipped times → shift to first valid instant.

Acceptance

* TICK examples:
  * `'$now-1h..$now'`, `'$today'`, `'2026-03-[24..26]T09:30@Europe/Rome;1h'`.
* Interval merging verified; large interval sets use temp table path.
* Compiler tolerates whitespace and normalizes output.

***

### 3.3 UI Virtualization & Pagination

**Keyset pagination**

* Order by `(timestamp_utc, segment_id, record_index)`.
* Page fetches 2–4k rows, with **front/back prefetch** and **recycling containers**.

**Avalonia grid**

* Fixed row height for fluid scroll; detail in side drawer (preferred) to keep list height stable.
* **Hot path** avoids heavy bindings; project visible columns to simple DTOs, hydrate JSON on expand.

**Timeline**

* Canvas with batched `DrawRects`.
* Progressive bin refinement (coarse → fine).

**Facets**

* Tri‑state include/exclude; tree for sources grouped by template tokens.
* Async counting; display approximate counts until stable.

Acceptance

* Smooth scroll through 1–10M rows with consistent interaction latency.
* Timeline first paint ≤150 ms for typical ranges; bins adapt on zoom.
* Facet counts match grid results under same predicates.

***

### 3.4 DuckLake Storage & Re‑ingestion

* Use DuckLake **ACID** operations for transactional delete+insert per segment; no bespoke manifest required (catalog & snapshots live in SQL). [\[duckdb.org\]](https://duckdb.org/docs/stable/sql/functions/date), [\[notepad.onghu.com\]](https://notepad.onghu.com/2025/exploring-duckdb-part2/)
* Data files are **Parquet**; catalog stores **file pointers, schema, stats**. [\[notepad.onghu.com\]](https://notepad.onghu.com/2025/exploring-duckdb-part2/)
* The DuckDB **DuckLake extension** reads/writes DuckLake datasets; install/attach and operate with SQL. [\[github.com\]](https://github.com/duckdb/duckdb/issues/600)

**Re‑ingest workflow**

1. Detect affected `segment_id`s (by file fingerprint or override scope).
2. `BEGIN; DELETE FROM logs WHERE segment_id IN (...); INSERT new rows; UPDATE segments; COMMIT;`
3. UI refresh: updated per‑source counts & min/max ts.

Acceptance

* Re‑ingest modifies only targeted segments and is atomic.
* Past runs can be snapshot‑queried (optional UI later).

***

## 4) Implementation Plan & Milestones

**Milestone 1 — Core ingest & browse (2–3 weeks)**

* Detectors (Text/CSV/NDJSON), multiline, CSV composite timestamps.
* DuckLake setup with `logs`, `skips`, `segments`. (DuckDB + DuckLake extension wired.) [\[github.com\]](https://github.com/duckdb/duckdb/issues/600)
* Virtual grid with keyset pagination; basic timeline; basic facets (Levels, Sources).
* Per‑file timezone overrides; rules/templates UX (light).

**Milestone 2 — TICK DSL & Damage audit (1–2 weeks)**

* TICK compiler MVP (dates, `$now/$today`, `@timezone`, `;duration`, ranges).
* Temp interval table path for large interval sets.
* Skips panel (viewer + jump).

**Milestone 3 — Polishing & performance (1–2 weeks)**

* Detection wizard; re‑ingest UX end‑to‑end.
* Progressive timeline refinement; facet top‑K caching.
* Export (Parquet/CSV) for current filter.

***

## 5) Open Questions / Decisions Needed

1. **TICK scope** for MVP: include `#workday/#weekend` now or phase 2? (Docs outline day filters). [\[cynkra.github.io\]](https://cynkra.github.io/dd/reference/date_trunc.html)
2. **Timezone engine**: stick to `TimeZoneInfo` or add **NodaTime** for edge‑case rigor?
3. **ICU** in DuckDB: we don’t rely on it; all TZ math client‑side—OK?
4. **Max expected session size** (rows and GB) to tune batch sizes and bin strategies.

***

## 6) Definition of Done (MVP)

* **Functional**
  * Ingests mixed sources with autodetection and produces a browsable session.
  * Reads/writes DuckLake; re‑ingests only the selected sources/segments (ACID). [\[duckdb.org\]](https://duckdb.org/docs/stable/sql/functions/date)
  * TICK expressions round‑trip from query box to intervals and filter the grid/timeline.
  * Skips are visible, linkable, and exportable.

* **Performance**
  * Grid scroll is consistently smooth; fetch time within budget.
  * Timeline & facets remain responsive under common workloads.

* **Quality**
  * Unit & property tests for detectors and TICK compiler (DST edges).
  * Integration tests for re‑ingest and skip logging.
  * Telemetry/logging (local) for troubleshooting.

***

## 7) Appendix — Quick References (for the devs)

* **DuckLake overview & rationale**: SQL‑backed catalog + Parquet; ACID; simpler than Iceberg/Delta for local use. [\[duckdb.org\]](https://duckdb.org/docs/stable/sql/functions/date)
* **DuckLake spec & tables**: catalog DB tables (`ducklake_*`), snapshots, file metadata. [\[notepad.onghu.com\]](https://notepad.onghu.com/2025/exploring-duckdb-part2/)
* **DuckLake DuckDB extension**: install/attach, manage databases/tables. [\[github.com\]](https://github.com/duckdb/duckdb/issues/600)
* **QuestDB TICK syntax**: `WHERE ts IN '...'`, bracket expansion, timezones, durations, day filters. [\[cynkra.github.io\]](https://cynkra.github.io/dd/reference/date_trunc.html), [\[newreleases.io\]](https://newreleases.io/project/github/questdb/questdb/release/9.3.2)
* **DuckDB date functions**: `date_trunc`, `date_part`; bucketing via `time_bucket`. [\[questdb.com\]](https://questdb.com/blog/questdb-9-3-2-release/), [\[py-questdb...thedocs.io\]](https://py-questdb-client.readthedocs.io/en/latest/examples.html)

