# ItomoriLog — Design Decisions

> Captured during a systematic design review (2026-03-24). Each decision was discussed and confirmed. These decisions refine and extend the PRD, system specs, and deep dives.

---

## 1. Ingestion & Detection

### 1.1 Detector Disambiguation (#1)
When two detectors both score above threshold, rank by `(Confidence desc, detector_priority)`. Fixed priority: **NDJSON > CSV > Text** (most structured wins). If top two are within 0.02, surface a disambiguation prompt in the progress pane.

### 1.2 Sliding Validator — Mid-File Re-detect (#2)
Rolling window of **256 records**. Track SoR match rate and timestamp extraction success rate. If either drops below **80%**, freeze the current segment, sniff the next 2K records, and re-run detection as a new `segment_id`. Fully automatic — log as a "format change" event in the progress pane (not a skip). User can override via re-ingest.

### 1.3 Damage Resync Thresholds (#3)
Resync K varies by format:
- **CSV:** K = 5 good rows
- **NDJSON:** K = 3
- **Text/SoR:** K = 5 (SoR match + timestamp extraction)

Max consecutive failures before abandoning a file: **10,000**. Mark remainder as a single mega-skip with `reason_code = Abandoned`.

### 1.4 Segment & File Identity (#4)
- `segment_id` = `SHA256(physical_file_id + ":" + format_region_index)[0:16hex]`. `format_region_index` is 0 normally, 1+ if the sliding validator triggered mid-file re-detect. Deterministic — re-ingesting the same file with the same detection yields the same `segment_id`, making `DELETE+INSERT` idempotent.
- `physical_file_id` = `SHA256(canonical_path + ":" + file_size + ":" + last_modified_utc)[0:16hex]`.
- `ingest_run_id` is a separate GUID tracking *when* (not *what*).

### 1.5 ZIP Handling (#5)
Each ZIP entry is an independent file with its own `physical_file_id`, detection pass, and `segment_id`. `source_path` uses the `!` separator: `/path/to/archive.zip!/entry/name.log`. No nested ZIP support in MVP — skip with `reason_code = NestedArchive`.

### 1.6 Non-Log Files (#6)
If detection finds no viable detector after sniffing (all below threshold), flag the file immediately and skip entirely. `reason_code = NotRecognized`. Surface in the progress pane.

### 1.7 Dual Timestamps (#15)
When a record has two timestamps (e.g., local in brackets + UTC with fractional seconds), prefer the more precise, explicit-offset one as `timestamp_utc`. Store the other in `timestamp_original`. User can override.

### 1.8 Two-Digit Years (#16)
Windowed century: YY ≤ 49 → 20xx, YY ≥ 50 → 19xx. Surface a one-time confirmation during sniff.

### 1.9 Field Synthesis from Text Logs (#14, #17)
**MVP scope** — essential. After timestamp extraction, apply a field extraction regex with named groups to the remainder of the first line. Ship a small library of common post-timestamp patterns. User can define custom field extraction regex in the detection wizard. Regex stored in the detection profile, carried over via templates/FKB. Extracted fields go into `fields` JSON and are promotable to facets. Field names are **user-assignable** — the user labels what each captured group means (e.g., `[CAL12]` = username, `SRV` = subsource).

### 1.10 Well-Known Field Names (#18)
Reserved names with special UI behavior:

| Field | Behavior |
|-------|----------|
| `level` | Color-coded chip in grid, severity facet |
| `subsource` | Merged into `logical_source_id` hierarchy for facet tree |
| `task_id` | Enables "follow this task" filter |
| `username` | User facet |
| `message` | Populates the `message` column directly (not `fields` JSON) |

All other fields are custom — stored in `fields` JSON, searchable, facetable, no special UI.

### 1.11 Encoding Detection (#43)
Sniff during initial read: (1) check BOM, (2) try UTF-8 — if clean, assume UTF-8, (3) fallback to system default code page (Windows-1252 on Western European). User override in detection wizard. Store encoding in detection profile/rules.

### 1.12 Max Record Size (#42)
Max **1 MB** per record (`IngestOptions.MaxRecordBytes = 1_048_576`). If exceeded: close record at limit, emit truncated, open skip with `reason_code = Oversize`, resync from next SoR match.

### 1.13 Detection Wizard UX (#25)
Staged progress pane, **not** a blocking wizard:
1. File selection → parallel sniffing starts immediately
2. Detection results stream in per file (format, confidence, preview, extracted fields)
3. Auto-ingest starts for ≥95% confidence files while user reviews others
4. User can override/edit any detection at any time → triggers re-ingest
5. Principle: **never block the happy path**

### 1.14 File Change Detection (#27)
On adding files to an existing session, compute `physical_file_id` and check against `segments` table. Options: **Skip** (default), **Re-ingest**, **Force add**. If file's `last_modified` changed since original ingest, flag it and suggest re-ingest.

---

## 2. Timezone Policy

### 2.1 Mixed Offsets Within a File (#7)
Per-record decision: if the timestamp has an explicit offset/Z, use it directly. If bare, apply the file's timezone policy (default Local). A single segment can have mixed `timestamp_basis` values. `timestamp_effective_offset_minutes` stores the **actual offset applied at ingest time** for that specific record. The zone name lives on file-level policy, not per-row.

### 2.2 Template Rules (#8)
- Glob-style syntax with token capture: `server-{host}-{env}-{YYYYMMDD}.log`
- Filename timestamp tokens (`{YYYY}`, `{MM}`, etc.) are normalized away for template matching
- Non-timestamp name changes **are significant**: `ptftst{YYYYMM}` vs `prftstZ{YYYYMM}` are different templates
- Decaying confidence: ×0.9 per session since last use, cross-session only via FKB
- Rules dual-stored: session DB (accepted) + FKB (suggestions). Never auto-applied.

---

## 3. TICK DSL

### 3.1 Invalid/Ambiguous Expressions (#9)
- Future ranges: valid
- Invalid dates (Feb 30): compile error with clear `Warning`, empty intervals
- DST gap: shift forward to first valid instant + Warning
- DST ambiguity (fall-back): post-transition offset by default
- Empty/garbage: compile error. **Never throw exceptions** — always return a `TickCompileResult`

### 3.2 Query Composition (#10)
**Implicit AND** between terms. **Explicit OR** keyword. **Parentheses** for grouping. TICK clause always ANDed. Facet selections also ANDed. No TICK clause → time window from timeline selection or full session range.

### 3.3 Interval Threshold (#11)
Configurable: `TickEmitterOptions.MaxOrChainIntervals = 64`. >64 intervals expected to be extremely rare. Temp table: `CREATE OR REPLACE TEMP TABLE` per query, no cleanup. Optimize the OR-chain path first.

### 3.4 Session-Relative Variables (#41)
- `$now` = current wall clock UTC
- `$today` = current day wall clock
- `$latest` = max `timestamp_utc` in session
- `$first` = min `timestamp_utc` in session

All computed at query time. Common: `$first..$latest` (full session), `$latest-1h..$latest` (last hour of data).

---

## 4. Query & Pagination

### 4.1 Keyset Cursor (#12)
Cursor `(timestamp_utc, segment_id, record_index)` is globally unique. `segment_id` + `record_index` are **tiebreakers** for deterministic total ordering across all files — not filters. All sources interleaved chronologically. Enforced with:
```sql
CREATE UNIQUE INDEX idx_logs_cursor ON logs(timestamp_utc, segment_id, record_index);
```

### 4.2 Logical Source ID (#33)
Default `logical_source_id` = filename minus timestamp tokens and extension. With template tokens, it becomes the pattern. Facet tree: logical source → files → subsource.

### 4.3 Text Search (#31)
MVP: DuckDB `ILIKE`. Case-insensitive by default. Later: DuckDB `fts` extension if needed.

---

## 5. UI

### 5.1 Grid Columns (#13)
Default visible: **Timestamp** (session TZ), **Level** (chip, graceful when null), **Source**, **Message** (truncated). Hidden: source_path, segment_id, record_index, timestamp_original, etc. Column layout persisted per session in `ui_preferences` JSON.

### 5.2 Detail Drawer (#23, #44)
Three tabs: **Overview** (key-value fields), **Structured** (fields JSON tree), **Raw** (exact original text). Raw tab uses a **virtualized text viewer** with line numbers, Ctrl+A/C/F support. Handles records up to 1 MB.

### 5.3 Follow Task (#19)
Right-click → "Follow task [X]" adds `task_id:X` to query box as a regular filter. No special mode. Bonus: rows from the same `task_id` get auto-assigned left-border color hue.

### 5.4 Timeline Interaction (#21)
Click bin → time window. Click+drag → range. Scroll wheel → zoom (day→hour→minute→second). Shift+drag → pan. Double-click → reset. Selection is a **separate implicit filter** ANDed with query box — does not inject TICK. Visible label: "Showing 09:30:00 – 09:45:00" with ✕ to clear.

### 5.5 Facets (#22)
Tri-state: Neutral → Include → Exclude → Neutral. No mixing include+exclude in same facet. Counts reflect current filters, update async. **High-cardinality** facets: ordered by count descending, **search-as-you-type** filter. Right-click grid → "Include/Exclude [value] in facet."

### 5.6 Global List Behavior (#24)
**All selectable lists** in the app support: search-as-you-type, mouse navigation + wheel, keyboard (arrows, PgUp/PgDn, Home/End), Enter to select.

### 5.7 Skips Panel (#32)
Left rail badge. Grouped by file → skip segment. Shows: reason code, line range, sample prefix, surrounding timestamps. "Jump to context" and "Re-ingest" per file. Full reason codes: `DecodeError`, `CsvColumnMismatch`, `JsonMalformed`, `RegexDrift`, `Oversize`, `TimeParse`, `ZipEntryCorrupt`, `UserSkip`, `NestedArchive`, `NotRecognized`, `Abandoned`, `IOError`.

### 5.8 Keyboard Shortcuts (#36)
`Ctrl+F` query box, `Ctrl+G` go-to-timestamp (via palette), `Escape` dismiss, `F5` refresh, `Ctrl+E` export, `Ctrl+O` open/add files, `Up/Down` grid, `Enter` detail, `Ctrl+T` follow task.

### 5.9 Command Palette (#37)
VS Code-style (`Ctrl+Shift+P`). Integrates all commands, go-to-timestamp as a palette mode, and query history (MRU). **Query history** is dual-level: per-session (session DB) and global (`global.duckdb`). Shown as suggestions in query box and searchable via palette.

### 5.10 Accessibility (#45)
MVP: keyboard navigable, respect Windows high contrast, severity colors have non-color differentiators. Not MVP: screen reader, localization, RTL.

---

## 6. Session Management

### 6.1 Sessions as Living Workspaces (#26)
Sessions persist across runs and can be extended. Input methods: file picker, folder picker, **drag-and-drop from Explorer**. This is a core concept: sessions are investigative workspaces that grow as you discover more relevant logs.

### 6.2 Lifecycle & Crash Recovery (#20)
Add `ingest_runs` table (`run_id`, `started_utc`, `completed_utc NULL = interrupted`, `status`). On open, if incomplete runs detected → "Last ingest was interrupted" banner. Abandoned sessions: not auto-deleted. Moved sessions: grayed "not found" with re-locate option.

### 6.3 Multi-Instance Concurrency (#39)
Session access is **exclusive** per instance (DuckDB single-writer). Global settings use `~/.itomorilog/global.duckdb` for ACID safety (#40). Replaces `RecentSessions.json`. Contains: recent sessions, FKB rules, global query history, app preferences.

---

## 7. Data Model Additions

### 7.1 Rules Table (#38)
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

### 7.2 Ingest Runs Table (#20)
```sql
CREATE TABLE ingest_runs (
  run_id VARCHAR PRIMARY KEY,
  started_utc TIMESTAMP,
  completed_utc TIMESTAMP NULL,
  status VARCHAR                -- 'running', 'completed', 'interrupted'
);
```

### 7.3 Segments Table — Additional Column (#34)
Add `last_byte_offset BIGINT` to the `segments` table to enable future live-tail without re-reading entire files.

### 7.4 Updated Skip Reason Codes
`DecodeError`, `CsvColumnMismatch`, `JsonMalformed`, `RegexDrift`, `Oversize`, `TimeParse`, `ZipEntryCorrupt`, `UserSkip`, `NestedArchive`, `NotRecognized`, `Abandoned`, `IOError`

---

## 8. Error Handling (#35)
- **Transient** (file locked, permission denied): inline in progress pane, `reason_code = IOError`, don't abort
- **Fatal** (disk full, DB corruption): modal dialog, ingest stops, committed data retained
- **No silent failures.** App diagnostic log at `~/.itomorilog/app.log`

---

## 9. Export (#28)
Three modes: current view (filtered), full session, selection. Target folder: user prompt with last-used memory. Parquet metadata includes session info. CSV gets companion `_metadata.json`.

---

## 10. Performance (#29)
- Batch size: **50,000** records per INSERT (`IngestOptions.BatchSize`)
- Memory budget: **~1 GB** peak acceptable
- Backpressure: bounded `Channel<LogRow[]>(capacity: 16)`
- 8+ concurrent file processing

---

## 11. Packaging (#30)
MVP: **Windows x64** only. Bundle `duckdb.dll` + DuckLake extension. Single-file AOT publish. No network calls on first run.

---

## 12. Deferred to Phase 2+
- Live tail (design for it: `last_byte_offset` in segments)
- Nested ZIP support
- Full-text search index (DuckDB fts extension)
- Parsing tabular data within multiline records
- Cross-platform packaging (Linux/macOS)
- Customizable shortcuts, screen reader, localization
- `#workday`/`#weekend` TICK filters

---

## MVP Feature Summary

**IN:** Text/CSV/NDJSON/ZIP ingest with auto-detection, field synthesis, timezone policy, TICK DSL ($now/$today/$first/$latest), query box (implicit AND/explicit OR/parentheses/ILIKE), keyset-paginated virtual grid, timeline histogram, tri-state facets, detail drawer, command palette, keyboard shortcuts, search-as-you-type lists, session-per-DB, incremental ingestion, drag-and-drop, export, skips panel, Windows x64, single-file AOT, bundled DuckDB.
