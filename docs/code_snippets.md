Absolutely—here’s a **single, copy‑pasteable appendix** that collects **all code & query samples** we drafted during the brainstorm. I grouped them by topic so you (or your dev) can drop each block into the repo / playground projects as needed.

> ⚠️ Note: there are **two storage variants** below:
>
> 1. **DuckLake/DuckDB (recommended)** — ACID table format on Parquet with SQL catalog; simple `DELETE+INSERT` re‑ingest.
> 2. **Legacy mini‑lake (manifest + Parquet parts)** — only keep if you want pure‑file mode.  
>     If you adopt **DuckLake**, you can ignore the legacy manifest section.

***

# Code & Query Appendix

## 0) Namespaces used in snippets

```csharp
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
```

***

## 1) **Storage: DuckLake / DuckDB** (recommended)

### 1.1 DDL (DuckDB SQL) — tables we write to

```sql
-- LOGS: main facts
CREATE TABLE IF NOT EXISTS logs (
  timestamp_utc TIMESTAMP NOT NULL,
  timestamp_basis VARCHAR,
  timestamp_effective_offset_minutes INTEGER,
  timestamp_original VARCHAR,
  logical_source_id VARCHAR NOT NULL,
  source_path VARCHAR NOT NULL,
  physical_file_id VARCHAR NOT NULL,
  segment_id VARCHAR NOT NULL,
  ingest_run_id VARCHAR NOT NULL,
  record_index BIGINT,
  level VARCHAR,
  message VARCHAR,
  fields JSON
);

-- SKIPS: audit of damaged regions
CREATE TABLE IF NOT EXISTS skips (
  session_id VARCHAR NOT NULL,
  logical_source_id VARCHAR NOT NULL,
  physical_file_id VARCHAR NOT NULL,
  segment_id VARCHAR NOT NULL,
  segment_seq BIGINT NOT NULL,
  start_line BIGINT,
  end_line BIGINT,
  start_offset BIGINT,
  end_offset BIGINT,
  reason_code VARCHAR,
  reason_detail VARCHAR,
  sample_prefix BLOB,
  detector_profile_id VARCHAR,
  utc_logged_at TIMESTAMP
);

-- SEGMENTS: per (file × format-epoch) stats to accelerate pruning in UI
CREATE TABLE IF NOT EXISTS segments (
  segment_id VARCHAR PRIMARY KEY,
  logical_source_id VARCHAR NOT NULL,
  physical_file_id VARCHAR NOT NULL,
  min_ts_utc TIMESTAMP,
  max_ts_utc TIMESTAMP,
  row_count BIGINT,
  last_ingest_run_id VARCHAR,
  active BOOLEAN DEFAULT TRUE
);
```

### 1.2 Transactional re‑ingest (only affected segments)

```sql
BEGIN;

-- Remove old contribution for this segment
DELETE FROM logs WHERE segment_id = $1;

-- Bulk insert new rows for the same segment (use prepared/bulk API in code)
-- INSERT INTO logs (...) VALUES (...), (...), ...;

-- Update segment stats
INSERT INTO segments (segment_id, logical_source_id, physical_file_id, min_ts_utc, max_ts_utc, row_count, last_ingest_run_id, active)
VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)
ON CONFLICT (segment_id) DO UPDATE SET
  min_ts_utc = EXCLUDED.min_ts_utc,
  max_ts_utc = EXCLUDED.max_ts_utc,
  row_count = EXCLUDED.row_count,
  last_ingest_run_id = EXCLUDED.last_ingest_run_id,
  active = TRUE;

COMMIT;
```

### 1.3 C# sketch — re‑ingest service using DuckDB.NET

```csharp
public sealed class ReingestService
{
    private readonly Func<DuckDbConnection> _connFactory;

    public ReingestService(Func<DuckDbConnection> connFactory) => _connFactory = connFactory;

    public async Task ReingestAsync(
        string segmentId,
        string logicalSourceId,
        string physicalFileId,
        string runId,
        IEnumerable<LogRow> rows,
        DateTimeOffset minTsUtc,
        DateTimeOffset maxTsUtc,
        long rowCount,
        CancellationToken ct)
    {
        using var conn = _connFactory();
        await conn.OpenAsync(ct);
        using var tx = await conn.BeginTransactionAsync(ct);

        // Delete old
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "DELETE FROM logs WHERE segment_id = ?";
            cmd.Parameters.Add(new DuckDbParameter{ Value = segmentId });
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // Bulk insert (illustrative; for throughput prefer COPY FROM or appender API)
        using (var insert = conn.CreateCommand())
        {
            insert.CommandText = @"
              INSERT INTO logs (timestamp_utc, timestamp_basis, timestamp_effective_offset_minutes, timestamp_original,
                                logical_source_id, source_path, physical_file_id, segment_id, ingest_run_id, record_index,
                                level, message, fields)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
            foreach (var r in rows)
            {
                insert.Parameters.Clear();
                insert.Parameters.AddRange(new[]
                {
                    new DuckDbParameter{ Value = r.TimestampUtc.UtcDateTime },
                    new DuckDbParameter{ Value = r.TimestampBasis },
                    new DuckDbParameter{ Value = r.EffectiveOffsetMinutes },
                    new DuckDbParameter{ Value = r.OriginalTimestamp },
                    new DuckDbParameter{ Value = r.LogicalSourceId },
                    new DuckDbParameter{ Value = r.SourcePath },
                    new DuckDbParameter{ Value = r.PhysicalFileId },
                    new DuckDbParameter{ Value = r.SegmentId },
                    new DuckDbParameter{ Value = r.IngestRunId },
                    new DuckDbParameter{ Value = r.RecordIndex },
                    new DuckDbParameter{ Value = r.Level },
                    new DuckDbParameter{ Value = r.Message },
                    new DuckDbParameter{ Value = r.FieldsJson }
                });
                await insert.ExecuteNonQueryAsync(ct);
            }
        }

        // Upsert segment stats
        using (var upsert = conn.CreateCommand())
        {
            upsert.CommandText = @"
              INSERT INTO segments (segment_id, logical_source_id, physical_file_id, min_ts_utc, max_ts_utc, row_count, last_ingest_run_id, active)
              VALUES (?,?,?,?,?,?,?, TRUE)
              ON CONFLICT (segment_id) DO UPDATE SET
                min_ts_utc = EXCLUDED.min_ts_utc,
                max_ts_utc = EXCLUDED.max_ts_utc,
                row_count = EXCLUDED.row_count,
                last_ingest_run_id = EXCLUDED.last_ingest_run_id,
                active = TRUE";
            upsert.Parameters.AddRange(new[]
            {
                new DuckDbParameter{ Value = segmentId },
                new DuckDbParameter{ Value = logicalSourceId },
                new DuckDbParameter{ Value = physicalFileId },
                new DuckDbParameter{ Value = minTsUtc.UtcDateTime },
                new DuckDbParameter{ Value = maxTsUtc.UtcDateTime },
                new DuckDbParameter{ Value = rowCount },
                new DuckDbParameter{ Value = runId },
            });
            await upsert.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
    }
}

public sealed record LogRow(
    DateTimeOffset TimestampUtc,
    string? TimestampBasis,
    int? EffectiveOffsetMinutes,
    string? OriginalTimestamp,
    string LogicalSourceId,
    string SourcePath,
    string PhysicalFileId,
    string SegmentId,
    string IngestRunId,
    long RecordIndex,
    string? Level,
    string? Message,
    string? FieldsJson);
```

***

## 2) **Ingestion & Detection**

### 2.1 Core contracts and models

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

public abstract record RecordBoundarySpec;

public sealed record CsvBoundary(char Delimiter, bool HasHeader) : RecordBoundarySpec;
public sealed record JsonNdBoundary(bool MultilineObjects) : RecordBoundarySpec;
public sealed record TextSoRBoundary(Regex StartRegex, bool Anchored) : RecordBoundarySpec;

public interface IRecordReader : IDisposable
{
    bool TryReadNext(out RawRecord raw);
}

public sealed record RawRecord(
    string FirstLine,
    string FullText,
    IReadOnlyDictionary<string,string>? Fields = null);

public interface ITimestampExtractor
{
    bool TryExtract(RawRecord raw, out DateTimeOffset ts);
    string Description { get; }
}
```

### 2.2 Multiline text reader (SoR state machine)

```csharp
public sealed class TextRecordReader : IRecordReader
{
    private readonly TextReader _reader;
    private readonly Regex _startRegex;
    private readonly bool _anchored;
    private readonly int _maxLookahead;
    private string? _pushback;

    public TextRecordReader(TextReader reader, Regex startRegex, bool anchored, int maxLookahead = 4096)
    {
        _reader = reader;
        _startRegex = startRegex;
        _anchored = anchored;
        _maxLookahead = maxLookahead;
    }

    public bool TryReadNext(out RawRecord rec)
    {
        var buf = new List<string>(8);
        string? line;

        // Seek next start-of-record
        if (_pushback != null)
        {
            line = _pushback; _pushback = null;
            if (IsStart(line)) buf.Add(line);
        }
        if (buf.Count == 0)
        {
            while ((line = _reader.ReadLine()) != null)
                if (IsStart(line)) { buf.Add(line); break; }
        }
        if (buf.Count == 0) { rec = default!; return false; }

        long lines = 0;
        while ((line = _reader.ReadLine()) != null)
        {
            if (IsStart(line))
            {
                _pushback = line;
                var full = string.Join('\n', buf);
                rec = new RawRecord(buf[0], full);
                return true;
            }
            buf.Add(line);
            if (++lines > _maxLookahead) break; // guardrail
        }

        rec = new RawRecord(buf[0], string.Join('\n', buf));
        return true;
    }

    private bool IsStart(string line) =>
        _anchored ? _startRegex.IsMatch(line) : _startRegex.Match(line).Success;

    public void Dispose() { /* no-op */ }
}
```

### 2.3 Timestamp extractor (regex group + formats + epoch fallback)

```csharp
public sealed class RegexGroupTsExtractor : ITimestampExtractor
{
    private readonly Regex _re;
    private readonly string _group;
    private readonly string[] _formats;
    private readonly IFormatProvider _culture = CultureInfo.InvariantCulture;

    public RegexGroupTsExtractor(Regex re, string group, params string[] formats)
    {
        _re = re; _group = group;
        _formats = formats is { Length: >0 } ? formats
                   : new[] { "o", "yyyy-MM-dd HH:mm:ss,fff", "yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-ddTHH:mm:ss.fffffffK" };
    }

    public bool TryExtract(RawRecord raw, out DateTimeOffset dto)
    {
        var m = _re.Match(raw.FirstLine);
        if (!m.Success) { dto = default; return false; }

        var s = m.Groups[_group].Value;

        // Try with offset first (Z / ±HH:mm)
        if (DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out dto))
            return true;

        // Try common exact patterns without offset
        if (DateTime.TryParseExact(s, _formats, _culture, DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.NoCurrentDateDefault, out var dt))
        {
            dto = new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Local)).ToUniversalTime();
            return true;
        }

        // Epoch fallback
        if (long.TryParse(s, out var num))
        {
            dto = s.Length >= 13 ? DateTimeOffset.FromUnixTimeMilliseconds(num)
                                 : DateTimeOffset.FromUnixTimeSeconds(num);
            return true;
        }

        dto = default;
        return false;
    }

    public string Description => "Regex group timestamp";
}
```

### 2.4 CSV composite timestamp extractor (e.g., Date + Time + ms)

```csharp
public sealed class CompositeCsvTsExtractor : ITimestampExtractor
{
    private readonly string[] _fields;     // e.g., ["Date","Time","ms"]
    private readonly TimeBasisConfig _basis;

    public CompositeCsvTsExtractor(string[] fields, TimeBasisConfig basis)
    { _fields = fields; _basis = basis; }

    public bool TryExtract(RawRecord raw, out DateTimeOffset dto)
    {
        if (raw.Fields is null) { dto = default; return false; }

        var parts = _fields.Select(f => raw.Fields.TryGetValue(f, out var v) ? v : null).ToArray();
        if (parts.Any(string.IsNullOrEmpty)) { dto = default; return false; }

        var s = string.Join(' ', parts!);

        if (DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out dto))
            return true;

        if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var dt))
        {
            dto = ApplyTimeBasis(DateTime.SpecifyKind(dt, DateTimeKind.Unspecified), _basis).ToUniversalTime();
            return true;
        }

        if (long.TryParse(s, out var num))
        {
            dto = s.Length >= 13 ? DateTimeOffset.FromUnixTimeMilliseconds(num)
                                 : DateTimeOffset.FromUnixTimeSeconds(num);
            return true;
        }

        dto = default;
        return false;
    }

    public string Description => $"Composite {string.Join("+", _fields)}";

    // Time basis helpers
    public enum TimeBasis { Local, Utc, FixedOffset, Zone }
    public readonly record struct TimeBasisConfig(TimeBasis Basis, TimeSpan? FixedOffset = null, string? ZoneId = null);

    public static DateTimeOffset ApplyTimeBasis(DateTime dt, in TimeBasisConfig cfg)
    {
        return cfg.Basis switch
        {
            TimeBasis.Utc => DateTime.SpecifyKind(dt, DateTimeKind.Utc),
            TimeBasis.Local => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Local)),
            TimeBasis.FixedOffset => new DateTimeOffset(dt, cfg.FixedOffset ?? TimeSpan.Zero),
            TimeBasis.Zone => TimeZoneInfo.ConvertTime(new DateTimeOffset(dt, TimeSpan.Zero),
                             TimeZoneInfo.FindSystemTimeZoneById(cfg.ZoneId ?? TimeZoneInfo.Local.Id)),
            _ => throw new ArgumentOutOfRangeException()
        };
    }
}
```

### 2.5 Resilient CSV row reader with resynchronization

```csharp
public sealed class ResilientCsvReader
{
    private readonly TextReader _reader;
    private readonly char _delim;
    private readonly SkipLogger _skip;
    private long _lineNo;

    public ResilientCsvReader(TextReader reader, char delim, SkipLogger skip)
        => (_reader, _delim, _skip) = (reader, delim, skip);

    public IEnumerable<(long line, string[] row)> ReadRows()
    {
        string? line;
        int okStreak = 0, badStreak = 0;
        SkipLogger.SkipSegment? seg = null;

        while ((line = _reader.ReadLine()) != null)
        {
            _lineNo++;
            var row = TryParseRow(line, _delim);

            if (row is null)
            {
                badStreak++;
                if (badStreak == 1)
                    seg = _skip.Begin(null, _lineNo, null, "CsvColumnMismatch", "Row parse failed",
                                      Encoding.UTF8.GetBytes(line.AsSpan()[..Math.Min(line.Length, 128)].ToArray()));
                if (badStreak > 1000) break; // safety
                continue;
            }

            okStreak++;
            if (badStreak > 0 && okStreak >= 5)
            {
                seg?.Close(null, _lineNo - 1, null);
                seg = null; badStreak = 0; okStreak = 0;
            }

            yield return (_lineNo, row);
        }

        seg?.Close(null, _lineNo, null);
    }

    private static string[]? TryParseRow(string line, char delim)
    {
        var cells = new List<string>();
        var sb = new StringBuilder();
        bool inQuotes = false;

        for (int i = 0; i < line.Length; i++)
        {
            var c = line[i];
            if (c == '"') { inQuotes = !inQuotes; continue; }
            if (c == delim && !inQuotes) { cells.Add(sb.ToString()); sb.Clear(); continue; }
            sb.Append(c);
        }
        if (inQuotes) return null; // unclosed quote
        cells.Add(sb.ToString());
        return cells.ToArray();
    }
}
```

### 2.6 Skip Logger API (records damaged regions in `skips`)

```csharp
public interface ISkipSink { void Emit(SkipRow row); }

public sealed class SkipLogger
{
    private readonly ISkipSink _sink;
    private readonly string _sessionId, _fileId, _sourcePath, _profileId;
    private long _segmentSeq;

    public SkipLogger(ISkipSink sink, string sessionId, string fileId, string sourcePath, string profileId)
        => (_sink, _sessionId, _fileId, _sourcePath, _profileId) = (sink, sessionId, fileId, sourcePath, profileId);

    public SkipSegment Begin(long? startOffset, long? startLine, long? startRecordIndex, string reasonCode, string reasonDetail, ReadOnlySpan<byte> samplePrefix)
    {
        var id = Interlocked.Increment(ref _segmentSeq);
        return new SkipSegment(_sink, _sessionId, _fileId, _sourcePath, _profileId, id, startOffset, startLine, startRecordIndex, reasonCode, reasonDetail, samplePrefix);
    }

    public readonly struct SkipSegment : IDisposable
    {
        private readonly ISkipSink _sink;
        private readonly string _sessionId, _fileId, _sourcePath, _profileId, _reasonCode, _reasonDetail;
        private readonly long _segmentId;
        private readonly long? _startOffset, _startLine, _startRecordIdx;
        private readonly byte[] _sample;
        private bool _disposed;

        internal SkipSegment(ISkipSink sink, string sessionId, string fileId, string sourcePath, string profileId,
            long segmentId, long? startOffset, long? startLine, long? startRecordIdx, string reasonCode, string reasonDetail, ReadOnlySpan<byte> samplePrefix)
        {
            _sink = sink; _sessionId = sessionId; _fileId = fileId; _sourcePath = sourcePath; _profileId = profileId;
            _segmentId = segmentId; _startOffset = startOffset; _startLine = startLine; _startRecordIdx = startRecordIdx;
            _reasonCode = reasonCode; _reasonDetail = reasonDetail; _sample = samplePrefix.ToArray();
            _disposed = false;
        }

        public void Dispose() => Close(null, null, null);

        public void Close(long? endOffset, long? endLine, long? endRecordIndex)
        {
            if (_disposed) return;
            _disposed = true;
            _sink.Emit(new SkipRow
            {
                SessionId = _sessionId,
                FileId = _fileId,
                SourcePath = _sourcePath,
                SegmentId = _segmentId,
                StartOffset = _startOffset, EndOffset = endOffset,
                StartLine = _startLine, EndLine = endLine,
                StartRecordIndex = _startRecordIdx, EndRecordIndex = endRecordIndex,
                ReasonCode = _reasonCode, ReasonDetail = _reasonDetail,
                DetectorProfileId = _profileId,
                SamplePrefix = _sample,
                UtcLoggedAt = DateTimeOffset.UtcNow
            });
        }
    }
}

public sealed class SkipRow
{
    public string SessionId { get; set; } = default!;
    public string FileId { get; set; } = default!;
    public string SourcePath { get; set; } = default!;
    public long SegmentId { get; set; }
    public long? StartOffset { get; set; }
    public long? EndOffset { get; set; }
    public long? StartLine { get; set; }
    public long? EndLine { get; set; }
    public long? StartRecordIndex { get; set; }
    public long? EndRecordIndex { get; set; }
    public string ReasonCode { get; set; } = default!;
    public string ReasonDetail { get; set; } = default!;
    public string DetectorProfileId { get; set; } = default!;
    public byte[] SamplePrefix { get; set; } = Array.Empty<byte>();
    public DateTimeOffset UtcLoggedAt { get; set; }
}
```

***

## 3) **TICK‑style period compiler & SQL emission**

### 3.1 Interfaces and core models

```csharp
public readonly record struct UtcInterval(DateTimeOffset StartUtc, DateTimeOffset EndUtc); // [Start, End)

public sealed record TickContext(
    DateTimeOffset NowUtc,
    string? DefaultTimeZoneId,
    CultureInfo Culture,
    bool AllowWorkdayFilter);

public sealed record TickCompileResult(
    IReadOnlyList<UtcInterval> Intervals,
    string? NormalizedTick,
    string? Warning);

// Compile 'tick' string → merged UTC intervals
public interface ITickCompiler
{
    TickCompileResult Compile(string tick, TickContext ctx);
}

// Emit SQL for intervals (OR chain or temp table)
public interface IIntervalSqlEmitter
{
    (string SqlPredicate, List<object> Parameters) EmitOrChain(IReadOnlyList<UtcInterval> intervals);
    Task PrepareTempTableAsync(IEnumerable<UtcInterval> intervals, CancellationToken ct);
}
```

### 3.2 Example compiler skeleton (outline)

```csharp
public sealed class TickCompiler : ITickCompiler
{
    public TickCompileResult Compile(string tick, TickContext ctx)
    {
        // Pseudocode:
        // 1) Parse → AST (dates, ranges, T..., @tz, ;duration, $now/$today +/-)
        // 2) Expand bracket lists/ranges in target time zone (TimeZoneInfo/NodaTime)
        // 3) Build [start,end) per anchor (apply duration or whole-day range)
        // 4) Convert endpoints to UTC (resolve DST: ambiguous → post-transition; invalid → shift forward)
        // 5) Merge overlaps/adjacents
        // 6) Return merged intervals + normalized tick text
        return new TickCompileResult(Array.Empty<UtcInterval>(), null, "TODO");
    }
}
```

### 3.3 SQL emission strategies

**Small interval set (≤ 64): OR‑chain (parameterized)**

```csharp
public (string SqlPredicate, List<object> Parameters) EmitOrChain(IReadOnlyList<UtcInterval> intervals)
{
    var sb = new StringBuilder();
    var pars = new List<object>(intervals.Count * 2);
    for (int i = 0; i < intervals.Count; i++)
    {
        if (i > 0) sb.Append(" OR ");
        sb.Append("(l.timestamp_utc >= ? AND l.timestamp_utc < ?)");
        pars.Add(intervals[i].StartUtc.UtcDateTime);
        pars.Add(intervals[i].EndUtc.UtcDateTime);
    }
    return (sb.ToString(), pars);
}
```

**Large interval set: temp table + `EXISTS`**

```sql
CREATE TEMP TABLE _q_intervals(start_ts TIMESTAMP, end_ts TIMESTAMP);
-- bulk insert N rows (StartUtc, EndUtc)

SELECT l.*
FROM logs l
WHERE EXISTS (
  SELECT 1 FROM _q_intervals i
  WHERE l.timestamp_utc >= i.start_ts AND l.timestamp_utc < i.end_ts
)
ORDER BY l.timestamp_utc, l.segment_id, l.record_index
LIMIT ?;
```

***

## 4) **Query, Pagination, Timeline & Facets**

### 4.1 Keyset pagination (forward / backward)

```sql
-- Forward page
SELECT /* projected columns */
FROM logs
WHERE timestamp_utc >= ? AND timestamp_utc < ?
  AND logical_source_id IN (?, ?, ...)
  /* + more predicates from facets/DSL */
  AND (timestamp_utc, segment_id, record_index) > (?, ?, ?)
ORDER BY timestamp_utc, segment_id, record_index
LIMIT ?;

-- Backward page (reverse comparator/order)
SELECT /* columns */
FROM logs
WHERE timestamp_utc >= ? AND timestamp_utc < ?
  AND logical_source_id IN (?, ?, ...)
  AND (timestamp_utc, segment_id, record_index) < (?, ?, ?)
ORDER BY timestamp_utc DESC, segment_id DESC, record_index DESC
LIMIT ?;
```

### 4.2 RowPager + IVirtualRows (Avalonia view model backing)

```csharp
public interface IVirtualRows
{
    ValueTask<ReadOnlyMemory<Row>> GetPageAsync(PageAnchor anchor, int pageSize, CancellationToken ct);
    PageAnchor AnchorAtTop { get; }
    PageAnchor AnchorAtBottom { get; }
}

public readonly record struct PageAnchor(DateTimeOffset Ts, string SegmentId, long RecordIndex, bool FromTop);

public sealed class RowPager : IVirtualRows
{
    private readonly IDbSession _db;
    private readonly LruCache<PageAnchor, Row[]> _cache = new(16);
    private QueryPlan _plan;
    private PageAnchor _top, _bottom;

    public RowPager(IDbSession db) => _db = db;

    public async ValueTask ResetAsync(QueryPlan plan, CancellationToken ct)
    {
        _plan = plan;
        _cache.Clear();
        var first = await FetchAsync(PageAnchorStart(), ct);
        _top = first.AnchorTop;
        _bottom = first.AnchorBottom;
    }

    public async ValueTask<ReadOnlyMemory<Row>> GetPageAsync(PageAnchor anchor, int size, CancellationToken ct)
    {
        if (_cache.TryGetValue(anchor, out var block)) return block;
        var block2 = await FetchAsync(anchor, ct);
        _cache.Put(block2.Anchor, block2.Rows);
        return block2.Rows;
    }

    private async Task<PageBlock> FetchAsync(PageAnchor anchor, CancellationToken ct)
    {
        using var cmd = _db.CreateCommand(_plan.SqlFor(anchor));
        _plan.BindParameters(cmd, anchor);
        using var rdr = await cmd.ExecuteReaderAsync(ct);
        var rows = await rdr.ReadAllRowsAsync(ct);
        var newAnchor = Advance(anchor, rows);
        return new PageBlock(newAnchor, rows);
    }

    public PageAnchor AnchorAtTop => _top;
    public PageAnchor AnchorAtBottom => _bottom;

    private static PageAnchor PageAnchorStart() => new(default, "", -1, true);

    private static PageAnchor Advance(PageAnchor anchor, Row[] rows)
    {
        if (rows.Length == 0) return anchor;
        var last = rows[^1];
        return new PageAnchor(last.TimestampUtc, last.SegmentId, last.RecordIndex, anchor.FromTop);
    }

    private sealed record PageBlock(PageAnchor Anchor, Row[] Rows)
    {
        public PageAnchor AnchorTop => Anchor;
        public PageAnchor AnchorBottom => Anchor;
    }
}

public sealed record Row(DateTimeOffset TimestampUtc, string SegmentId, long RecordIndex /* + projected cols */);

public interface IDbSession
{
    DbCommand CreateCommand(string sql);
}
```

### 4.3 Timeline histogram queries (server‑side bins)

```sql
-- Choose bin granularity at runtime (e.g., 'minute', 'hour', 'day')
SELECT date_trunc($bin, timestamp_utc) AS bucket, COUNT(*) AS n
FROM logs l
WHERE /* time window */ timestamp_utc >= ? AND timestamp_utc < ?
  AND /* same interval predicate from TICK / temp table if used */
  AND /* facet predicates */
GROUP BY 1
ORDER BY 1;
```

(Alternatively, `time_bucket(INTERVAL '5 minutes', timestamp_utc)` if you prefer bucketed semantics.)

### 4.4 Facet counts (top‑K)

```sql
-- Levels facet (top-K within current predicates)
SELECT level, COUNT(*) AS n
FROM logs
WHERE timestamp_utc >= ? AND timestamp_utc < ?
  AND /* same predicates (sources, DSL, intervals) */
GROUP BY level
ORDER BY n DESC NULLS LAST
LIMIT 50;

-- Sources facet (flat)
SELECT logical_source_id, COUNT(*) AS n
FROM logs
WHERE timestamp_utc >= ? AND timestamp_utc < ?
  AND /* predicates */
GROUP BY logical_source_id
ORDER BY n DESC
LIMIT 200;
```

***

## 5) **Avalonia UI skeletons**

### 5.1 XAML layout (conceptual)

```xml
<Grid ColumnDefinitions="auto,*" RowDefinitions="auto,*">
  <!-- Top bar -->
  <StackPanel Grid.Row="0" Orientation="Horizontal" Spacing="8">
    <TextBox Width="600"
             Watermark="level:Error app:api &quot;timeout&quot;  timestamp IN '$now-1h..$now'"
             Text="{Binding QueryText, UpdateSourceTrigger=PropertyChanged}" />
    <Button Content="Run" Command="{Binding RunQuery}" />
    <ToggleSwitch Content="Live Tail" IsChecked="{Binding LiveTailEnabled}" />
  </StackPanel>

  <!-- Left rail -->
  <StackPanel Grid.Row="1" Grid.Column="0" Width="320">
    <local:TimelineView ViewModel="{Binding Timeline}" Height="180"/>
    <local:FacetView Header="Level" Items="{Binding Facets.Levels}" />
    <local:FacetTreeView Header="Sources" Items="{Binding Facets.SourcesTree}" />
    <Button Content="{Binding SkipsSummary}" Command="{Binding OpenSkips}"/>
  </StackPanel>

  <!-- Main grid -->
  <local:VirtualLogGrid Grid.Row="1" Grid.Column="1"
                        ItemsSource="{Binding VirtualRows}"
                        OnExpandRow="{Binding ExpandRow}" />
</Grid>
```

### 5.2 ViewModel shell

```csharp
public sealed class LogsPageVM : ReactiveObject
{
    public TimelineVM Timeline { get; }
    public FacetsVM Facets { get; }
    public IVirtualRows VirtualRows { get; }

    private readonly QueryPlanner _planner;
    private readonly RowPager _pager;

    private string _queryText = string.Empty;
    public string QueryText { get => _queryText; set { this.RaiseAndSetIfChanged(ref _queryText, value); DebouncedRun(); } }

    public ReactiveCommand<Unit, Unit> RunQuery { get; }
    public bool LiveTailEnabled { get; set; }

    public LogsPageVM(QueryPlanner planner, RowPager pager, TimelineVM timeline, FacetsVM facets)
    {
        _planner = planner; _pager = pager; Timeline = timeline; Facets = facets; VirtualRows = pager;

        RunQuery = ReactiveCommand.CreateFromTask(async () =>
        {
            var plan = _planner.Build(CurrentFiltersFrom(_queryText));
            await _pager.ResetAsync(plan, CancellationToken.None);
            await Timeline.RefreshAsync(plan, CancellationToken.None);
            await Facets.RefreshAsync(plan, CancellationToken.None);
        });
    }

    private void DebouncedRun() { /* throttle + invoke RunQuery */ }
}
```

***

## 6) **Regex library (SoR patterns for text logs)**

> Use **source‑generated** regex where possible (AOT‑friendly).

```csharp
// ISO-8601 / RFC3339 with fractional and Z/offset
[GeneratedRegex(@"^(?<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,9})?(?:Z|[+\-]\d{2}:\d{2})?)",
    RegexOptions.Compiled | RegexOptions.CultureInvariant)]
private static partial Regex IsoStartRegex();

// Common timestamp with comma millis
[GeneratedRegex(@"^(?<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[,\.]\d{1,6})",
    RegexOptions.Compiled | RegexOptions.CultureInvariant)]
private static partial Regex YmdHmsFracRegex();

// Syslog-like
[GeneratedRegex(@"^(?<ts>[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})",
    RegexOptions.Compiled | RegexOptions.CultureInvariant)]
private static partial Regex SyslogRegex();

// Apache-style with [..]
[GeneratedRegex(@"^\[(?<ts>\d{2}/[A-Z][a-z]{2}/\d{4}:\d{2}:\d{2}:\d{2}\s[+\-]\d{4})\]",
    RegexOptions.Compiled | RegexOptions.CultureInvariant)]
private static partial Regex ApacheRegex();

// Epoch seconds/millis (anchored)
[GeneratedRegex(@"^(?<ts>\d{10})(?:\.\d{1,6})?$",
    RegexOptions.Compiled | RegexOptions.CultureInvariant)]
private static partial Regex EpochSecondsRegex();

[GeneratedRegex(@"^(?<ts>\d{13})$",
    RegexOptions.Compiled | RegexOptions.CultureInvariant)]
private static partial Regex EpochMillisRegex();
```

***

## 7) **Timezone policy helper** (for bare timestamps)

```csharp
public enum TimeBasis { Local, Utc, FixedOffset, Zone }
public readonly record struct TimeBasisConfig(TimeBasis Basis, TimeSpan? FixedOffset = null, string? ZoneId = null);

public static DateTimeOffset ApplyTimeBasis(DateTime dt, in TimeBasisConfig cfg)
{
    return cfg.Basis switch
    {
        TimeBasis.Utc         => DateTime.SpecifyKind(dt, DateTimeKind.Utc),
        TimeBasis.Local       => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Local)),
        TimeBasis.FixedOffset => new DateTimeOffset(dt, cfg.FixedOffset ?? TimeSpan.Zero),
        TimeBasis.Zone        => TimeZoneInfo.ConvertTime(new DateTimeOffset(dt, TimeSpan.Zero),
                                TimeZoneInfo.FindSystemTimeZoneById(cfg.ZoneId ?? TimeZoneInfo.Local.Id)),
        _ => throw new ArgumentOutOfRangeException()
    };
}
```

***

## 8) **Example queries** (hide/show, drill, histogram)

```sql
-- Hide/show sources quickly
SELECT timestamp_utc, logical_source_id, level, message
FROM logs
WHERE timestamp_utc BETWEEN ? AND ?
  AND logical_source_id IN (?, ?, ...)
ORDER BY timestamp_utc, segment_id, record_index
LIMIT ?;

-- Focus on errors for app=api (assuming 'app' lives in fields JSON)
SELECT timestamp_utc, logical_source_id, level, message
FROM logs
WHERE timestamp_utc BETWEEN ? AND ?
  AND level = 'Error'
  AND json_extract_string(fields, '$.app') = 'api'
ORDER BY timestamp_utc DESC
LIMIT 500;

-- Histogram per minute for visible window
SELECT date_trunc('minute', timestamp_utc) AS bucket, COUNT(*) AS n
FROM logs
WHERE timestamp_utc BETWEEN ? AND ?
GROUP BY 1
ORDER BY 1;
```

***

## 10) **Misc. utilities**

### 10.1 GeneratedRegex example usage in SoR

```csharp
[GeneratedRegex(@"^(?<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,6})?(?:Z|[+\-]\d{2}:\d{2})?)",
    RegexOptions.Compiled | RegexOptions.CultureInvariant)]
private static partial Regex IsoStartRegex();

bool IsStart(ReadOnlySpan<char> line) => IsoStartRegex().IsMatch(line.ToString());
```

***

## 11) **How to wire these together**

1. **Detection** picks a `RecordBoundarySpec` + `ITimestampExtractor`.
2. **RecordReader** yields `RawRecord`; extractor gives `DateTimeOffset`.
3. **Ingestion** builds `LogRow` batches → transactional `INSERT` into DuckLake’s `logs`.
4. **Skips** go into `skips`.
5. **QueryPlanner**:
    * Parse user DSL & **TICK** (`timestamp IN '...'`) → compile to UTC intervals.
    * Build keyset SQL + predicates; for many intervals, use **temp intervals** table.
6. **RowPager** drives the **virtual grid**; **Timeline** and **Facets** are just SQL with the **same predicates**.

***
