using DuckDB.NET.Data;

using ItomoriLog.Core.Ingest.Readers;
using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Ingest;

/// <summary>
/// Re-ingests a single segment within a transaction. On failure the transaction is
/// rolled back so existing data is never corrupted.
/// </summary>
public sealed class ReingestService
{
    private readonly DuckDBConnection _connection;
    private readonly DetectionEngine _detectionEngine;

    public ReingestService(DuckDBConnection connection, DetectionEngine? detectionEngine = null)
    {
        _connection = connection;
        _detectionEngine = detectionEngine ?? new DetectionEngine();
    }

    public async Task<ReingestResult> ReingestSegmentAsync(
        string segmentId,
        TimeBasisConfig defaultTimeBasis,
        CancellationToken ct = default,
        FileFormatOverride? formatOverride = null)
    {
        // 1. Load segment metadata
        var segment = await LoadSegmentAsync(segmentId, ct);
        if (segment is null)
            return ReingestResult.Failed(segmentId, "Segment not found");

        // 2. Resolve source path from existing log rows
        var sourcePath = await ResolveSourcePathAsync(segmentId, ct);
        if (sourcePath is null)
            return ReingestResult.Failed(segmentId, "No source path found for segment");

        var canonicalSourcePath = SourcePathHelper.Normalize(sourcePath);
        Func<Stream> reopenSourceStream;
        long sourceSizeBytes;
        DateTimeOffset sourceLastModifiedUtc;
        string sourceName;

        if (SourcePathHelper.TrySplitArchiveEntry(canonicalSourcePath, out var archivePath, out var entryFullName)) {
            if (!File.Exists(archivePath))
                return ReingestResult.Failed(segmentId, $"Source archive not found: {archivePath}");

            if (!ZipHandler.TryGetEntry(archivePath, entryFullName, out var zipEntry))
                return ReingestResult.Failed(segmentId, $"Source archive entry not found: {entryFullName}");

            reopenSourceStream = () => ZipHandler.OpenRead(archivePath, zipEntry.EntryName);
            sourceSizeBytes = zipEntry.SizeBytes;
            sourceLastModifiedUtc = new DateTimeOffset(File.GetLastWriteTimeUtc(archivePath), TimeSpan.Zero);
            sourceName = Path.GetFileName(zipEntry.EntryName);
        } else {
            if (!File.Exists(canonicalSourcePath))
                return ReingestResult.Failed(segmentId, $"Source file not found: {canonicalSourcePath}");

            reopenSourceStream = () => File.OpenRead(canonicalSourcePath);
            var fileInfo = new FileInfo(canonicalSourcePath);
            sourceSizeBytes = fileInfo.Length;
            sourceLastModifiedUtc = new DateTimeOffset(fileInfo.LastWriteTimeUtc, TimeSpan.Zero);
            sourceName = Path.GetFileName(canonicalSourcePath);
        }

        await using var sourceStream = reopenSourceStream();
        var sampleBytes = await StreamSampling.ReadPrefixAsync(sourceStream, 256 * 1024, ct);

        // 3. Re-detect format from source file
        var detection = formatOverride?.Detection;
        if (detection is null) {
            using var detectionStream = new MemoryStream(sampleBytes, writable: false);
            var engineResult = _detectionEngine.Detect(detectionStream, sourceName);
            if (engineResult.Detection is null)
                return ReingestResult.Failed(segmentId, "Format could not be detected on re-ingest");
            detection = engineResult.Detection;
        }

        // 4. Read all records
        using var encodingStream = new MemoryStream(sampleBytes, writable: false);
        var encoding = formatOverride?.EncodingOverride ?? EncodingDetector.Detect(encodingStream);
        using var replayStream = new ReplayPrefixStream(sampleBytes, sourceStream, leaveInnerOpen: true);
        var textReader = new StreamReader(replayStream, encoding, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
        var skipSink = new ListSkipSink();
        var skipLogger = new SkipLogger(skipSink, segment.LogicalSourceId, segment.PhysicalFileId, segmentId);
        using var recordReader = CreateReader(detection.Boundary, textReader, skipLogger);

        var rows = new List<LogRow>();
        long recordIndex = 0;
        var synthesizer = new FieldSynthesizer();

        var tracker = new IngestRunTracker(_connection);
        var runId = await tracker.StartRunAsync(ct);

        var effectiveTimeBasis = formatOverride?.TimeBasisOverride ?? defaultTimeBasis;

        while (recordReader.TryReadNext(out var raw)) {
            ct.ThrowIfCancellationRequested();

            if (!TimestampResolver.TryResolve(detection.Extractor, raw, effectiveTimeBasis, out var resolvedTimestamp)) {
                var seg = skipLogger.BeginSkip(SkipReasonCode.TimeParse,
                    "Timestamp extraction failed", startLine: raw.LineNumber);
                seg.Close(endLine: raw.LineNumber);
                continue;
            }

            var utcTimestamp = resolvedTimestamp.UtcTimestamp;
            var offsetMinutes = resolvedTimestamp.EffectiveOffsetMinutes;

            string? level = null;
            string message = raw.FullText;
            string? fieldsJson = null;

            if (detection.Boundary is TextSoRBoundary sor) {
                var tsMatch = sor.StartRegex.Match(raw.FirstLine);
                if (tsMatch.Success) {
                    var postTs = raw.FirstLine[(tsMatch.Index + tsMatch.Length)..];
                    var extracted = synthesizer.Extract(postTs);
                    level = extracted.Level;
                    message = extracted.Message;
                    fieldsJson = extracted.FieldsJson;
                }
            } else if (raw.Fields is not null) {
                raw.Fields.TryGetValue("level", out var lvl);
                raw.Fields.TryGetValue("severity", out var sev);
                level = lvl ?? sev;
                if (level is not null) level = FieldSynthesizer.NormalizeLevel(level);

                raw.Fields.TryGetValue("message", out var msg);
                raw.Fields.TryGetValue("msg", out var msg2);
                if (msg is not null || msg2 is not null)
                    message = msg ?? msg2 ?? raw.FullText;

                var extra = raw.Fields
                    .Where(kv => kv.Key is not "level" and not "severity" and not "message" and not "msg")
                    .ToDictionary(kv => kv.Key, kv => kv.Value);
                if (extra.Count > 0)
                    fieldsJson = System.Text.Json.JsonSerializer.Serialize(extra);
            }

            rows.Add(new LogRow(
                TimestampUtc: utcTimestamp,
                TimestampBasis: resolvedTimestamp.Basis,
                TimestampEffectiveOffsetMinutes: offsetMinutes,
                TimestampOriginal: resolvedTimestamp.TimestampOriginal ?? raw.FirstLine[..Math.Min(raw.FirstLine.Length, 50)],
                LogicalSourceId: segment.LogicalSourceId,
                SourcePath: canonicalSourcePath,
                PhysicalFileId: segment.PhysicalFileId,
                SegmentId: segmentId,
                IngestRunId: runId,
                RecordIndex: recordIndex++,
                Level: level,
                Message: message,
                FieldsJson: fieldsJson));
        }

        // 5. Transactional replace: BEGIN → DELETE → INSERT → UPDATE segment → COMMIT
        try {
            await ExecuteAsync("BEGIN TRANSACTION", ct);

            await DeleteSegmentLogsAsync(segmentId, ct);
            await DeleteSegmentSkipsAsync(segmentId, ct);

            var inserter = new LogBatchInserter(_connection);
            await inserter.InsertBatchAsync(rows, ct);

            var skipInserter = new SkipBatchInserter(_connection);
            await skipInserter.InsertBatchAsync(skipSink.GetSkips(), sessionId: null, ct);

            DateTimeOffset? minTs = rows.Count > 0 ? rows.Min(r => r.TimestampUtc) : null;
            DateTimeOffset? maxTs = rows.Count > 0 ? rows.Max(r => r.TimestampUtc) : null;
            await using var hashStream = reopenSourceStream();
            var fileHash = await FileChangeDetector.ComputeStreamHashAsync(hashStream, ct);
            await UpdateSegmentAsync(
                segmentId,
                runId,
                rows.Count,
                minTs,
                maxTs,
                canonicalSourcePath,
                sourceSizeBytes,
                sourceLastModifiedUtc,
                fileHash,
                sourceSizeBytes,
                ct);

            await ExecuteAsync("COMMIT", ct);
            await tracker.CompleteRunAsync(runId, ct);

            return new ReingestResult(
                SegmentId: segmentId,
                Success: true,
                NewRowCount: rows.Count,
                Skips: skipSink.GetSkips(),
                Error: null);
        } catch (Exception ex) when (ex is not OperationCanceledException) {
            try { await ExecuteAsync("ROLLBACK", ct); } catch { }
            try { await tracker.CompleteRunAsync(runId, ct); } catch { }
            return ReingestResult.Failed(segmentId, $"Transaction failed: {ex.Message}");
        }
    }

    private async Task<SegmentRow?> LoadSegmentAsync(string segmentId, CancellationToken ct)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            SELECT segment_id, logical_source_id, physical_file_id,
                   min_ts_utc, max_ts_utc, row_count, last_ingest_run_id, active,
                   source_path, file_size_bytes, last_modified_utc, file_hash, last_byte_offset
            FROM segments WHERE segment_id = $1
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        return new SegmentRow(
            SegmentId: reader.GetString(0),
            LogicalSourceId: reader.GetString(1),
            PhysicalFileId: reader.GetString(2),
            MinTsUtc: reader.IsDBNull(3) ? null : reader.GetDateTime(3),
            MaxTsUtc: reader.IsDBNull(4) ? null : reader.GetDateTime(4),
            RowCount: reader.GetInt64(5),
            LastIngestRunId: reader.GetString(6),
            Active: reader.GetBoolean(7),
            SourcePath: reader.IsDBNull(8) ? null : reader.GetString(8),
            FileSizeBytes: reader.IsDBNull(9) ? null : reader.GetInt64(9),
            LastModifiedUtc: reader.IsDBNull(10) ? null : reader.GetDateTime(10),
            FileHash: reader.IsDBNull(11) ? null : reader.GetString(11),
            LastByteOffset: reader.IsDBNull(12) ? null : reader.GetInt64(12));
    }

    private async Task<string?> ResolveSourcePathAsync(string segmentId, CancellationToken ct)
    {
        var segment = await LoadSegmentAsync(segmentId, ct);
        if (segment?.SourcePath is not null)
            return segment.SourcePath;

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT source_path FROM logs WHERE segment_id = $1 LIMIT 1";
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        var result = await cmd.ExecuteScalarAsync(ct);
        return result as string;
    }

    private async Task DeleteSegmentLogsAsync(string segmentId, CancellationToken ct)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "DELETE FROM logs WHERE segment_id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task DeleteSegmentSkipsAsync(string segmentId, CancellationToken ct)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "DELETE FROM skips WHERE segment_id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task UpdateSegmentAsync(
        string segmentId, string runId, long rowCount,
        DateTimeOffset? minTs, DateTimeOffset? maxTs,
        string sourcePath,
        long fileSizeBytes,
        DateTimeOffset lastModifiedUtc,
        string fileHash,
        long lastByteOffset,
        CancellationToken ct)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            UPDATE segments SET
                row_count = $1,
                min_ts_utc = $2,
                max_ts_utc = $3,
                last_ingest_run_id = $4,
                source_path = $5,
                file_size_bytes = $6,
                last_modified_utc = $7,
                file_hash = $8,
                last_byte_offset = $9
            WHERE segment_id = $10
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = rowCount });
        cmd.Parameters.Add(new DuckDBParameter { Value = minTs.HasValue ? (object)minTs.Value.UtcDateTime : DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = maxTs.HasValue ? (object)maxTs.Value.UtcDateTime : DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = runId });
        cmd.Parameters.Add(new DuckDBParameter { Value = sourcePath });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileSizeBytes });
        cmd.Parameters.Add(new DuckDBParameter { Value = lastModifiedUtc.UtcDateTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = lastByteOffset });
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task ExecuteAsync(string sql, CancellationToken ct)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static IRecordReader CreateReader(
        RecordBoundarySpec boundary, TextReader textReader, SkipLogger skipLogger)
    {
        return boundary switch {
            TextSoRBoundary sor => new TextRecordReader(textReader, sor.StartRegex),
            CsvBoundary csv => new CsvRecordReader(textReader, csv, skipLogger),
            JsonNdBoundary json => new NdjsonRecordReader(textReader, json, skipLogger),
            _ => throw new NotSupportedException($"Unknown boundary type: {boundary.GetType().Name}")
        };
    }
}

public sealed record ReingestResult(
    string SegmentId,
    bool Success,
    long NewRowCount,
    IReadOnlyList<SkipRow> Skips,
    string? Error)
{
    public static ReingestResult Failed(string segmentId, string error) =>
        new(segmentId, false, 0, [], error);
}
