using DuckDB.NET.Data;

using ItomoriLog.Core.Ingest.Readers;
using ItomoriLog.Core.Model;

using System.Buffers;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace ItomoriLog.Core.Ingest;

public sealed class IngestOrchestrator
{
    private readonly DuckDBConnection _connection;
    private readonly DetectionEngine _detectionEngine;
    private readonly int _maxConcurrency;
    private readonly int _batchChannelCapacity;
    private readonly int _batchesPerTransaction;
    private readonly IReadOnlyDictionary<string, FileFormatOverride> _formatOverrides;

    public IngestOrchestrator(
        DuckDBConnection connection,
        DetectionEngine? detectionEngine = null,
        int maxConcurrency = 2,
        int batchChannelCapacity = 16,
        int batchesPerTransaction = 2,
        IReadOnlyDictionary<string, FileFormatOverride>? formatOverrides = null)
    {
        _connection = connection;
        _detectionEngine = detectionEngine ?? new DetectionEngine();
        _maxConcurrency = (-1 == maxConcurrency) ? Environment.ProcessorCount : maxConcurrency;
        _batchChannelCapacity = batchChannelCapacity;
        _batchesPerTransaction = batchesPerTransaction;
        _formatOverrides = formatOverrides ?? new Dictionary<string, FileFormatOverride>(StringComparer.OrdinalIgnoreCase);
    }

    public async Task<IngestResult> IngestFilesAsync(
        IReadOnlyList<string> filePaths,
        TimeBasisConfig defaultTimeBasis,
        IProgress<IngestProgressUpdate>? progress = null,
        IProgress<IngestVisibilityUpdate>? visibility = null,
        CancellationToken ct = default)
    {
        var tracker = new IngestRunTracker(_connection);
        var runId = await tracker.StartRunAsync(ct);
        await tracker.RegisterSourcesAsync(
            runId,
            filePaths
                .Select(SourcePathHelper.Normalize)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray(),
            ct);

        var skipSink = new ListSkipSink();
        var segmentRows = new List<SegmentUpsertRow>();
        var totalRows = 0L;
        var filesProcessed = 0;
        var entries = new List<FileToIngest>();

        try {
            // Expand ZIP files
            entries.Clear();
            foreach (var path in filePaths) {
                var canonicalPath = CanonicalizeSourcePath(path);
                if (SourcePathHelper.TrySplitArchiveEntry(canonicalPath, out var archivePath, out var entryFullName)) {
                    if (!ZipHandler.TryGetEntry(archivePath, entryFullName, out var zipEntry))
                        throw new FileNotFoundException($"Entry not found: {entryFullName}", archivePath);

                    var capturedArchivePath = archivePath;
                    var capturedEntryName = zipEntry.EntryName;
                    entries.Add(new FileToIngest(
                        zipEntry.SourcePath,
                        capturedEntryName,
                        () => ZipHandler.OpenRead(capturedArchivePath, capturedEntryName),
                        isZipEntry: true,
                        initialSizeBytes: zipEntry.SizeBytes));
                } else if (SourcePathHelper.IsArchiveFilePath(canonicalPath)) {
                    foreach (var zipEntry in ZipHandler.EnumerateEntries(canonicalPath, skipSink)) {
                        var capturedPath = canonicalPath;
                        var capturedEntryName = zipEntry.EntryName;
                        entries.Add(new FileToIngest(
                            zipEntry.SourcePath,
                            capturedEntryName,
                            () => ZipHandler.OpenRead(capturedPath, capturedEntryName),
                            isZipEntry: true,
                            initialSizeBytes: zipEntry.SizeBytes));
                    }
                } else {
                    var capturedPath = canonicalPath;
                    var info = new FileInfo(capturedPath);
                    entries.Add(new FileToIngest(
                        capturedPath,
                        Path.GetFileName(capturedPath),
                        () => File.OpenRead(capturedPath),
                        isZipEntry: false,
                        initialSizeBytes: info.Exists ? info.Length : 0));
                }
            }

            foreach (var entry in entries) {
                progress?.Report(new IngestProgressUpdate(
                    SourcePath: CanonicalizeSourcePath(entry.SourcePath),
                    Phase: IngestFilePhase.Queued,
                    BytesProcessed: 0,
                    BytesTotal: entry.initialSizeBytes,
                    RecordsProcessed: 0,
                    Message: "Queued"));
            }

            // Channel for batch writing
            var channel = Channel.CreateBounded<IReadOnlyList<LogRow>>(
                new BoundedChannelOptions(_batchChannelCapacity) {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true
                });

            // Consumer task: writes batches to DB.
            // Batches are accumulated up to _batchesPerTransaction (default 5 × 50 k = 250 k rows)
            // before a single COMMIT.  Visibility is reported AFTER commit so the
            // connection is free when the UI triggers a live browse refresh.
            var writerTask = Task.Run(async () => {
                var inserter = new LogBatchInserter(_connection);
                var pending = new List<IReadOnlyList<LogRow>>(_batchesPerTransaction);

                async Task CommitPendingAsync(CancellationToken token)
                {
                    if (pending.Count == 0) return;
                    await ExecuteInTransactionAsync(async t => {
                        foreach (var b in pending)
                            await inserter.InsertBatchAsync(b, t);
                    }, token);

                    // Aggregate stats and report AFTER commit — the connection is now
                    // free and the data is actually queryable by the live browse refresh.
                    var committed = 0;
                    string? lastSourcePath = null;
                    foreach (var b in pending) {
                        committed += b.Count;
                        if (b.Count > 0) lastSourcePath = b[0].SourcePath;
                    }
                    pending.Clear();

                    var totalCommitted = Interlocked.Add(ref totalRows, committed);
                    if (visibility is not null && committed > 0 && lastSourcePath is not null) {
                        visibility.Report(new IngestVisibilityUpdate(
                            SourcePath: lastSourcePath,
                            RowsCommitted: committed,
                            TotalRowsCommitted: totalCommitted));
                    }
                }

                await foreach (var batch in channel.Reader.ReadAllAsync(ct)) {
                    pending.Add(batch);

                    if (pending.Count >= _batchesPerTransaction)
                        await CommitPendingAsync(ct);
                }

                await CommitPendingAsync(ct);
            }, ct);

            // Producer tasks: detect + read + extract per file.
            // Wrapped in Task.Run so the record-parsing loops execute on thread
            // pool threads instead of the caller's SynchronizationContext (the UI
            // thread).  Without this, a fast consumer means WriteAsync rarely
            // blocks, so the synchronous parsing loop never yields the UI thread.
            var semaphore = new SemaphoreSlim(_maxConcurrency);
            var producerTasks = entries.Select(entry => Task.Run(async () => {
                await semaphore.WaitAsync(ct);
                try {
                    var segmentRow = await ProcessFileAsync(
                        entry,
                        runId,
                        defaultTimeBasis,
                        skipSink,
                        channel.Writer,
                        progress,
                        ct);
                    if (segmentRow is not null) {
                        lock (segmentRows) {
                            segmentRows.Add(segmentRow);
                        }
                    }
                    Interlocked.Increment(ref filesProcessed);
                } finally {
                    semaphore.Release();
                }
            }, ct)).ToArray();

            await Task.WhenAll(producerTasks);
            channel.Writer.Complete();
            await writerTask;

            var snapshotSegments = segmentRows.ToArray();
            var skipRows = skipSink.GetSkips();
            await ExecuteInTransactionAsync(async token => {
                if (snapshotSegments.Length > 0) {
                    var segmentUpserter = new SegmentUpserter(_connection);
                    await segmentUpserter.UpsertBatchAsync(snapshotSegments, token);
                }

                if (skipRows.Count > 0) {
                    var skipInserter = new SkipBatchInserter(_connection);
                    await skipInserter.InsertBatchAsync(skipRows, sessionId: null, token);
                }

                await tracker.CompleteRunAsync(runId, token);
            }, ct);

            return new IngestResult(
                RunId: runId,
                TotalRows: Interlocked.Read(ref totalRows),
                FilesProcessed: filesProcessed,
                Skips: skipSink.GetSkips(),
                Status: "completed");
        } catch (OperationCanceledException) {
            await CleanupRunDataAsync(runId, CancellationToken.None);
            await tracker.AbandonRunAsync(runId, CancellationToken.None);
            throw;
        } catch (Exception) when (ct.IsCancellationRequested is false) {
            await CleanupRunDataAsync(runId, CancellationToken.None);
            await tracker.FailRunAsync(runId, CancellationToken.None);
            foreach (var entry in entries) {
                progress?.Report(new IngestProgressUpdate(
                    SourcePath: CanonicalizeSourcePath(entry.SourcePath),
                    Phase: IngestFilePhase.Failed,
                    BytesProcessed: 0,
                    BytesTotal: entry.initialSizeBytes,
                    RecordsProcessed: 0,
                    Message: "Ingest failed"));
            }
            return new IngestResult(runId, Interlocked.Read(ref totalRows), filesProcessed,
                skipSink.GetSkips(), "failed");
        }
    }

    private async Task<SegmentUpsertRow?> ProcessFileAsync(
        FileToIngest entry,
        string runId,
        TimeBasisConfig timeBasis,
        ListSkipSink skipSink,
        ChannelWriter<IReadOnlyList<LogRow>> writer,
        IProgress<IngestProgressUpdate>? progress,
        CancellationToken ct)
    {
        await using var sourceStream = entry.OpenStream();
        var canonicalSourcePath = CanonicalizeSourcePath(entry.SourcePath);
        var logicalSourceId = IdentityGenerator.LogicalSourceId(entry.FileName);
        var lastModifiedUtc = ResolveLastModifiedUtc(canonicalSourcePath, entry.isZipEntry);
        var totalBytes = entry.initialSizeBytes;
        var progressReporter = new ProgressReporter(canonicalSourcePath, totalBytes, progress);
        var sampleBytes = await StreamSampling.ReadPrefixAsync(sourceStream, 256 * 1024, ct);

        progressReporter.Report(IngestFilePhase.Sniffing, 0, 0, "Sniffing", force: true);

        // Detect format
        DetectionResult? detection;
        var effectiveTimeBasis = timeBasis;
        FileFormatOverride? formatOverride = null;
        if (!_formatOverrides.TryGetValue(canonicalSourcePath, out formatOverride)) {
            using var detectionStream = new MemoryStream(sampleBytes, writable: false);
            var engineResult = _detectionEngine.Detect(detectionStream, entry.FileName);
            detection = engineResult.Detection;
            if (detection is null) {
                var failedPhysicalFileId = IdentityGenerator.PhysicalFileId(
                    canonicalSourcePath,
                    totalBytes,
                    lastModifiedUtc);
                var failedSegmentId = IdentityGenerator.SegmentId(failedPhysicalFileId);

                skipSink.Write(new SkipRow(
                    LogicalSourceId: logicalSourceId,
                    PhysicalFileId: failedPhysicalFileId,
                    SegmentId: failedSegmentId,
                    SegmentSeq: 0,
                    StartLine: null, EndLine: null, StartOffset: null, EndOffset: null,
                    ReasonCode: SkipReasonCode.NotRecognized,
                    ReasonDetail: $"No viable format detected for {entry.FileName}",
                    SamplePrefix: null, DetectorProfileId: null,
                    UtcLoggedAt: DateTimeOffset.UtcNow));

                var failedSourcePath = canonicalSourcePath;
                var failedFileSize = totalBytes;
                await using var failedHashStream = entry.OpenStream();
                var failedFileHash = await FileChangeDetector.ComputeStreamHashAsync(failedHashStream, ct);

                progressReporter.Report(
                    IngestFilePhase.Skipped,
                    failedFileSize,
                    0,
                    "Not recognized",
                    force: true);

                return new SegmentUpsertRow(
                    SegmentId: failedSegmentId,
                    LogicalSourceId: logicalSourceId,
                    PhysicalFileId: failedPhysicalFileId,
                    MinTsUtc: null,
                    MaxTsUtc: null,
                    RowCount: 0,
                    LastIngestRunId: runId,
                    Active: true,
                    LastByteOffset: 0,
                    SourcePath: failedSourcePath,
                    FileSizeBytes: failedFileSize,
                    LastModifiedUtc: lastModifiedUtc,
                    FileHash: failedFileHash);
            }
        } else {
            detection = formatOverride.Detection;
            if (formatOverride.TimeBasisOverride is not null)
                effectiveTimeBasis = formatOverride.TimeBasisOverride;
        }

        progressReporter.Report(IngestFilePhase.Ingesting, 0, 0, "Ingesting", force: true);

        // Generate identity
        var physicalFileId = IdentityGenerator.PhysicalFileId(
            canonicalSourcePath, totalBytes, lastModifiedUtc);
        var segmentId = IdentityGenerator.SegmentId(physicalFileId);

        var skipLogger = new SkipLogger(skipSink, logicalSourceId, physicalFileId, segmentId);

        // Create reader based on boundary type
        using var encodingStream = new MemoryStream(sampleBytes, writable: false);
        var encoding = formatOverride?.EncodingOverride ?? EncodingDetector.Detect(encodingStream);
        using var replayStream = new ReplayPrefixStream(sampleBytes, sourceStream, leaveInnerOpen: true);
        var textReader = new StreamReader(replayStream, encoding, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
        var byteCounter = BuildByteCounter(encoding);
        using var recordReader = CreateReader(detection.Boundary, textReader, skipLogger, byteCounter);

        // Read and batch records
        var batch = new List<LogRow>(25_000);
        long recordIndex = 0;
        var synthesizer = new FieldSynthesizer();
        DateTimeOffset? minTs = null;
        DateTimeOffset? maxTs = null;
        var fieldsBuffer = new ArrayBufferWriter<byte>(512);
        using var fieldsWriter = new Utf8JsonWriter(fieldsBuffer);

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
            minTs = minTs.HasValue && minTs.Value <= utcTimestamp ? minTs : utcTimestamp;
            maxTs = maxTs.HasValue && maxTs.Value >= utcTimestamp ? maxTs : utcTimestamp;

            // Field synthesis
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
                // CSV/NDJSON — level and message may be in fields
                raw.Fields.TryGetValue("level", out var lvl);
                raw.Fields.TryGetValue("severity", out var sev);
                level = lvl ?? sev;
                if (level is not null) level = FieldSynthesizer.NormalizeLevel(level);

                raw.Fields.TryGetValue("message", out var msg);
                raw.Fields.TryGetValue("msg", out var msg2);
                if (msg is not null || msg2 is not null)
                    message = msg ?? msg2 ?? raw.FullText;

                // Remaining fields as JSON — writer and buffer are reused per record to avoid
                // per-record Dictionary + JsonSerializer allocations.
                fieldsBuffer.ResetWrittenCount();
                fieldsWriter.Reset();
                fieldsWriter.WriteStartObject();
                var extraCount = 0;
                foreach (var kv in raw.Fields) {
                    if (kv.Key is not "level" and not "severity" and not "message" and not "msg") {
                        fieldsWriter.WriteString(kv.Key, kv.Value);
                        extraCount++;
                    }
                }
                fieldsWriter.WriteEndObject();
                fieldsWriter.Flush();
                if (extraCount > 0)
                    fieldsJson = Encoding.UTF8.GetString(fieldsBuffer.WrittenSpan);
            }

            batch.Add(new LogRow(
                TimestampUtc: utcTimestamp,
                TimestampBasis: resolvedTimestamp.Basis,
                TimestampEffectiveOffsetMinutes: offsetMinutes,
                TimestampOriginal: resolvedTimestamp.TimestampOriginal ?? raw.FirstLine[..Math.Min(raw.FirstLine.Length, 50)],
                LogicalSourceId: logicalSourceId,
                SourcePath: canonicalSourcePath,
                PhysicalFileId: physicalFileId,
                SegmentId: segmentId,
                IngestRunId: runId,
                RecordIndex: recordIndex++,
                Level: level,
                Message: message,
                FieldsJson: fieldsJson));

            progressReporter.Report(
                IngestFilePhase.Ingesting,
                Math.Min(raw.EndByteOffset, totalBytes),
                recordIndex,
                "Ingesting");

            if (batch.Count >= 50_000) {
                var readyBatch = batch;
                batch = new List<LogRow>(50_000);
                await writer.WriteAsync(readyBatch, ct);
            }
        }

        // Flush remaining
        if (batch.Count > 0)
            await writer.WriteAsync(batch, ct);

        // Only hash files within the threshold — consistent with FileChangeDetector.DetectAsync —
        // to avoid a full second read of large files.
        string? fileHash = null;
        if (totalBytes <= FileChangeDetector.HashThresholdBytes) {
            await using var hashStream = entry.OpenStream();
            fileHash = await FileChangeDetector.ComputeStreamHashAsync(hashStream, ct);
        }

        progressReporter.Report(
            IngestFilePhase.Completed,
            totalBytes,
            recordIndex,
            "Completed",
            force: true);

        return new SegmentUpsertRow(
            SegmentId: segmentId,
            LogicalSourceId: logicalSourceId,
            PhysicalFileId: physicalFileId,
            MinTsUtc: minTs,
            MaxTsUtc: maxTs,
            RowCount: recordIndex,
            LastIngestRunId: runId,
            Active: true,
            LastByteOffset: totalBytes,
            SourcePath: canonicalSourcePath,
            FileSizeBytes: totalBytes,
            LastModifiedUtc: lastModifiedUtc,
            FileHash: fileHash);
    }

    private static IRecordReader CreateReader(
        RecordBoundarySpec boundary,
        TextReader textReader,
        SkipLogger skipLogger,
        Func<string, long> byteCounter)
    {
        return boundary switch {
            TextSoRBoundary sor => new TextRecordReader(textReader, sor.StartRegex, byteCounter: byteCounter),
            CsvBoundary csv => new CsvRecordReader(textReader, csv, skipLogger, byteCounter: byteCounter),
            JsonNdBoundary json => new NdjsonRecordReader(textReader, json, skipLogger, byteCounter: byteCounter),
            _ => throw new NotSupportedException($"Unknown boundary type: {boundary.GetType().Name}")
        };
    }

    private sealed record FileToIngest(
        string SourcePath,
        string FileName,
        Func<Stream> OpenStream,
        bool isZipEntry,
        long initialSizeBytes);

    private static DateTimeOffset ResolveLastModifiedUtc(string sourcePath, bool isZipEntry)
    {
        if (!isZipEntry && File.Exists(sourcePath))
            return new DateTimeOffset(File.GetLastWriteTimeUtc(sourcePath), TimeSpan.Zero);

        if (SourcePathHelper.TrySplitArchiveEntry(sourcePath, out var archivePath, out _)
            && File.Exists(archivePath)) {
            return new DateTimeOffset(File.GetLastWriteTimeUtc(archivePath), TimeSpan.Zero);
        }

        return DateTimeOffset.UtcNow;
    }

    private static string CanonicalizeSourcePath(string sourcePath)
        => SourcePathHelper.Normalize(sourcePath);

    private async Task ExecuteControlStatementAsync(string sql, CancellationToken ct)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task ExecuteInTransactionAsync(Func<CancellationToken, Task> action, CancellationToken ct)
    {
        await ExecuteControlStatementAsync("BEGIN TRANSACTION", ct);
        try {
            await action(ct);
            await ExecuteControlStatementAsync("COMMIT", ct);
        } catch {
            try {
                await ExecuteControlStatementAsync("ROLLBACK", CancellationToken.None);
            } catch {
                // Preserve the original failure if rollback also fails.
            }

            throw;
        }
    }

    private async Task CleanupRunDataAsync(string runId, CancellationToken ct)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            DELETE FROM skips
            WHERE segment_id IN (
                SELECT segment_id
                FROM segments
                WHERE last_ingest_run_id = $1
            );

            DELETE FROM segments
            WHERE last_ingest_run_id = $1;

            DELETE FROM logs
            WHERE ingest_run_id = $1;
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = runId });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static Func<string, long> BuildByteCounter(Encoding encoding) =>
        line => encoding.GetByteCount(line);

    private sealed class ProgressReporter
    {
        private static readonly TimeSpan MinInterval = TimeSpan.FromMilliseconds(250);
        private const long MinRecordDelta = 5_000;
        private const long MinByteDelta = 256 * 1024;

        private readonly string _sourcePath;
        private readonly long _bytesTotal;
        private readonly IProgress<IngestProgressUpdate>? _progress;
        private DateTimeOffset _lastReportedUtc = DateTimeOffset.MinValue;
        private long _lastReportedBytes;
        private long _lastReportedRecords;

        public ProgressReporter(string sourcePath, long bytesTotal, IProgress<IngestProgressUpdate>? progress)
        {
            _sourcePath = sourcePath;
            _bytesTotal = bytesTotal;
            _progress = progress;
        }

        public void Report(
            IngestFilePhase phase,
            long bytesProcessed,
            long recordsProcessed,
            string? message,
            bool force = false)
        {
            if (_progress is null)
                return;

            var now = DateTimeOffset.UtcNow;
            var shouldReport = force
                || _lastReportedUtc == DateTimeOffset.MinValue
                || bytesProcessed >= _bytesTotal
                || phase != IngestFilePhase.Ingesting
                || recordsProcessed - _lastReportedRecords >= MinRecordDelta
                || bytesProcessed - _lastReportedBytes >= MinByteDelta
                || now - _lastReportedUtc >= MinInterval;

            if (!shouldReport)
                return;

            _lastReportedUtc = now;
            _lastReportedBytes = bytesProcessed;
            _lastReportedRecords = recordsProcessed;

            _progress.Report(new IngestProgressUpdate(
                SourcePath: _sourcePath,
                Phase: phase,
                BytesProcessed: bytesProcessed,
                BytesTotal: _bytesTotal,
                RecordsProcessed: recordsProcessed,
                Message: message));
        }
    }
}

public sealed record IngestResult(
    string RunId,
    long TotalRows,
    int FilesProcessed,
    IReadOnlyList<SkipRow> Skips,
    string Status);
