using System.Threading.Channels;
using System.Text;
using DuckDB.NET.Data;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Ingest.Readers;

namespace ItomoriLog.Core.Ingest;

public sealed class IngestOrchestrator
{
    private readonly DuckDBConnection _connection;
    private readonly DetectionEngine _detectionEngine;
    private readonly int _maxConcurrency;
    private readonly int _batchChannelCapacity;
    private readonly IReadOnlyDictionary<string, DetectionResult> _detectionOverrides;

    public IngestOrchestrator(
        DuckDBConnection connection,
        DetectionEngine? detectionEngine = null,
        int maxConcurrency = 8,
        int batchChannelCapacity = 16,
        IReadOnlyDictionary<string, DetectionResult>? detectionOverrides = null)
    {
        _connection = connection;
        _detectionEngine = detectionEngine ?? new DetectionEngine();
        _maxConcurrency = maxConcurrency;
        _batchChannelCapacity = batchChannelCapacity;
        _detectionOverrides = detectionOverrides ?? new Dictionary<string, DetectionResult>(StringComparer.OrdinalIgnoreCase);
    }

    public async Task<IngestResult> IngestFilesAsync(
        IReadOnlyList<string> filePaths,
        TimeBasisConfig defaultTimeBasis,
        IProgress<IngestProgressUpdate>? progress = null,
        CancellationToken ct = default)
    {
        var tracker = new IngestRunTracker(_connection);
        var runId = await tracker.StartRunAsync(ct);
        var skipSink = new ListSkipSink();
        var segmentRows = new List<SegmentUpsertRow>();
        var totalRows = 0L;
        var filesProcessed = 0;
        var entries = new List<FileToIngest>();

        try
        {
            // Expand ZIP files
            entries.Clear();
            foreach (var path in filePaths)
            {
                if (path.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var zipEntry in ZipHandler.EnumerateEntries(path, skipSink))
                    {
                        var capturedPath = path;
                        var capturedEntryName = zipEntry.EntryName;
                        entries.Add(new FileToIngest(
                            zipEntry.SourcePath,
                            capturedEntryName,
                            () => ZipHandler.ExtractToMemory(capturedPath, capturedEntryName),
                            isZipEntry: true,
                            initialSizeBytes: zipEntry.CompressedLength));
                    }
                }
                else
                {
                    var capturedPath = path;
                    var info = new FileInfo(capturedPath);
                    entries.Add(new FileToIngest(
                        capturedPath,
                        Path.GetFileName(capturedPath),
                        () => File.OpenRead(capturedPath),
                        isZipEntry: false,
                        initialSizeBytes: info.Exists ? info.Length : 0));
                }
            }

            foreach (var entry in entries)
            {
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
                new BoundedChannelOptions(_batchChannelCapacity)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true
                });

            // Consumer task: writes batches to DB
            var writerTask = Task.Run(async () =>
            {
                var inserter = new LogBatchInserter(_connection);
                await foreach (var batch in channel.Reader.ReadAllAsync(ct))
                {
                    await inserter.InsertBatchAsync(batch, ct);
                    Interlocked.Add(ref totalRows, batch.Count);
                }
            }, ct);

            // Producer tasks: detect + read + extract per file
            var semaphore = new SemaphoreSlim(_maxConcurrency);
            var producerTasks = entries.Select(async entry =>
            {
                await semaphore.WaitAsync(ct);
                try
                {
                    var segmentRow = await ProcessFileAsync(
                        entry,
                        runId,
                        defaultTimeBasis,
                        skipSink,
                        channel.Writer,
                        progress,
                        ct);
                    if (segmentRow is not null)
                    {
                        lock (segmentRows)
                        {
                            segmentRows.Add(segmentRow);
                        }
                    }
                    Interlocked.Increment(ref filesProcessed);
                }
                finally
                {
                    semaphore.Release();
                }
            }).ToArray();

            await Task.WhenAll(producerTasks);
            channel.Writer.Complete();
            await writerTask;

            var snapshotSegments = segmentRows.ToArray();
            if (snapshotSegments.Length > 0)
            {
                var segmentUpserter = new SegmentUpserter(_connection);
                await segmentUpserter.UpsertBatchAsync(snapshotSegments, ct);
            }

            var skipRows = skipSink.GetSkips();
            if (skipRows.Count > 0)
            {
                var skipInserter = new SkipBatchInserter(_connection);
                await skipInserter.InsertBatchAsync(skipRows, sessionId: null, ct);
            }

            await tracker.CompleteRunAsync(runId, ct);

            return new IngestResult(
                RunId: runId,
                TotalRows: Interlocked.Read(ref totalRows),
                FilesProcessed: filesProcessed,
                Skips: skipSink.GetSkips(),
                Status: "completed");
        }
        catch (Exception) when (ct.IsCancellationRequested is false)
        {
            try { await tracker.CompleteRunAsync(runId, ct); } catch { }
            foreach (var entry in entries)
            {
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
        using var stream = entry.OpenStream();
        var canonicalSourcePath = CanonicalizeSourcePath(entry.SourcePath);
        var logicalSourceId = IdentityGenerator.LogicalSourceId(entry.FileName);
        var lastModifiedUtc = ResolveLastModifiedUtc(canonicalSourcePath, entry.isZipEntry);
        var totalBytes = stream.Length;

        progress?.Report(new IngestProgressUpdate(
            SourcePath: canonicalSourcePath,
            Phase: IngestFilePhase.Sniffing,
            BytesProcessed: 0,
            BytesTotal: totalBytes,
            RecordsProcessed: 0,
            Message: "Sniffing"));

        // Detect format
        DetectionResult? detection;
        if (!_detectionOverrides.TryGetValue(canonicalSourcePath, out detection))
        {
            var engineResult = _detectionEngine.Detect(stream, entry.FileName);
            detection = engineResult.Detection;
            if (detection is null)
            {
                var failedPhysicalFileId = IdentityGenerator.PhysicalFileId(
                    canonicalSourcePath,
                    stream.Length,
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
                var failedFileSize = stream.Length;
                var failedFileHash = await ComputeStreamHashAsync(stream, ct);

                progress?.Report(new IngestProgressUpdate(
                    SourcePath: canonicalSourcePath,
                    Phase: IngestFilePhase.Skipped,
                    BytesProcessed: failedFileSize,
                    BytesTotal: failedFileSize,
                    RecordsProcessed: 0,
                    Message: "Not recognized"));

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
        }

        progress?.Report(new IngestProgressUpdate(
            SourcePath: canonicalSourcePath,
            Phase: IngestFilePhase.Ingesting,
            BytesProcessed: 0,
            BytesTotal: totalBytes,
            RecordsProcessed: 0,
            Message: "Ingesting"));

        // Generate identity
        var physicalFileId = IdentityGenerator.PhysicalFileId(
            canonicalSourcePath, stream.Length, lastModifiedUtc);
        var segmentId = IdentityGenerator.SegmentId(physicalFileId);

        var skipLogger = new SkipLogger(skipSink, logicalSourceId, physicalFileId, segmentId);

        // Create reader based on boundary type
        stream.Position = 0;
        var encoding = EncodingDetector.Detect(stream);
        stream.Position = 0;
        var textReader = new StreamReader(stream, encoding, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
        var byteCounter = BuildByteCounter(encoding);
        using var recordReader = CreateReader(detection.Boundary, textReader, skipLogger, byteCounter);

        // Read and batch records
        var batch = new List<LogRow>(50_000);
        long recordIndex = 0;
        var synthesizer = new FieldSynthesizer();
        DateTimeOffset? minTs = null;
        DateTimeOffset? maxTs = null;

        while (recordReader.TryReadNext(out var raw))
        {
            ct.ThrowIfCancellationRequested();

            if (!TimestampResolver.TryResolve(detection.Extractor, raw, timeBasis, out var resolvedTimestamp))
            {
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

            if (detection.Boundary is TextSoRBoundary sor)
            {
                var tsMatch = sor.StartRegex.Match(raw.FirstLine);
                if (tsMatch.Success)
                {
                    var postTs = raw.FirstLine[(tsMatch.Index + tsMatch.Length)..];
                    var extracted = synthesizer.Extract(postTs);
                    level = extracted.Level;
                    message = extracted.Message;
                    fieldsJson = extracted.FieldsJson;
                }
            }
            else if (raw.Fields is not null)
            {
                // CSV/NDJSON — level and message may be in fields
                raw.Fields.TryGetValue("level", out var lvl);
                raw.Fields.TryGetValue("severity", out var sev);
                level = lvl ?? sev;
                if (level is not null) level = FieldSynthesizer.NormalizeLevel(level);

                raw.Fields.TryGetValue("message", out var msg);
                raw.Fields.TryGetValue("msg", out var msg2);
                if (msg is not null || msg2 is not null)
                    message = msg ?? msg2 ?? raw.FullText;

                // Remaining fields as JSON
                var extra = raw.Fields
                    .Where(kv => kv.Key is not "level" and not "severity" and not "message" and not "msg")
                    .ToDictionary(kv => kv.Key, kv => kv.Value);
                if (extra.Count > 0)
                    fieldsJson = System.Text.Json.JsonSerializer.Serialize(extra);
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

            progress?.Report(new IngestProgressUpdate(
                SourcePath: canonicalSourcePath,
                Phase: IngestFilePhase.Ingesting,
                BytesProcessed: Math.Min(raw.EndByteOffset, totalBytes),
                BytesTotal: totalBytes,
                RecordsProcessed: recordIndex,
                Message: "Ingesting"));

            if (batch.Count >= 50_000)
            {
                await writer.WriteAsync(batch.ToList(), ct);
                batch.Clear();
            }
        }

        // Flush remaining
        if (batch.Count > 0)
            await writer.WriteAsync(batch.ToList(), ct);

        var fileHash = await ComputeStreamHashAsync(stream, ct);

        progress?.Report(new IngestProgressUpdate(
            SourcePath: canonicalSourcePath,
            Phase: IngestFilePhase.Completed,
            BytesProcessed: totalBytes,
            BytesTotal: totalBytes,
            RecordsProcessed: recordIndex,
            Message: "Completed"));

        return new SegmentUpsertRow(
            SegmentId: segmentId,
            LogicalSourceId: logicalSourceId,
            PhysicalFileId: physicalFileId,
            MinTsUtc: minTs,
            MaxTsUtc: maxTs,
            RowCount: recordIndex,
            LastIngestRunId: runId,
            Active: true,
            LastByteOffset: stream.Length,
            SourcePath: canonicalSourcePath,
            FileSizeBytes: stream.Length,
            LastModifiedUtc: lastModifiedUtc,
            FileHash: fileHash);
    }

    private static IRecordReader CreateReader(
        RecordBoundarySpec boundary,
        TextReader textReader,
        SkipLogger skipLogger,
        Func<string, long> byteCounter)
    {
        return boundary switch
        {
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

        var bangIndex = sourcePath.IndexOf('!');
        if (bangIndex > 0)
        {
            var zipPath = sourcePath[..bangIndex];
            if (File.Exists(zipPath))
                return new DateTimeOffset(File.GetLastWriteTimeUtc(zipPath), TimeSpan.Zero);
        }

        return DateTimeOffset.UtcNow;
    }

    private static string CanonicalizeSourcePath(string sourcePath)
    {
        var bangIndex = sourcePath.IndexOf('!');
        if (bangIndex > 0)
        {
            var archivePath = sourcePath[..bangIndex];
            var entrySuffix = sourcePath[bangIndex..];
            var fullArchivePath = Path.GetFullPath(archivePath);
            return $"{fullArchivePath}{entrySuffix}";
        }

        return Path.GetFullPath(sourcePath);
    }

    private static async Task<string> ComputeStreamHashAsync(Stream stream, CancellationToken ct)
    {
        stream.Position = 0;
        var hashBytes = await System.Security.Cryptography.SHA256.HashDataAsync(stream, ct);
        stream.Position = 0;
        return Convert.ToHexStringLower(hashBytes);
    }

    private static Func<string, long> BuildByteCounter(Encoding encoding) =>
        line => encoding.GetByteCount(line);
}

public sealed record IngestResult(
    string RunId,
    long TotalRows,
    int FilesProcessed,
    IReadOnlyList<SkipRow> Skips,
    string Status);
