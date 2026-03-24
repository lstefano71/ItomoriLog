using System.Text.RegularExpressions;
using System.Threading.Channels;
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

    public IngestOrchestrator(
        DuckDBConnection connection,
        DetectionEngine? detectionEngine = null,
        int maxConcurrency = 8,
        int batchChannelCapacity = 16)
    {
        _connection = connection;
        _detectionEngine = detectionEngine ?? new DetectionEngine();
        _maxConcurrency = maxConcurrency;
        _batchChannelCapacity = batchChannelCapacity;
    }

    public async Task<IngestResult> IngestFilesAsync(
        IReadOnlyList<string> filePaths,
        TimeBasisConfig defaultTimeBasis,
        CancellationToken ct = default)
    {
        var tracker = new IngestRunTracker(_connection);
        var runId = await tracker.StartRunAsync(ct);
        var skipSink = new ListSkipSink();
        var totalRows = 0L;
        var filesProcessed = 0;

        try
        {
            // Expand ZIP files
            var entries = new List<FileToIngest>();
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
                            isZipEntry: true));
                    }
                }
                else
                {
                    var capturedPath = path;
                    entries.Add(new FileToIngest(
                        capturedPath,
                        Path.GetFileName(capturedPath),
                        () =>
                        {
                            var ms = new MemoryStream();
                            using (var fs = File.OpenRead(capturedPath))
                                fs.CopyTo(ms);
                            ms.Position = 0;
                            return ms;
                        },
                        isZipEntry: false));
                }
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
                    await ProcessFileAsync(entry, runId, defaultTimeBasis, skipSink, channel.Writer, ct);
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

            await tracker.CompleteRunAsync(runId, ct);

            return new IngestResult(
                RunId: runId,
                TotalRows: Interlocked.Read(ref totalRows),
                FilesProcessed: filesProcessed,
                Skips: skipSink.GetSkips(),
                Status: "completed");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            try { await tracker.CompleteRunAsync(runId, ct); } catch { }
            return new IngestResult(runId, Interlocked.Read(ref totalRows), filesProcessed,
                skipSink.GetSkips(), "failed");
        }
    }

    private async Task ProcessFileAsync(
        FileToIngest entry,
        string runId,
        TimeBasisConfig timeBasis,
        ListSkipSink skipSink,
        ChannelWriter<IReadOnlyList<LogRow>> writer,
        CancellationToken ct)
    {
        using var stream = entry.OpenStream();

        // Detect format
        var engineResult = _detectionEngine.Detect(stream, entry.FileName);
        if (engineResult.Detection is null)
        {
            skipSink.Write(new SkipRow(
                LogicalSourceId: IdentityGenerator.LogicalSourceId(entry.FileName),
                PhysicalFileId: "unknown",
                SegmentId: "unknown",
                SegmentSeq: 0,
                StartLine: null, EndLine: null, StartOffset: null, EndOffset: null,
                ReasonCode: SkipReasonCode.NotRecognized,
                ReasonDetail: $"No viable format detected for {entry.FileName}",
                SamplePrefix: null, DetectorProfileId: null,
                UtcLoggedAt: DateTimeOffset.UtcNow));
            return;
        }

        var detection = engineResult.Detection;
        var logicalSourceId = IdentityGenerator.LogicalSourceId(entry.FileName);

        // Generate identity
        var physicalFileId = IdentityGenerator.PhysicalFileId(
            entry.SourcePath, stream.Length, DateTimeOffset.UtcNow);
        var segmentId = IdentityGenerator.SegmentId(physicalFileId);

        var skipLogger = new SkipLogger(skipSink, logicalSourceId, physicalFileId, segmentId);

        // Create reader based on boundary type
        stream.Position = 0;
        var textReader = new StreamReader(stream);
        using var recordReader = CreateReader(detection.Boundary, textReader, skipLogger);

        // Read and batch records
        var batch = new List<LogRow>(50_000);
        long recordIndex = 0;
        var synthesizer = new FieldSynthesizer();

        while (recordReader.TryReadNext(out var raw))
        {
            ct.ThrowIfCancellationRequested();

            if (!detection.Extractor.TryExtract(raw, out var timestamp))
            {
                var seg = skipLogger.BeginSkip(SkipReasonCode.TimeParse,
                    "Timestamp extraction failed", startLine: raw.LineNumber);
                seg.Close(endLine: raw.LineNumber);
                continue;
            }

            var utcTimestamp = TimezonePolicy.ApplyTimeBasis(timestamp, timeBasis);
            var offsetMinutes = (int)timestamp.Offset.TotalMinutes;

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
                TimestampBasis: timeBasis.Basis,
                TimestampEffectiveOffsetMinutes: offsetMinutes,
                TimestampOriginal: raw.FirstLine[..Math.Min(raw.FirstLine.Length, 50)],
                LogicalSourceId: logicalSourceId,
                SourcePath: entry.SourcePath,
                PhysicalFileId: physicalFileId,
                SegmentId: segmentId,
                IngestRunId: runId,
                RecordIndex: recordIndex++,
                Level: level,
                Message: message,
                FieldsJson: fieldsJson));

            if (batch.Count >= 50_000)
            {
                await writer.WriteAsync(batch.ToList(), ct);
                batch.Clear();
            }
        }

        // Flush remaining
        if (batch.Count > 0)
            await writer.WriteAsync(batch.ToList(), ct);
    }

    private static IRecordReader CreateReader(
        RecordBoundarySpec boundary, TextReader textReader, SkipLogger skipLogger)
    {
        return boundary switch
        {
            TextSoRBoundary sor => new TextRecordReader(textReader, sor.StartRegex),
            CsvBoundary csv => new CsvRecordReader(textReader, csv, skipLogger),
            JsonNdBoundary json => new NdjsonRecordReader(textReader, json, skipLogger),
            _ => throw new NotSupportedException($"Unknown boundary type: {boundary.GetType().Name}")
        };
    }

    private sealed record FileToIngest(
        string SourcePath,
        string FileName,
        Func<MemoryStream> OpenStream,
        bool isZipEntry);
}

public sealed record IngestResult(
    string RunId,
    long TotalRows,
    int FilesProcessed,
    IReadOnlyList<SkipRow> Skips,
    string Status);
