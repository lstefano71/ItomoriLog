using FluentAssertions;

using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Ingest.Detectors;
using ItomoriLog.Core.Ingest.Extractors;
using ItomoriLog.Core.Ingest.Readers;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Tests.Ingest;

public class NdjsonIngestionTests
{
    [Fact]
    public void Probe_ValidNdjson_DetectsWithHighConfidence()
    {
        var lines = Enumerable.Range(0, 20)
            .Select(i => $$"""{"timestamp":"2024-03-15T10:00:{{i:D2}}Z","level":"INFO","message":"Event {{i}}"}""");
        var content = string.Join('\n', lines);

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
        var detector = new NdjsonFormatDetector();
        var result = detector.Probe(stream, "test.jsonl");

        result.Should().NotBeNull();
        result!.Confidence.Should().BeGreaterThan(0.8);
        result.Boundary.Should().BeOfType<JsonNdBoundary>();

        var boundary = (JsonNdBoundary)result.Boundary;
        boundary.TimestampFieldPath.Should().Be("timestamp");
    }

    [Fact]
    public void Probe_NotJson_ReturnsNull()
    {
        var lines = Enumerable.Range(0, 20)
            .Select(i => $"This is plain text line {i} with no JSON");
        var content = string.Join('\n', lines);

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
        var detector = new NdjsonFormatDetector();
        var result = detector.Probe(stream, "test.txt");

        result.Should().BeNull();
    }

    [Fact]
    public void NdjsonReader_ValidLines_ReadsAll()
    {
        var lines = Enumerable.Range(0, 5)
            .Select(i => $$"""{"timestamp":"2024-03-15T10:00:0{{i}}Z","level":"INFO","message":"Msg {{i}}"}""");
        var content = string.Join('\n', lines);

        var boundary = new JsonNdBoundary("timestamp");
        using var reader = new NdjsonRecordReader(new StringReader(content), boundary);

        var records = new List<RawRecord>();
        while (reader.TryReadNext(out var rec))
            records.Add(rec);

        records.Should().HaveCount(5);
        records[0].Fields.Should().ContainKey("timestamp");
        records[0].Fields.Should().ContainKey("level");
        records[0].Fields.Should().ContainKey("message");
        records[0].Fields!["level"].Should().Be("INFO");
    }

    [Fact]
    public void NdjsonReader_MalformedLine_SkipsAndResyncs()
    {
        var lines = new List<string>
        {
            """{"timestamp":"2024-03-15T10:00:00Z","level":"INFO","message":"Good 1"}""",
            """{"timestamp":"2024-03-15T10:00:01Z","level":"INFO","message":"Good 2"}""",
            "THIS IS NOT JSON",
            """{"timestamp":"2024-03-15T10:00:03Z","level":"INFO","message":"Good 3"}""",
            """{"timestamp":"2024-03-15T10:00:04Z","level":"INFO","message":"Good 4"}""",
            """{"timestamp":"2024-03-15T10:00:05Z","level":"INFO","message":"Good 5"}""",
        };
        var content = string.Join('\n', lines);

        var skipSink = new ListSkipSink();
        var skipLogger = new SkipLogger(skipSink, "test", "file1", "seg1");
        var boundary = new JsonNdBoundary("timestamp");
        using var reader = new NdjsonRecordReader(new StringReader(content), boundary, skipLogger);

        var records = new List<RawRecord>();
        while (reader.TryReadNext(out var rec))
            records.Add(rec);

        // 5 good lines parsed (malformed one skipped)
        records.Should().HaveCount(5);

        var skips = skipSink.GetSkips();
        skips.Should().HaveCountGreaterThanOrEqualTo(1);
        skips[0].ReasonCode.Should().Be(SkipReasonCode.JsonMalformed);
    }

    [Fact]
    public void JsonTsExtractor_IsoTimestamp_Parses()
    {
        var fields = new Dictionary<string, string> {
            ["timestamp"] = "2024-03-15T10:30:45.123Z",
            ["message"] = "Hello"
        };
        var raw = new RawRecord("", "", 0, 0, fields);

        var extractor = new JsonTimestampExtractor("timestamp");
        extractor.TryExtract(raw, out var ts).Should().BeTrue();
        ts.Year.Should().Be(2024);
        ts.Month.Should().Be(3);
        ts.Day.Should().Be(15);
    }

    [Fact]
    public void JsonTsExtractor_EpochTimestamp_Parses()
    {
        // Epoch seconds: 1710500000 = 2024-03-15T12:53:20Z
        var fields = new Dictionary<string, string> {
            ["ts"] = "1710500000",
            ["message"] = "Hello"
        };
        var raw = new RawRecord("", "", 0, 0, fields);

        var extractor = new JsonTimestampExtractor("ts");
        extractor.TryExtract(raw, out var ts).Should().BeTrue();
        ts.Year.Should().Be(2024);
        ts.Month.Should().Be(3);
        ts.Day.Should().Be(15);
    }

    [Fact]
    public async Task Integration_NdjsonEndToEnd_IngestsIntoDb()
    {
        var lines = Enumerable.Range(0, 15)
            .Select(i => $$"""{"timestamp":"2024-03-15T10:00:{{i:D2}}Z","level":"INFO","message":"Event {{i}}","source":"app"}""");
        var content = string.Join('\n', lines);

        // Detect
        using var detectStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
        var detector = new NdjsonFormatDetector();
        var detection = detector.Probe(detectStream, "test.jsonl");
        detection.Should().NotBeNull();

        var boundary = (JsonNdBoundary)detection!.Boundary;

        // Read records
        using var reader = new NdjsonRecordReader(new StringReader(content), boundary);
        var rows = new List<LogRow>();
        long idx = 0;
        while (reader.TryReadNext(out var rec)) {
            if (detection.Extractor.TryExtract(rec, out var ts)) {
                rows.Add(new LogRow(
                    TimestampUtc: ts.ToUniversalTime(),
                    TimestampBasis: TimeBasis.Utc,
                    TimestampEffectiveOffsetMinutes: 0,
                    TimestampOriginal: rec.Fields?["timestamp"],
                    LogicalSourceId: "test",
                    SourcePath: "test.jsonl",
                    PhysicalFileId: "json001",
                    SegmentId: "seg001",
                    IngestRunId: "run001",
                    RecordIndex: idx++,
                    Level: rec.Fields?.GetValueOrDefault("level"),
                    Message: rec.Fields?.GetValueOrDefault("message") ?? rec.FullText,
                    FieldsJson: rec.FullText));
            }
        }

        rows.Should().HaveCount(15);

        // Insert into DuckDB
        var tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_ndjson_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);
        try {
            var dbPath = Path.Combine(tempDir, "test.duckdb");
            using var factory = new DuckLakeConnectionFactory(dbPath);
            var conn = await factory.GetConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);

            var inserter = new LogBatchInserter(conn);
            await inserter.InsertBatchAsync(rows);

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM logs";
            var count = await cmd.ExecuteScalarAsync();
            Convert.ToInt64(count).Should().Be(15);
        } finally {
            try { Directory.Delete(tempDir, recursive: true); } catch { }
        }
    }
}
