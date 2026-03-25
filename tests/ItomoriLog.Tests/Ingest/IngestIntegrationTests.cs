using FluentAssertions;

using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Ingest.Detectors;
using ItomoriLog.Core.Ingest.Readers;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Tests.Ingest;

public class IngestIntegrationTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public IngestIntegrationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_ingest_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    [Fact]
    public async Task EndToEnd_TextFile_IngestsIntoDb()
    {
        // Arrange: create a log file
        var baseTime = new DateTime(2024, 3, 15, 10, 0, 0);
        var logLines = Enumerable.Range(0, 50)
            .Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO Message line {i}");
        var logContent = string.Join('\n', logLines);

        // Initialize DB
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        // Detect format
        using var detectStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(logContent));
        var detector = new TextFormatDetector();
        var detection = detector.Probe(detectStream, "test.log");
        detection.Should().NotBeNull();

        // Read records
        var boundary = (TextSoRBoundary)detection!.Boundary;
        using var readStream = new StringReader(logContent);
        using var reader = new TextRecordReader(readStream, boundary.StartRegex);

        var rows = new List<LogRow>();
        long idx = 0;
        while (reader.TryReadNext(out var rec)) {
            if (detection.Extractor.TryExtract(rec, out var ts)) {
                rows.Add(new LogRow(
                    TimestampUtc: ts.ToUniversalTime(),
                    TimestampBasis: TimeBasis.Local,
                    TimestampEffectiveOffsetMinutes: (int)ts.Offset.TotalMinutes,
                    TimestampOriginal: rec.FirstLine[..23],
                    LogicalSourceId: "test",
                    SourcePath: "test.log",
                    PhysicalFileId: "abc123",
                    SegmentId: "seg001",
                    IngestRunId: "run001",
                    RecordIndex: idx++,
                    Level: "INFO",
                    Message: rec.FullText,
                    FieldsJson: null));
            }
        }

        rows.Should().HaveCount(50);

        // Insert into DB
        var inserter = new LogBatchInserter(conn);
        await inserter.InsertBatchAsync(rows);

        // Verify rows in DB
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM logs";
        var count = await cmd.ExecuteScalarAsync();
        Convert.ToInt64(count).Should().Be(50);
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
