using DuckDB.NET.Data;
using FluentAssertions;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Tests.Ingest;

public class IngestPersistenceTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public IngestPersistenceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_ingest_persist_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    [Fact]
    public async Task IngestOrchestrator_PersistsSegmentAndMetadata()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var logPath = Path.Combine(_tempDir, "app.log");
        var baseTime = new DateTime(2024, 3, 15, 10, 0, 0);
        var lines = Enumerable.Range(0, 25)
            .Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO Message {i}");
        await File.WriteAllTextAsync(logPath, string.Join('\n', lines));

        var orchestrator = new IngestOrchestrator(conn, maxConcurrency: 1);
        var result = await orchestrator.IngestFilesAsync([logPath], new TimeBasisConfig(TimeBasis.Utc));
        result.TotalRows.Should().Be(25);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT s.row_count, s.source_path, s.file_size_bytes, s.file_hash, s.last_byte_offset,
                   COUNT(l.segment_id)
            FROM segments s
            LEFT JOIN logs l ON l.segment_id = s.segment_id
            GROUP BY s.segment_id, s.row_count, s.source_path, s.file_size_bytes, s.file_hash, s.last_byte_offset
            """;

        using var reader = await cmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt64(0).Should().Be(25);
        reader.GetString(1).Should().Be(Path.GetFullPath(logPath));
        reader.GetInt64(2).Should().Be(new FileInfo(logPath).Length);
        reader.GetString(3).Should().HaveLength(64);
        reader.GetInt64(4).Should().Be(new FileInfo(logPath).Length);
        reader.GetInt64(5).Should().Be(25);
    }

    [Fact]
    public async Task IngestOrchestrator_PersistsNotRecognizedAsSkipRow()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var binPath = Path.Combine(_tempDir, "random.bin");
        var random = new Random(42);
        var bytes = new byte[2048];
        random.NextBytes(bytes);
        for (int i = 0; i < bytes.Length; i++)
            bytes[i] = (byte)((bytes[i] % 26) + 200);
        await File.WriteAllBytesAsync(binPath, bytes);

        var orchestrator = new IngestOrchestrator(conn, maxConcurrency: 1);
        var result = await orchestrator.IngestFilesAsync([binPath], new TimeBasisConfig(TimeBasis.Utc));
        result.TotalRows.Should().Be(0);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM skips WHERE reason_code = 'NotRecognized'";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        count.Should().Be(1);
    }

    [Fact]
    public async Task ReingestService_PersistsSkipsAndSegmentMetadata()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        const string segmentId = "seg_reingest_skip";
        const string logicalSourceId = "test_csv";
        const string physicalFileId = "phys_reingest";
        const string runId = "run_reingest_seed";

        var logFile = Path.Combine(_tempDir, "reingest.csv");
        var header = "Timestamp,Level,Message";
        var goodLines = Enumerable.Range(0, 30)
            .Select(i => $"2024-03-15 10:00:{i:00},INFO,Good line {i}")
            .ToList();
        await File.WriteAllTextAsync(logFile, $"{header}\n{string.Join('\n', goodLines)}");

        await InsertSegmentAsync(conn, segmentId, logicalSourceId, physicalFileId, runId);

        var inserter = new LogBatchInserter(conn);
        var seedRows = Enumerable.Range(0, 30).Select(i => new LogRow(
            TimestampUtc: new DateTimeOffset(new DateTime(2024, 3, 15, 10, 0, i), TimeSpan.Zero),
            TimestampBasis: TimeBasis.Utc,
            TimestampEffectiveOffsetMinutes: 0,
            TimestampOriginal: $"2024-03-15 10:00:{i:00}.000",
            LogicalSourceId: logicalSourceId,
            SourcePath: Path.GetFullPath(logFile),
            PhysicalFileId: physicalFileId,
            SegmentId: segmentId,
            IngestRunId: runId,
            RecordIndex: i,
            Level: "INFO",
            Message: $"Good line {i}",
            FieldsJson: null)).ToList();
        await inserter.InsertBatchAsync(seedRows);

        // Introduce one malformed CSV row to force skip rows during re-ingest.
        var mixedLines = new List<string>
        {
            header,
            "2024-03-15 10:00:bad,INFO"
        };
        mixedLines.AddRange(goodLines);
        await File.WriteAllTextAsync(logFile, string.Join('\n', mixedLines));

        var service = new ReingestService(conn);
        var result = await service.ReingestSegmentAsync(segmentId, new TimeBasisConfig(TimeBasis.Utc));
        result.Success.Should().BeTrue();
        result.Skips.Should().NotBeEmpty();

        using var skipsCmd = conn.CreateCommand();
        skipsCmd.CommandText = "SELECT COUNT(*) FROM skips WHERE segment_id = $1";
        skipsCmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        var skipCount = Convert.ToInt64(await skipsCmd.ExecuteScalarAsync());
        skipCount.Should().BeGreaterThan(0);

        using var segCmd = conn.CreateCommand();
        segCmd.CommandText = """
            SELECT source_path, file_size_bytes, file_hash, last_byte_offset
            FROM segments
            WHERE segment_id = $1
            """;
        segCmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        using var reader = await segCmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetString(0).Should().Be(Path.GetFullPath(logFile));
        reader.GetInt64(1).Should().Be(new FileInfo(logFile).Length);
        reader.GetString(2).Should().HaveLength(64);
        reader.GetInt64(3).Should().Be(new FileInfo(logFile).Length);
    }

    private static async Task InsertSegmentAsync(
        DuckDBConnection conn, string segmentId, string logicalSourceId,
        string physicalFileId, string runId)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO segments (segment_id, logical_source_id, physical_file_id, row_count, last_ingest_run_id, active)
            VALUES ($1, $2, $3, $4, $5, $6)
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        cmd.Parameters.Add(new DuckDBParameter { Value = logicalSourceId });
        cmd.Parameters.Add(new DuckDBParameter { Value = physicalFileId });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = runId });
        cmd.Parameters.Add(new DuckDBParameter { Value = true });
        await cmd.ExecuteNonQueryAsync();
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
