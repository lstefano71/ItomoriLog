using FluentAssertions;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;
using DuckDB.NET.Data;

namespace ItomoriLog.Tests.Ingest;

public class ReingestServiceTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public ReingestServiceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_reingest_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    [Fact]
    public async Task ReingestSegment_ReplacesExistingData()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        const string segmentId = "seg001";
        const string logicalSourceId = "test";
        const string physicalFileId = "phys001";
        const string runId = "run001";

        // Create a log file with 30 lines
        var logFile = Path.Combine(_tempDir, "test.log");
        var baseTime = new DateTime(2024, 3, 15, 10, 0, 0);
        var lines = Enumerable.Range(0, 30)
            .Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO Original message {i}");
        await File.WriteAllTextAsync(logFile, string.Join('\n', lines));

        // Insert initial segment row
        await InsertSegmentAsync(conn, segmentId, logicalSourceId, physicalFileId, runId);

        // Insert initial log rows pointing at this segment
        var inserter = new LogBatchInserter(conn);
        var initialRows = Enumerable.Range(0, 30).Select(i => new LogRow(
            TimestampUtc: new DateTimeOffset(baseTime.AddSeconds(i), TimeSpan.Zero),
            TimestampBasis: TimeBasis.Local,
            TimestampEffectiveOffsetMinutes: 0,
            TimestampOriginal: $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff}",
            LogicalSourceId: logicalSourceId,
            SourcePath: logFile,
            PhysicalFileId: physicalFileId,
            SegmentId: segmentId,
            IngestRunId: runId,
            RecordIndex: i,
            Level: "INFO",
            Message: $"Original message {i}",
            FieldsJson: null)).ToList();
        await inserter.InsertBatchAsync(initialRows);

        // Verify initial state
        (await CountLogsAsync(conn, segmentId)).Should().Be(30);

        // Now modify the file — add 10 more lines
        var newLines = Enumerable.Range(30, 10)
            .Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} WARN Updated message {i}");
        await File.AppendAllTextAsync(logFile, "\n" + string.Join('\n', newLines));

        // Re-ingest
        var service = new ReingestService(conn);
        var result = await service.ReingestSegmentAsync(
            segmentId, new TimeBasisConfig(TimeBasis.Utc));

        result.Success.Should().BeTrue(because: $"Re-ingest should succeed but got error: {result.Error}");
        result.NewRowCount.Should().Be(40); // 30 original + 10 new
        result.Error.Should().BeNull();

        // Verify old data was replaced (all rows belong to new run)
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT DISTINCT ingest_run_id FROM logs WHERE segment_id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        using var reader = await cmd.ExecuteReaderAsync();
        var runIds = new List<string>();
        while (await reader.ReadAsync())
            runIds.Add(reader.GetString(0));
        runIds.Should().HaveCount(1);
        runIds[0].Should().NotBe(runId, "re-ingest should use a new run ID");
    }

    [Fact]
    public async Task ReingestSegment_RollsBackOnMissingFile()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        const string segmentId = "seg002";
        const string logicalSourceId = "test";
        const string physicalFileId = "phys002";
        const string runId = "run002";

        // Create log file, insert data, then delete the file
        var logFile = Path.Combine(_tempDir, "disappearing.log");
        var baseTime = new DateTime(2024, 3, 15, 10, 0, 0);
        var lines = Enumerable.Range(0, 30)
            .Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO Message {i}");
        await File.WriteAllTextAsync(logFile, string.Join('\n', lines));

        await InsertSegmentAsync(conn, segmentId, logicalSourceId, physicalFileId, runId);

        var inserter = new LogBatchInserter(conn);
        var initialRows = Enumerable.Range(0, 30).Select(i => new LogRow(
            TimestampUtc: new DateTimeOffset(baseTime.AddSeconds(i), TimeSpan.Zero),
            TimestampBasis: TimeBasis.Local,
            TimestampEffectiveOffsetMinutes: 0,
            TimestampOriginal: $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff}",
            LogicalSourceId: logicalSourceId,
            SourcePath: logFile,
            PhysicalFileId: physicalFileId,
            SegmentId: segmentId,
            IngestRunId: runId,
            RecordIndex: i,
            Level: "INFO",
            Message: $"Message {i}",
            FieldsJson: null)).ToList();
        await inserter.InsertBatchAsync(initialRows);

        // Delete the source file
        File.Delete(logFile);

        // Re-ingest should fail gracefully
        var service = new ReingestService(conn);
        var result = await service.ReingestSegmentAsync(
            segmentId, new TimeBasisConfig(TimeBasis.Utc));

        result.Success.Should().BeFalse();
        result.Error.Should().Contain("not found");

        // Original data should be untouched
        (await CountLogsAsync(conn, segmentId)).Should().Be(30);
    }

    [Fact]
    public async Task ReingestSegment_SegmentNotFound_ReturnsError()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var service = new ReingestService(conn);
        var result = await service.ReingestSegmentAsync(
            "nonexistent_segment", new TimeBasisConfig(TimeBasis.Utc));

        result.Success.Should().BeFalse();
        result.Error.Should().Contain("not found");
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

    private static async Task<long> CountLogsAsync(DuckDBConnection conn, string segmentId)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM logs WHERE segment_id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt64(result);
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
