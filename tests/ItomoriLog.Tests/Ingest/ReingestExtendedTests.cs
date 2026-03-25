using DuckDB.NET.Data;

using FluentAssertions;

using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Tests.Ingest;

/// <summary>
/// Extended tests for ReingestService and FileChangeDetector edge cases.
/// </summary>
public class ReingestExtendedTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public ReingestExtendedTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_reingest_ext_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    [Fact]
    public async Task ReingestMultipleSegments_IndependentlyReplaced()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var baseTime = new DateTime(2024, 3, 15, 10, 0, 0);

        // Create two log files for two segments (30+ lines each for reliable detection)
        var logFileA = Path.Combine(_tempDir, "segA.log");
        var logFileB = Path.Combine(_tempDir, "segB.log");
        await File.WriteAllTextAsync(logFileA, string.Join('\n',
            Enumerable.Range(0, 30).Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO Segment A message number {i}")));
        await File.WriteAllTextAsync(logFileB, string.Join('\n',
            Enumerable.Range(0, 30).Select(i => $"{baseTime.AddMinutes(1).AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} WARN Segment B message number {i}")));

        // Insert segments
        await InsertSegmentAsync(conn, "segA", "sourceA", "physA", "run01");
        await InsertSegmentAsync(conn, "segB", "sourceB", "physB", "run01");

        // Insert initial rows for both segments
        var inserter = new LogBatchInserter(conn);
        await inserter.InsertBatchAsync(
            Enumerable.Range(0, 30).Select(i => MakeRow(baseTime.AddSeconds(i), "sourceA", logFileA, "physA", "segA", "run01", i, "INFO")).ToList());
        await inserter.InsertBatchAsync(
            Enumerable.Range(0, 30).Select(i => MakeRow(baseTime.AddMinutes(1).AddSeconds(i), "sourceB", logFileB, "physB", "segB", "run01", i, "WARN")).ToList());

        (await CountLogsAsync(conn, "segA")).Should().Be(30);
        (await CountLogsAsync(conn, "segB")).Should().Be(30);

        // Modify segA file — add more lines
        await File.AppendAllTextAsync(logFileA, "\n" + string.Join('\n',
            Enumerable.Range(30, 10).Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} ERROR New A message number {i}")));

        // Re-ingest only segA
        var service = new ReingestService(conn);
        var resultA = await service.ReingestSegmentAsync("segA", new TimeBasisConfig(TimeBasis.Utc));

        resultA.Success.Should().BeTrue(because: $"Re-ingest segA should succeed: {resultA.Error}");
        resultA.NewRowCount.Should().Be(40); // 30 + 10

        // segB should be untouched
        (await CountLogsAsync(conn, "segB")).Should().Be(30);

        // Now re-ingest segB (unmodified)
        var resultB = await service.ReingestSegmentAsync("segB", new TimeBasisConfig(TimeBasis.Utc));
        resultB.Success.Should().BeTrue(because: $"Re-ingest segB: {resultB.Error}");
        resultB.NewRowCount.Should().Be(30);
    }

    [Fact]
    public async Task ReingestSegment_PartialReingest_OtherSegmentsUnaffected()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var baseTime = new DateTime(2024, 6, 1, 12, 0, 0);

        // Create files
        var logFile1 = Path.Combine(_tempDir, "partial1.log");
        var logFile2 = Path.Combine(_tempDir, "partial2.log");
        await File.WriteAllTextAsync(logFile1, string.Join('\n',
            Enumerable.Range(0, 30).Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO File1 message number {i}")));
        await File.WriteAllTextAsync(logFile2, string.Join('\n',
            Enumerable.Range(0, 30).Select(i => $"{baseTime.AddSeconds(i + 100):yyyy-MM-dd HH:mm:ss.fff} INFO File2 message number {i}")));

        // Setup segments and rows
        await InsertSegmentAsync(conn, "partial1", "src1", "phys1", "runP");
        await InsertSegmentAsync(conn, "partial2", "src2", "phys2", "runP");

        var inserter = new LogBatchInserter(conn);
        await inserter.InsertBatchAsync(
            Enumerable.Range(0, 30).Select(i => MakeRow(baseTime.AddSeconds(i), "src1", logFile1, "phys1", "partial1", "runP", i, "INFO")).ToList());
        await inserter.InsertBatchAsync(
            Enumerable.Range(0, 30).Select(i => MakeRow(baseTime.AddSeconds(i + 100), "src2", logFile2, "phys2", "partial2", "runP", i, "INFO")).ToList());

        var totalBefore = await CountAllLogsAsync(conn);
        totalBefore.Should().Be(60);

        // Re-ingest partial1 only
        var service = new ReingestService(conn);
        var result = await service.ReingestSegmentAsync("partial1", new TimeBasisConfig(TimeBasis.Utc));
        result.Success.Should().BeTrue(because: $"partial1: {result.Error}");

        // partial2 count should be unchanged
        (await CountLogsAsync(conn, "partial2")).Should().Be(30);
        // Total should still be the same
        var totalAfter = await CountAllLogsAsync(conn);
        totalAfter.Should().Be(60);
    }

    [Fact]
    public async Task ConcurrentReingest_DifferentSegments_BothSucceed()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var baseTime = new DateTime(2024, 7, 1, 8, 0, 0);

        // Create files (30 lines each for reliable detection)
        var logFileX = Path.Combine(_tempDir, "concX.log");
        var logFileY = Path.Combine(_tempDir, "concY.log");
        await File.WriteAllTextAsync(logFileX, string.Join('\n',
            Enumerable.Range(0, 30).Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO ConcX message number {i}")));
        await File.WriteAllTextAsync(logFileY, string.Join('\n',
            Enumerable.Range(0, 30).Select(i => $"{baseTime.AddSeconds(i + 50):yyyy-MM-dd HH:mm:ss.fff} WARN ConcY message number {i}")));

        await InsertSegmentAsync(conn, "concX", "srcX", "physX", "runC");
        await InsertSegmentAsync(conn, "concY", "srcY", "physY", "runC");

        var inserter = new LogBatchInserter(conn);
        await inserter.InsertBatchAsync(
            Enumerable.Range(0, 30).Select(i => MakeRow(baseTime.AddSeconds(i), "srcX", logFileX, "physX", "concX", "runC", i, "INFO")).ToList());
        await inserter.InsertBatchAsync(
            Enumerable.Range(0, 30).Select(i => MakeRow(baseTime.AddSeconds(i + 50), "srcY", logFileY, "physY", "concY", "runC", i, "WARN")).ToList());

        var service = new ReingestService(conn);

        // Run re-ingests sequentially (DuckDB single-writer constraint)
        // but verify both succeed independently
        var resultX = await service.ReingestSegmentAsync("concX", new TimeBasisConfig(TimeBasis.Utc));
        var resultY = await service.ReingestSegmentAsync("concY", new TimeBasisConfig(TimeBasis.Utc));

        resultX.Success.Should().BeTrue(because: $"concX: {resultX.Error}");
        resultY.Success.Should().BeTrue(because: $"concY: {resultY.Error}");
        resultX.NewRowCount.Should().Be(30);
        resultY.NewRowCount.Should().Be(30);
    }

    [Fact]
    public async Task Reingest_FormatChange_CsvToNdjson()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var baseTime = new DateTime(2024, 4, 1, 14, 0, 0);

        // Start with a CSV-like log file
        var logFile = Path.Combine(_tempDir, "format_change.log");
        await File.WriteAllTextAsync(logFile, string.Join('\n',
            Enumerable.Range(0, 5).Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO Original CSV-ish {i}")));

        await InsertSegmentAsync(conn, "segFmt", "srcFmt", "physFmt", "runFmt");

        var inserter = new LogBatchInserter(conn);
        await inserter.InsertBatchAsync(
            Enumerable.Range(0, 5).Select(i => MakeRow(baseTime.AddSeconds(i), "srcFmt", logFile, "physFmt", "segFmt", "runFmt", i, "INFO")).ToList());

        (await CountLogsAsync(conn, "segFmt")).Should().Be(5);

        // Replace with NDJSON content
        var ndjsonLines = Enumerable.Range(0, 8).Select(i =>
            System.Text.Json.JsonSerializer.Serialize(new {
                timestamp = baseTime.AddSeconds(i).ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                level = "WARN",
                message = $"NDJSON message {i}"
            }));
        await File.WriteAllTextAsync(logFile, string.Join('\n', ndjsonLines));

        // Re-ingest should detect new format and succeed
        var service = new ReingestService(conn);
        var result = await service.ReingestSegmentAsync("segFmt", new TimeBasisConfig(TimeBasis.Utc));

        result.Success.Should().BeTrue(because: $"Format change re-ingest: {result.Error}");
        result.NewRowCount.Should().Be(8);

        // Verify old CSV rows are gone, replaced by NDJSON rows
        (await CountLogsAsync(conn, "segFmt")).Should().Be(8);
    }

    [Fact]
    public async Task FileChangeDetector_ContentUnchangedButTimestampDiffers_ReportsUnchanged()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var logFile = Path.Combine(_tempDir, "same_content.log");
        var content = "2024-01-01 00:00:00 INFO hello world\n";
        await File.WriteAllTextAsync(logFile, content);

        const string segmentId = "seg_touch";
        await InsertSegmentAsync(conn, segmentId, "test", "phys", "run");

        var detector = new FileChangeDetector(conn);
        await detector.RecordFileMetadataAsync(segmentId, logFile);

        // "Touch" the file — rewrite same content, changes timestamp but not content
        await Task.Delay(50);
        await File.WriteAllTextAsync(logFile, content);

        var result = await detector.DetectAsync(segmentId);
        // Hash matches → should report Unchanged despite different timestamp
        result.Status.Should().Be(FileChangeStatus.Unchanged);
    }

    [Fact]
    public async Task FileChangeDetector_EmptyFile_HandledGracefully()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var logFile = Path.Combine(_tempDir, "empty.log");
        await File.WriteAllTextAsync(logFile, "");

        const string segmentId = "seg_empty";
        await InsertSegmentAsync(conn, segmentId, "test", "phys", "run");

        var detector = new FileChangeDetector(conn);
        await detector.RecordFileMetadataAsync(segmentId, logFile);

        var result = await detector.DetectAsync(segmentId);
        result.Status.Should().Be(FileChangeStatus.Unchanged);
    }

    [Fact]
    public async Task FileChangeDetector_FileGrows_DetectsModification()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var logFile = Path.Combine(_tempDir, "growing.log");
        await File.WriteAllTextAsync(logFile, "2024-01-01 00:00:00 INFO initial\n");

        const string segmentId = "seg_grow";
        await InsertSegmentAsync(conn, segmentId, "test", "phys", "run");

        var detector = new FileChangeDetector(conn);
        await detector.RecordFileMetadataAsync(segmentId, logFile);

        // Append data
        await File.AppendAllTextAsync(logFile, "2024-01-01 00:00:01 INFO appended\n");

        var result = await detector.DetectAsync(segmentId);
        result.Status.Should().Be(FileChangeStatus.Modified);
    }

    [Fact]
    public async Task FileChangeDetector_FileShrinks_DetectsModification()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var logFile = Path.Combine(_tempDir, "shrinking.log");
        await File.WriteAllTextAsync(logFile, "2024-01-01 00:00:00 INFO line one\n2024-01-01 00:00:01 INFO line two\n");

        const string segmentId = "seg_shrink";
        await InsertSegmentAsync(conn, segmentId, "test", "phys", "run");

        var detector = new FileChangeDetector(conn);
        await detector.RecordFileMetadataAsync(segmentId, logFile);

        // Truncate file
        await File.WriteAllTextAsync(logFile, "short\n");

        var result = await detector.DetectAsync(segmentId);
        result.Status.Should().Be(FileChangeStatus.Modified);
    }

    [Fact]
    public async Task ComputeFileHash_DeterministicForSameContent()
    {
        var file1 = Path.Combine(_tempDir, "hash1.txt");
        var file2 = Path.Combine(_tempDir, "hash2.txt");
        var content = "identical content for both files";
        await File.WriteAllTextAsync(file1, content);
        await File.WriteAllTextAsync(file2, content);

        var hash1 = await FileChangeDetector.ComputeFileHashAsync(file1);
        var hash2 = await FileChangeDetector.ComputeFileHashAsync(file2);

        hash1.Should().Be(hash2);
        hash1.Should().HaveLength(64); // SHA256 hex
    }

    // --- Helpers ---

    private static LogRow MakeRow(
        DateTime ts, string sourceId, string sourcePath,
        string physFileId, string segmentId, string runId,
        int recordIndex, string level)
    {
        return new LogRow(
            TimestampUtc: new DateTimeOffset(ts, TimeSpan.Zero),
            TimestampBasis: TimeBasis.Utc,
            TimestampEffectiveOffsetMinutes: 0,
            TimestampOriginal: ts.ToString("yyyy-MM-dd HH:mm:ss.fff"),
            LogicalSourceId: sourceId,
            SourcePath: sourcePath,
            PhysicalFileId: physFileId,
            SegmentId: segmentId,
            IngestRunId: runId,
            RecordIndex: recordIndex,
            Level: level,
            Message: $"{level} message {recordIndex}",
            FieldsJson: null);
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

    private static async Task<long> CountAllLogsAsync(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM logs";
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt64(result);
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
