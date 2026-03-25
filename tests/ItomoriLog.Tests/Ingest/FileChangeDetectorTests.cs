using DuckDB.NET.Data;

using FluentAssertions;

using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Tests.Ingest;

public class FileChangeDetectorTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public FileChangeDetectorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_fcd_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    [Fact]
    public async Task DetectsUnchangedFile()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var logFile = Path.Combine(_tempDir, "stable.log");
        await File.WriteAllTextAsync(logFile, "2024-01-01 00:00:00 INFO hello\n");

        const string segmentId = "seg_unchanged";
        await InsertSegmentAsync(conn, segmentId);

        var detector = new FileChangeDetector(conn);
        await detector.RecordFileMetadataAsync(segmentId, logFile);

        var result = await detector.DetectAsync(segmentId);
        result.Status.Should().Be(FileChangeStatus.Unchanged);
    }

    [Fact]
    public async Task DetectsModifiedFile_SizeChange()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var logFile = Path.Combine(_tempDir, "growing.log");
        await File.WriteAllTextAsync(logFile, "2024-01-01 00:00:00 INFO hello\n");

        const string segmentId = "seg_modified_size";
        await InsertSegmentAsync(conn, segmentId);

        var detector = new FileChangeDetector(conn);
        await detector.RecordFileMetadataAsync(segmentId, logFile);

        // Modify file — append data
        await File.AppendAllTextAsync(logFile, "2024-01-01 00:00:01 INFO world\n");

        var result = await detector.DetectAsync(segmentId);
        result.Status.Should().Be(FileChangeStatus.Modified);
        result.Detail.Should().Contain("Size changed");
    }

    [Fact]
    public async Task DetectsDeletedFile()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var logFile = Path.Combine(_tempDir, "ephemeral.log");
        await File.WriteAllTextAsync(logFile, "2024-01-01 00:00:00 INFO hello\n");

        const string segmentId = "seg_deleted";
        await InsertSegmentAsync(conn, segmentId);

        var detector = new FileChangeDetector(conn);
        await detector.RecordFileMetadataAsync(segmentId, logFile);

        File.Delete(logFile);

        var result = await detector.DetectAsync(segmentId);
        result.Status.Should().Be(FileChangeStatus.Deleted);
    }

    [Fact]
    public async Task DetectsNewSegment_NoMetadata()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        const string segmentId = "seg_new";
        await InsertSegmentAsync(conn, segmentId);

        // Don't record metadata — source_path will be null
        var detector = new FileChangeDetector(conn);
        var result = await detector.DetectAsync(segmentId);
        result.Status.Should().Be(FileChangeStatus.New);
    }

    [Fact]
    public async Task DetectsNewSegment_NoSegmentRow()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var detector = new FileChangeDetector(conn);
        var result = await detector.DetectAsync("nonexistent");
        result.Status.Should().Be(FileChangeStatus.New);
    }

    [Fact]
    public async Task DetectsModifiedFile_TimestampChange()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var logFile = Path.Combine(_tempDir, "touched.log");
        await File.WriteAllTextAsync(logFile, "2024-01-01 00:00:00 INFO hello\n");

        const string segmentId = "seg_modified_time";
        await InsertSegmentAsync(conn, segmentId);

        var detector = new FileChangeDetector(conn);
        await detector.RecordFileMetadataAsync(segmentId, logFile);

        // Modify only the content (same-ish size doesn't matter — we change content with same byte count)
        // Actually, we'll change last write time to force detection
        await File.WriteAllTextAsync(logFile, "2024-01-01 00:00:00 INFO world\n");

        var result = await detector.DetectAsync(segmentId);
        // Could be Modified (size or timestamp changed) or Unchanged if hash matches
        // Since content changed, hash differs → Modified
        result.Status.Should().Be(FileChangeStatus.Modified);
    }

    [Fact]
    public async Task RecordFileMetadata_StoresHash()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var logFile = Path.Combine(_tempDir, "hashme.log");
        await File.WriteAllTextAsync(logFile, "test content for hashing\n");

        const string segmentId = "seg_hash";
        await InsertSegmentAsync(conn, segmentId);

        var detector = new FileChangeDetector(conn);
        await detector.RecordFileMetadataAsync(segmentId, logFile);

        // Verify hash was stored
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT file_hash FROM segments WHERE segment_id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        var hash = await cmd.ExecuteScalarAsync();
        hash.Should().NotBeNull();
        hash.Should().BeOfType<string>();
        ((string)hash!).Should().HaveLength(64); // SHA256 hex
    }

    private static async Task InsertSegmentAsync(DuckDBConnection conn, string segmentId)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO segments (segment_id, logical_source_id, physical_file_id, row_count, last_ingest_run_id, active)
            VALUES ($1, $2, $3, $4, $5, $6)
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "test" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "phys001" });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "run001" });
        cmd.Parameters.Add(new DuckDBParameter { Value = true });
        await cmd.ExecuteNonQueryAsync();
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
