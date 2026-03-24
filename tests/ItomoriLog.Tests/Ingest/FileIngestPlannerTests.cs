using DuckDB.NET.Data;
using FluentAssertions;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Tests.Ingest;

public class FileIngestPlannerTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public FileIngestPlannerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_ingest_plan_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    [Fact]
    public async Task PlanAsync_UnseenFile_PlansForIngest()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var file = Path.Combine(_tempDir, "fresh.log");
        await File.WriteAllTextAsync(file, "2024-01-01 00:00:00 INFO hello\n");

        var planner = new FileIngestPlanner(conn);
        var plan = await planner.PlanAsync([file], ExistingFileAction.Skip);

        plan.FilesToIngest.Should().ContainSingle().Which.Should().Be(Path.GetFullPath(file));
        plan.SegmentsToReingest.Should().BeEmpty();
        plan.SkippedFiles.Should().BeEmpty();
    }

    [Fact]
    public async Task PlanAsync_ExistingSameFingerprint_DefaultSkip_SkipsFile()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var file = Path.Combine(_tempDir, "stable.log");
        await File.WriteAllTextAsync(file, "2024-01-01 00:00:00 INFO hello\n");

        await InsertSegmentForFileAsync(conn, "seg_stable", file, "run_1");

        var planner = new FileIngestPlanner(conn);
        var plan = await planner.PlanAsync([file], ExistingFileAction.Skip);

        plan.FilesToIngest.Should().BeEmpty();
        plan.SegmentsToReingest.Should().BeEmpty();
        plan.SkippedFiles.Should().ContainSingle(s => s.SourcePath == Path.GetFullPath(file));
    }

    [Fact]
    public async Task PlanAsync_ExistingSameFingerprint_Reingest_SelectsSegment()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var file = Path.Combine(_tempDir, "stable_reingest.log");
        await File.WriteAllTextAsync(file, "2024-01-01 00:00:00 INFO hello\n");

        await InsertSegmentForFileAsync(conn, "seg_reingest_target", file, "run_1");

        var planner = new FileIngestPlanner(conn);
        var plan = await planner.PlanAsync([file], ExistingFileAction.Reingest);

        plan.FilesToIngest.Should().BeEmpty();
        plan.SegmentsToReingest.Should().ContainSingle().Which.Should().Be("seg_reingest_target");
        plan.SkippedFiles.Should().BeEmpty();
    }

    [Fact]
    public async Task PlanAsync_ExistingSameFingerprint_ForceAdd_QueuesFile()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var file = Path.Combine(_tempDir, "stable_force.log");
        await File.WriteAllTextAsync(file, "2024-01-01 00:00:00 INFO hello\n");

        await InsertSegmentForFileAsync(conn, "seg_force_target", file, "run_1");

        var planner = new FileIngestPlanner(conn);
        var plan = await planner.PlanAsync([file], ExistingFileAction.ForceAdd);

        plan.FilesToIngest.Should().ContainSingle().Which.Should().Be(Path.GetFullPath(file));
        plan.SegmentsToReingest.Should().BeEmpty();
        plan.SkippedFiles.Should().BeEmpty();
    }

    [Fact]
    public async Task PlanAsync_MissingFile_IsSkippedWithReason()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var missing = Path.Combine(_tempDir, "missing.log");
        var planner = new FileIngestPlanner(conn);
        var plan = await planner.PlanAsync([missing], ExistingFileAction.Skip);

        plan.FilesToIngest.Should().BeEmpty();
        plan.SegmentsToReingest.Should().BeEmpty();
        plan.SkippedFiles.Should().ContainSingle();
        plan.SkippedFiles[0].Reason.Should().Contain("does not exist");
    }

    [Fact]
    public async Task PlanAsync_ZipFile_AlwaysQueuedForIngest()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var zipPath = Path.Combine(_tempDir, "logs.zip");
        await File.WriteAllBytesAsync(zipPath, []);

        var planner = new FileIngestPlanner(conn);
        var plan = await planner.PlanAsync([zipPath], ExistingFileAction.Skip);

        plan.FilesToIngest.Should().ContainSingle().Which.Should().Be(Path.GetFullPath(zipPath));
    }

    private static async Task InsertSegmentForFileAsync(
        DuckDBConnection conn,
        string segmentId,
        string filePath,
        string runId)
    {
        var fullPath = Path.GetFullPath(filePath);
        var info = new FileInfo(fullPath);
        var lastModified = new DateTimeOffset(info.LastWriteTimeUtc, TimeSpan.Zero);
        var physicalFileId = IdentityGenerator.PhysicalFileId(fullPath, info.Length, lastModified);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO segments (
                segment_id, logical_source_id, physical_file_id, row_count, last_ingest_run_id, active,
                source_path, file_size_bytes, last_modified_utc, file_hash, last_byte_offset
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "test" });
        cmd.Parameters.Add(new DuckDBParameter { Value = physicalFileId });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = runId });
        cmd.Parameters.Add(new DuckDBParameter { Value = true });
        cmd.Parameters.Add(new DuckDBParameter { Value = fullPath });
        cmd.Parameters.Add(new DuckDBParameter { Value = info.Length });
        cmd.Parameters.Add(new DuckDBParameter { Value = lastModified.UtcDateTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = info.Length });
        await cmd.ExecuteNonQueryAsync();
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
