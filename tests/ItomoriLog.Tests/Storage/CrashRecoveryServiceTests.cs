using FluentAssertions;

using ItomoriLog.Core.Storage;

namespace ItomoriLog.Tests.Storage;

public class CrashRecoveryServiceTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public CrashRecoveryServiceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_crash_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, SessionPaths.DefaultDbFileName);
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    [Fact]
    public void AcquireAndReleaseLock_WritesAndDeletesFile()
    {
        var svc = new CrashRecoveryService(_tempDir);

        svc.AcquireLock();
        File.Exists(svc.LockFilePath).Should().BeTrue();

        svc.ReleaseLock();
        File.Exists(svc.LockFilePath).Should().BeFalse();
    }

    [Fact]
    public void IsLockStale_NoLockFile_ReturnsFalse()
    {
        var svc = new CrashRecoveryService(_tempDir);

        svc.IsLockStale().Should().BeFalse();
    }

    [Fact]
    public void IsLockStale_CurrentProcess_ReturnsFalse()
    {
        var svc = new CrashRecoveryService(_tempDir);
        svc.AcquireLock();

        // Current process PID → not stale
        svc.IsLockStale().Should().BeFalse();
    }

    [Fact]
    public void IsLockStale_DeadPid_ReturnsTrue()
    {
        var svc = new CrashRecoveryService(_tempDir);
        // Write a lockfile with a PID that definitely doesn't exist
        var fakeLock = new LockInfo(99999999, DateTime.UtcNow);
        File.WriteAllText(svc.LockFilePath,
            System.Text.Json.JsonSerializer.Serialize(fakeLock));

        svc.IsLockStale().Should().BeTrue();
    }

    [Fact]
    public void IsLockStale_CorruptFile_ReturnsTrue()
    {
        var svc = new CrashRecoveryService(_tempDir);
        File.WriteAllText(svc.LockFilePath, "not-json");

        svc.IsLockStale().Should().BeTrue();
    }

    [Fact]
    public void ReadLock_ReturnsLockInfo()
    {
        var svc = new CrashRecoveryService(_tempDir);
        svc.AcquireLock();

        var info = svc.ReadLock();
        info.Should().NotBeNull();
        info!.Pid.Should().Be(Environment.ProcessId);
    }

    [Fact]
    public void ReadLock_NoFile_ReturnsNull()
    {
        var svc = new CrashRecoveryService(_tempDir);

        svc.ReadLock().Should().BeNull();
    }

    [Fact]
    public async Task CheckAsync_NoIncompleteRuns_ReturnsClean()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var svc = new CrashRecoveryService(_tempDir);
        var status = await svc.CheckAsync(conn);

        status.CrashDetected.Should().BeFalse();
        status.IncompleteSegmentCount.Should().Be(0);
        status.IncompleteRunIds.Should().BeEmpty();
    }

    [Fact]
    public async Task CheckAsync_WithRunningRunButNoSources_DoesNotSurfaceResume()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        // Insert an incomplete ingest run
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO ingest_runs (run_id, started_utc, status) VALUES ('run1', '2024-01-01', 'running')";
        await cmd.ExecuteNonQueryAsync();

        var svc = new CrashRecoveryService(_tempDir);
        var status = await svc.CheckAsync(conn);

        status.CrashDetected.Should().BeFalse();
        status.CanResume.Should().BeFalse();
        status.IncompleteRunIds.Should().Contain("run1");
    }

    [Fact]
    public async Task CheckAsync_StaleLockOnly_DoesNotSurfaceResume()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var svc = new CrashRecoveryService(_tempDir);
        // Write stale lock
        var fakeLock = new LockInfo(99999999, DateTime.UtcNow);
        File.WriteAllText(svc.LockFilePath,
            System.Text.Json.JsonSerializer.Serialize(fakeLock));

        var status = await svc.CheckAsync(conn);

        status.CrashDetected.Should().BeFalse();
        status.CanResume.Should().BeFalse();
    }

    [Fact]
    public async Task MarkRunsAbandonedAsync_SetsStatusAndCleansLock()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        // Insert a running ingest run
        using var insertCmd = conn.CreateCommand();
        insertCmd.CommandText = "INSERT INTO ingest_runs (run_id, started_utc, status) VALUES ('run1', '2024-01-01', 'running')";
        await insertCmd.ExecuteNonQueryAsync();

        var svc = new CrashRecoveryService(_tempDir);
        svc.AcquireLock();

        await svc.MarkRunsAbandonedAsync(conn);

        // Verify status changed
        using var checkCmd = conn.CreateCommand();
        checkCmd.CommandText = "SELECT status FROM ingest_runs WHERE run_id = 'run1'";
        var status = (string)(await checkCmd.ExecuteScalarAsync())!;
        status.Should().Be("abandoned");

        // Verify lock removed
        File.Exists(svc.LockFilePath).Should().BeFalse();
    }

    [Fact]
    public async Task CheckAsync_WithSegments_CountsIncompleteSegmentsWithoutResume()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        // Insert an incomplete run with segments
        using var runCmd = conn.CreateCommand();
        runCmd.CommandText = "INSERT INTO ingest_runs (run_id, started_utc, status) VALUES ('run1', '2024-01-01', 'running')";
        await runCmd.ExecuteNonQueryAsync();

        using var segCmd = conn.CreateCommand();
        segCmd.CommandText = """
            INSERT INTO segments (segment_id, logical_source_id, physical_file_id, row_count, last_ingest_run_id, active)
            VALUES ('seg1', 'src1', 'file1', 100, 'run1', true),
                   ('seg2', 'src1', 'file2', 200, 'run1', true)
            """;
        await segCmd.ExecuteNonQueryAsync();

        var svc = new CrashRecoveryService(_tempDir);
        var status = await svc.CheckAsync(conn);

        status.CrashDetected.Should().BeFalse();
        status.CanResume.Should().BeFalse();
        status.IncompleteSegmentCount.Should().Be(2);
    }

    [Fact]
    public async Task CheckAsync_WithResumableSources_SurfacesResume()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        using var runCmd = conn.CreateCommand();
        runCmd.CommandText = "INSERT INTO ingest_runs (run_id, started_utc, status) VALUES ('run1', '2024-01-01', 'running')";
        await runCmd.ExecuteNonQueryAsync();

        using var sourceCmd = conn.CreateCommand();
        sourceCmd.CommandText = """
            INSERT INTO ingest_run_sources (run_id, source_path, source_order)
            VALUES ('run1', 'C:\logs\app.log', 0),
                   ('run1', 'C:\logs\worker.log', 1)
            """;
        await sourceCmd.ExecuteNonQueryAsync();

        var svc = new CrashRecoveryService(_tempDir);
        var status = await svc.CheckAsync(conn);

        status.CrashDetected.Should().BeTrue();
        status.CanResume.Should().BeTrue();
        status.ResumableSourcePaths.Should().ContainInOrder(@"C:\logs\app.log", @"C:\logs\worker.log");
    }

    [Fact]
    public void IsProcessRunning_CurrentProcess_ReturnsTrue()
    {
        CrashRecoveryService.IsProcessRunning(Environment.ProcessId).Should().BeTrue();
    }

    [Fact]
    public void IsProcessRunning_NonexistentPid_ReturnsFalse()
    {
        CrashRecoveryService.IsProcessRunning(99999999).Should().BeFalse();
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
