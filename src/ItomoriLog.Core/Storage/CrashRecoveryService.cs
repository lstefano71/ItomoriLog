using System.Diagnostics;
using System.Text.Json;
using DuckDB.NET.Data;

namespace ItomoriLog.Core.Storage;

public sealed record LockInfo(int Pid, DateTime TimestampUtc);

public sealed record CrashRecoveryStatus(
    bool CrashDetected,
    int IncompleteSegmentCount,
    IReadOnlyList<string> IncompleteRunIds,
    IReadOnlyList<string> ResumableSourcePaths)
{
    public bool CanResume => ResumableSourcePaths.Count > 0;
}

public sealed class CrashRecoveryService
{
    private const string LockFileName = ".lock";
    private readonly string _sessionFolder;

    public CrashRecoveryService(string sessionFolder)
    {
        _sessionFolder = sessionFolder;
    }

    public string LockFilePath => Path.Combine(_sessionFolder, LockFileName);

    public void AcquireLock()
    {
        var info = new LockInfo(Environment.ProcessId, DateTime.UtcNow);
        var json = JsonSerializer.Serialize(info);
        File.WriteAllText(LockFilePath, json);
    }

    public void ReleaseLock()
    {
        if (File.Exists(LockFilePath))
            File.Delete(LockFilePath);
    }

    public bool IsLockStale()
    {
        if (!File.Exists(LockFilePath))
            return false;

        try
        {
            var json = File.ReadAllText(LockFilePath);
            var info = JsonSerializer.Deserialize<LockInfo>(json);
            if (info is null) return true;

            return !IsProcessRunning(info.Pid);
        }
        catch
        {
            // Corrupt lockfile → treat as stale
            return true;
        }
    }

    public LockInfo? ReadLock()
    {
        if (!File.Exists(LockFilePath))
            return null;

        try
        {
            var json = File.ReadAllText(LockFilePath);
            return JsonSerializer.Deserialize<LockInfo>(json);
        }
        catch
        {
            return null;
        }
    }

    public async Task<CrashRecoveryStatus> CheckAsync(DuckDBConnection connection, CancellationToken ct = default)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT run_id FROM ingest_runs WHERE status = 'running'";
        using var reader = await cmd.ExecuteReaderAsync(ct);

        var incompleteRuns = new List<string>();
        while (await reader.ReadAsync(ct))
            incompleteRuns.Add(reader.GetString(0));

        var resumableSourcePaths = await ReadResumableSourcePathsAsync(connection, ct);
        var segmentCount = 0;
        if (incompleteRuns.Count > 0)
        {
            using var segCmd = connection.CreateCommand();
            // Count segments that belong to incomplete runs
            segCmd.CommandText = "SELECT COUNT(*) FROM segments s INNER JOIN ingest_runs r ON s.last_ingest_run_id = r.run_id WHERE r.status = 'running'";
            var count = await segCmd.ExecuteScalarAsync(ct);
            segmentCount = Convert.ToInt32(count);
        }

        return new CrashRecoveryStatus(
            CrashDetected: resumableSourcePaths.Count > 0,
            IncompleteSegmentCount: segmentCount,
            IncompleteRunIds: incompleteRuns,
            ResumableSourcePaths: resumableSourcePaths);
    }

    public async Task MarkRunsAbandonedAsync(DuckDBConnection connection, CancellationToken ct = default)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            UPDATE ingest_runs
            SET status = 'abandoned', completed_utc = $1
            WHERE status = 'running'
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        await cmd.ExecuteNonQueryAsync(ct);

        ReleaseLock();
    }

    public async Task<IReadOnlyList<string>> RecoverInterruptedIngestionAsync(
        DuckDBConnection connection,
        CancellationToken ct = default)
    {
        var status = await CheckAsync(connection, ct);
        if (!status.CanResume)
            return [];

        await MarkRunsAbandonedAsync(connection, ct);
        return status.ResumableSourcePaths;
    }

    internal static bool IsProcessRunning(int pid)
    {
        try
        {
            var process = Process.GetProcessById(pid);
            return !process.HasExited;
        }
        catch (ArgumentException)
        {
            // Process does not exist
            return false;
        }
    }

    private static async Task<IReadOnlyList<string>> ReadResumableSourcePathsAsync(
        DuckDBConnection connection,
        CancellationToken ct)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT source_path
            FROM ingest_run_sources
            WHERE run_id IN (
                SELECT run_id
                FROM ingest_runs
                WHERE status = 'running'
            )
            ORDER BY source_order, source_path
            """;

        var results = new List<string>();
        using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            results.Add(reader.GetString(0));
        return results;
    }
}
