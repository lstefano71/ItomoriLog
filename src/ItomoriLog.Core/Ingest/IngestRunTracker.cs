using DuckDB.NET.Data;

namespace ItomoriLog.Core.Ingest;

public sealed class IngestRunTracker
{
    private readonly DuckDBConnection _connection;

    public IngestRunTracker(DuckDBConnection connection) => _connection = connection;

    public async Task<string> StartRunAsync(CancellationToken ct = default)
    {
        var runId = Guid.NewGuid().ToString("N");
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "INSERT INTO ingest_runs (run_id, started_utc, status) VALUES ($1, $2, $3)";
        cmd.Parameters.Add(new DuckDBParameter { Value = runId });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTimeOffset.UtcNow.UtcDateTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = "running" });
        await cmd.ExecuteNonQueryAsync(ct);
        return runId;
    }

    public async Task CompleteRunAsync(string runId, CancellationToken ct = default)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "UPDATE ingest_runs SET completed_utc = $1, status = $2 WHERE run_id = $3";
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTimeOffset.UtcNow.UtcDateTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = "completed" });
        cmd.Parameters.Add(new DuckDBParameter { Value = runId });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<IReadOnlyList<string>> GetInterruptedRunsAsync(CancellationToken ct = default)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT run_id FROM ingest_runs WHERE completed_utc IS NULL";
        using var reader = await cmd.ExecuteReaderAsync(ct);
        var runs = new List<string>();
        while (await reader.ReadAsync(ct))
            runs.Add(reader.GetString(0));
        return runs;
    }
}
