using DuckDB.NET.Data;

namespace ItomoriLog.Core.Storage;

public sealed class DuckLakeConnectionFactory : IDisposable
{
    private readonly string _dbPath;
    private DuckDBConnection? _connection;
    private bool _disposed;

    public DuckLakeConnectionFactory(string dbPath)
    {
        _dbPath = dbPath;
    }

    public async Task<DuckDBConnection> GetConnectionAsync(CancellationToken ct = default)
    {
        if (_connection is { State: System.Data.ConnectionState.Open })
            return _connection;

        _connection = new DuckDBConnection($"Data Source={_dbPath}");
        await _connection.OpenAsync(ct);

        // Install and load DuckLake extension
        try
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "INSTALL ducklake; LOAD ducklake;";
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch
        {
            // DuckLake may not be available — fall back to plain DuckDB
        }

        return _connection;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection?.Dispose();
    }
}
