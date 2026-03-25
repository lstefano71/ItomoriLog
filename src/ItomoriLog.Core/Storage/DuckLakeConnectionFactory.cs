using DuckDB.NET.Data;

namespace ItomoriLog.Core.Storage;

public sealed class DuckLakeConnectionFactory : IDisposable
{
    private const string CatalogAlias = "itomori_session";
    private readonly string _dbPath;
    private readonly string _sessionFolder;
    private readonly string _dataPath;
    private DuckDBConnection? _connection;
    private bool _disposed;

    public DuckLakeConnectionFactory(string dbPath)
    {
        _dbPath = Path.GetFullPath(dbPath);
        _sessionFolder = Path.GetDirectoryName(_dbPath)
            ?? throw new ArgumentException("Session database path must include a parent folder.", nameof(dbPath));
        _dataPath = SessionPaths.GetDuckLakeDataPath(_sessionFolder);
    }

    public async Task<DuckDBConnection> GetConnectionAsync(CancellationToken ct = default)
    {
        if (_connection is { State: System.Data.ConnectionState.Open })
            return _connection;

        Directory.CreateDirectory(_sessionFolder);
        Directory.CreateDirectory(_dataPath);
        var createCatalog = !File.Exists(_dbPath);

        _connection = new DuckDBConnection("Data Source=:memory:");
        await _connection.OpenAsync(ct);

        await EnsureDuckLakeExtensionAsync(_connection, ct);
        await AttachDuckLakeAsync(_connection, createCatalog, ct);
        await DuckLakeSessionMaintenance.ApplyPreferredOptionsAsync(_connection, CatalogAlias, ct);

        return _connection;
    }

    private async Task EnsureDuckLakeExtensionAsync(DuckDBConnection connection, CancellationToken ct)
    {
        try
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "LOAD ducklake;";
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (Exception loadException)
        {
            try
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = "INSTALL ducklake; LOAD ducklake;";
                await cmd.ExecuteNonQueryAsync(ct);
            }
            catch (Exception installException)
            {
                throw new InvalidOperationException(
                    $"DuckLake support is required for session storage but the extension could not be loaded or installed for '{_dbPath}'.",
                    new AggregateException(loadException, installException));
            }
        }
    }

    private async Task AttachDuckLakeAsync(DuckDBConnection connection, bool createCatalog, CancellationToken ct)
    {
        var catalogPath = EscapeSqlLiteral(_dbPath);
        var optionClause = createCatalog
            // DuckLake resolves the ATTACH DATA_PATH parameter from the process context,
            // so use the session's absolute data folder to keep Parquet under the session.
            ? $" (DATA_PATH '{EscapeSqlLiteral(_dataPath)}')"
            : string.Empty;

        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            ATTACH 'ducklake:{catalogPath}' AS {CatalogAlias}{optionClause};
            USE {CatalogAlias};
            """;

        try
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to attach DuckLake catalog '{_dbPath}' with data path '{_dataPath}'.",
                ex);
        }
    }

    private static string EscapeSqlLiteral(string value) =>
        value.Replace("'", "''", StringComparison.Ordinal);

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection?.Dispose();
    }
}
