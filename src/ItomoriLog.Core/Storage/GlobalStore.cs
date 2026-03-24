using DuckDB.NET.Data;

namespace ItomoriLog.Core.Storage;

public sealed class GlobalStore : IDisposable
{
    private readonly string _dbPath;
    private DuckDBConnection? _connection;
    private bool _disposed;

    public GlobalStore(string? appDataRoot = null)
    {
        var root = appDataRoot ?? Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            ".itomorilog");
        Directory.CreateDirectory(root);
        _dbPath = Path.Combine(root, "global.duckdb");
    }

    private async Task<DuckDBConnection> GetConnectionAsync(CancellationToken ct = default)
    {
        if (_connection is { State: System.Data.ConnectionState.Open })
            return _connection;

        _connection = new DuckDBConnection($"Data Source={_dbPath}");
        await _connection.OpenAsync(ct);
        await EnsureSchemaAsync(_connection, ct);
        return _connection;
    }

    private static async Task EnsureSchemaAsync(DuckDBConnection conn, CancellationToken ct)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS recent_sessions (
                session_folder VARCHAR PRIMARY KEY,
                title          VARCHAR NOT NULL,
                description    VARCHAR,
                last_opened_utc TIMESTAMP NOT NULL
            );

            CREATE TABLE IF NOT EXISTS fkb_rules (
                rule_id     VARCHAR PRIMARY KEY,
                rule_type   VARCHAR NOT NULL,
                config      JSON NOT NULL,
                created_utc TIMESTAMP NOT NULL,
                confidence  DOUBLE NOT NULL DEFAULT 1.0
            );

            CREATE TABLE IF NOT EXISTS query_history (
                id          INTEGER PRIMARY KEY,
                query_text  VARCHAR NOT NULL,
                session_id  VARCHAR,
                executed_utc TIMESTAMP NOT NULL,
                result_count BIGINT
            );
            CREATE SEQUENCE IF NOT EXISTS seq_query_history START 1;

            CREATE TABLE IF NOT EXISTS preferences (
                key   VARCHAR PRIMARY KEY,
                value VARCHAR NOT NULL
            );
            """;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    // --- Recent Sessions ---

    public async Task AddRecentSessionAsync(string sessionFolder, string title, string? description = null, CancellationToken ct = default)
    {
        var conn = await GetConnectionAsync(ct);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO recent_sessions (session_folder, title, description, last_opened_utc)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (session_folder) DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                last_opened_utc = EXCLUDED.last_opened_utc
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = sessionFolder });
        cmd.Parameters.Add(new DuckDBParameter { Value = title });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)description ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTimeOffset.UtcNow.UtcDateTime });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<IReadOnlyList<RecentSessionEntry>> GetRecentSessionsAsync(int limit = 50, CancellationToken ct = default)
    {
        var conn = await GetConnectionAsync(ct);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT session_folder, title, description, last_opened_utc FROM recent_sessions ORDER BY last_opened_utc DESC LIMIT {limit}";

        using var reader = await cmd.ExecuteReaderAsync(ct);
        var results = new List<RecentSessionEntry>();
        while (await reader.ReadAsync(ct))
        {
            results.Add(new RecentSessionEntry(
                SessionFolder: reader.GetString(0),
                Title: reader.GetString(1),
                Description: reader.IsDBNull(2) ? null : reader.GetString(2),
                LastOpenedUtc: reader.GetDateTime(3)));
        }
        return results;
    }

    public async Task<int> PruneDeadSessionsAsync(CancellationToken ct = default)
    {
        var sessions = await GetRecentSessionsAsync(1000, ct);
        var dead = sessions.Where(s =>
            !File.Exists(Path.Combine(s.SessionFolder, SessionPaths.DefaultDbFileName))).ToList();

        if (dead.Count == 0) return 0;

        var conn = await GetConnectionAsync(ct);
        foreach (var d in dead)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM recent_sessions WHERE session_folder = $1";
            cmd.Parameters.Add(new DuckDBParameter { Value = d.SessionFolder });
            await cmd.ExecuteNonQueryAsync(ct);
        }
        return dead.Count;
    }

    // --- Query History ---

    public async Task AddQueryHistoryAsync(string queryText, string? sessionId = null, long? resultCount = null, CancellationToken ct = default)
    {
        var conn = await GetConnectionAsync(ct);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO query_history (id, query_text, session_id, executed_utc, result_count)
            VALUES (nextval('seq_query_history'), $1, $2, $3, $4)
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = queryText });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)sessionId ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTimeOffset.UtcNow.UtcDateTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = resultCount.HasValue ? (object)resultCount.Value : DBNull.Value });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<IReadOnlyList<GlobalQueryHistoryEntry>> GetRecentQueriesAsync(int limit = 50, CancellationToken ct = default)
    {
        var conn = await GetConnectionAsync(ct);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT id, query_text, session_id, executed_utc, result_count FROM query_history ORDER BY executed_utc DESC LIMIT {limit}";

        using var reader = await cmd.ExecuteReaderAsync(ct);
        var results = new List<GlobalQueryHistoryEntry>();
        while (await reader.ReadAsync(ct))
        {
            results.Add(new GlobalQueryHistoryEntry(
                Id: reader.GetInt32(0),
                QueryText: reader.GetString(1),
                SessionId: reader.IsDBNull(2) ? null : reader.GetString(2),
                ExecutedUtc: reader.GetDateTime(3),
                ResultCount: reader.IsDBNull(4) ? null : reader.GetInt64(4)));
        }
        return results;
    }

    public async Task<IReadOnlyList<GlobalQueryHistoryEntry>> SearchQueriesAsync(string searchText, int limit = 50, CancellationToken ct = default)
    {
        var conn = await GetConnectionAsync(ct);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, query_text, session_id, executed_utc, result_count FROM query_history WHERE query_text ILIKE $1 ORDER BY executed_utc DESC LIMIT $2";
        cmd.Parameters.Add(new DuckDBParameter { Value = $"%{searchText}%" });
        cmd.Parameters.Add(new DuckDBParameter { Value = limit });

        using var reader = await cmd.ExecuteReaderAsync(ct);
        var results = new List<GlobalQueryHistoryEntry>();
        while (await reader.ReadAsync(ct))
        {
            results.Add(new GlobalQueryHistoryEntry(
                Id: reader.GetInt32(0),
                QueryText: reader.GetString(1),
                SessionId: reader.IsDBNull(2) ? null : reader.GetString(2),
                ExecutedUtc: reader.GetDateTime(3),
                ResultCount: reader.IsDBNull(4) ? null : reader.GetInt64(4)));
        }
        return results;
    }

    // --- Preferences ---

    public async Task SetPreferenceAsync(string key, string value, CancellationToken ct = default)
    {
        var conn = await GetConnectionAsync(ct);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO preferences (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = key });
        cmd.Parameters.Add(new DuckDBParameter { Value = value });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<string?> GetPreferenceAsync(string key, CancellationToken ct = default)
    {
        var conn = await GetConnectionAsync(ct);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT value FROM preferences WHERE key = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = key });
        var result = await cmd.ExecuteScalarAsync(ct);
        return result as string;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection?.Dispose();
    }
}

public sealed record RecentSessionEntry(
    string SessionFolder,
    string Title,
    string? Description,
    DateTimeOffset LastOpenedUtc);

public sealed record GlobalQueryHistoryEntry(
    int Id,
    string QueryText,
    string? SessionId,
    DateTime ExecutedUtc,
    long? ResultCount);
