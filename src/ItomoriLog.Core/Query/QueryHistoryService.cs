using DuckDB.NET.Data;

namespace ItomoriLog.Core.Query;

/// <summary>
/// Manages query history in both the global store and per-session databases.
/// </summary>
public sealed class QueryHistoryService
{
    private readonly DuckDBConnection _sessionConnection;

    public QueryHistoryService(DuckDBConnection sessionConnection)
    {
        _sessionConnection = sessionConnection;
    }

    /// <summary>
    /// Ensures the per-session query_history table exists.
    /// </summary>
    public async Task EnsureSchemaAsync(CancellationToken ct = default)
    {
        using var cmd = _sessionConnection.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS query_history (
                id           INTEGER PRIMARY KEY,
                query_text   VARCHAR NOT NULL,
                executed_utc TIMESTAMP NOT NULL,
                result_count BIGINT
            );
            CREATE SEQUENCE IF NOT EXISTS seq_session_qh START 1;
            """;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Records a query execution in the per-session history.
    /// </summary>
    public async Task RecordQueryAsync(string queryText, long? resultCount = null, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(queryText)) return;

        using var cmd = _sessionConnection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO query_history (id, query_text, executed_utc, result_count)
            VALUES (nextval('seq_session_qh'), $1, $2, $3)
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = queryText });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTimeOffset.UtcNow.UtcDateTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = resultCount.HasValue ? (object)resultCount.Value : DBNull.Value });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Returns the most recent queries from the per-session history.
    /// </summary>
    public async Task<IReadOnlyList<QueryHistoryEntry>> GetRecentAsync(int limit = 50, CancellationToken ct = default)
    {
        using var cmd = _sessionConnection.CreateCommand();
        cmd.CommandText = $"SELECT id, query_text, executed_utc, result_count FROM query_history ORDER BY executed_utc DESC LIMIT {limit}";

        using var reader = await cmd.ExecuteReaderAsync(ct);
        var results = new List<QueryHistoryEntry>();
        while (await reader.ReadAsync(ct))
        {
            results.Add(new QueryHistoryEntry(
                Id: reader.GetInt32(0),
                QueryText: reader.GetString(1),
                ExecutedUtc: reader.GetDateTime(2),
                ResultCount: reader.IsDBNull(3) ? null : reader.GetInt64(3)));
        }
        return results;
    }

    /// <summary>
    /// Searches query history by text substring.
    /// </summary>
    public async Task<IReadOnlyList<QueryHistoryEntry>> SearchAsync(string searchText, int limit = 50, CancellationToken ct = default)
    {
        using var cmd = _sessionConnection.CreateCommand();
        cmd.CommandText = "SELECT id, query_text, executed_utc, result_count FROM query_history WHERE query_text ILIKE $1 ORDER BY executed_utc DESC LIMIT $2";
        cmd.Parameters.Add(new DuckDBParameter { Value = $"%{searchText}%" });
        cmd.Parameters.Add(new DuckDBParameter { Value = limit });

        using var reader = await cmd.ExecuteReaderAsync(ct);
        var results = new List<QueryHistoryEntry>();
        while (await reader.ReadAsync(ct))
        {
            results.Add(new QueryHistoryEntry(
                Id: reader.GetInt32(0),
                QueryText: reader.GetString(1),
                ExecutedUtc: reader.GetDateTime(2),
                ResultCount: reader.IsDBNull(3) ? null : reader.GetInt64(3)));
        }
        return results;
    }
}

public sealed record QueryHistoryEntry(
    int Id,
    string QueryText,
    DateTime ExecutedUtc,
    long? ResultCount);
