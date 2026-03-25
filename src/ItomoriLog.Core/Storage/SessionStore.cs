using DuckDB.NET.Data;
using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Storage;

public sealed class SessionStore
{
    private readonly DuckLakeConnectionFactory _factory;

    public SessionStore(DuckLakeConnectionFactory factory)
    {
        _factory = factory;
    }

    public async Task InitializeAsync(string title, string? description, string? defaultTimezone, CancellationToken ct = default)
    {
        var conn = await _factory.GetConnectionAsync(ct);
        await SchemaInitializer.EnsureSchemaAsync(conn, ct);

        var sessionId = Guid.NewGuid().ToString("N");
        var now = DateTimeOffset.UtcNow;

        using (var clearCmd = conn.CreateCommand())
        {
            clearCmd.CommandText = "DELETE FROM session";
            await clearCmd.ExecuteNonQueryAsync(ct);
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO session (session_id, created_utc, modified_utc, title, description, created_by, default_timezone, app_version)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = sessionId });
        cmd.Parameters.Add(new DuckDBParameter { Value = now.UtcDateTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = now.UtcDateTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = title });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)description ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = Environment.UserName });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)defaultTimezone ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = "ItomoriLog 0.1.0" });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<SessionHeader?> ReadHeaderAsync(CancellationToken ct = default)
    {
        var conn = await _factory.GetConnectionAsync(ct);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT session_id, created_utc, modified_utc, title, description, created_by, default_timezone, app_version
            FROM session
            ORDER BY modified_utc DESC, created_utc DESC
            LIMIT 1
            """;

        using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        return new SessionHeader(
            SessionId: reader.GetString(0),
            CreatedUtc: reader.GetDateTime(1),
            ModifiedUtc: reader.GetDateTime(2),
            Title: reader.GetString(3),
            Description: reader.IsDBNull(4) ? null : reader.GetString(4),
            CreatedBy: reader.IsDBNull(5) ? null : reader.GetString(5),
            DefaultTimezone: reader.IsDBNull(6) ? null : reader.GetString(6),
            AppVersion: reader.IsDBNull(7) ? null : reader.GetString(7));
    }

    public async Task UpdateHeaderAsync(string? title = null, string? description = null, string? defaultTimezone = null, CancellationToken ct = default)
    {
        var conn = await _factory.GetConnectionAsync(ct);

        var sets = new List<string>();
        var parameters = new List<DuckDBParameter>();

        if (title is not null)
        {
            sets.Add($"title = ${parameters.Count + 1}");
            parameters.Add(new DuckDBParameter { Value = title });
        }
        if (description is not null)
        {
            sets.Add($"description = ${parameters.Count + 1}");
            parameters.Add(new DuckDBParameter { Value = description });
        }
        if (defaultTimezone is not null)
        {
            sets.Add($"default_timezone = ${parameters.Count + 1}");
            parameters.Add(new DuckDBParameter { Value = defaultTimezone });
        }

        if (sets.Count == 0) return;

        sets.Add($"modified_utc = ${parameters.Count + 1}");
        parameters.Add(new DuckDBParameter { Value = DateTimeOffset.UtcNow.UtcDateTime });

        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"UPDATE session SET {string.Join(", ", sets)}";
        foreach (var p in parameters)
            cmd.Parameters.Add(p);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
