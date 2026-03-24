using DuckDB.NET.Data;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Core.Query;

/// <summary>
/// Queries distinct log levels and logical sources with counts,
/// respecting the same time window / filters as <see cref="QueryPlanner"/>.
/// </summary>
public sealed class FacetQuery
{
    private readonly DuckLakeConnectionFactory _factory;

    public FacetQuery(DuckLakeConnectionFactory factory)
    {
        _factory = factory;
    }

    /// <summary>
    /// Get distinct log levels with record counts.
    /// </summary>
    public async Task<FacetItem[]> QueryLevelsAsync(
        DateTimeOffset? startUtc = null,
        DateTimeOffset? endUtc = null,
        IReadOnlyList<string>? sourceIds = null,
        CancellationToken ct = default)
    {
        var (whereClause, parameters) = BuildWhereClause(startUtc, endUtc, sourceIds: sourceIds);

        var sql = $"""
            SELECT level, COUNT(*) AS cnt
            FROM logs
            {whereClause}
            GROUP BY level
            ORDER BY cnt DESC
            """;

        return await ExecuteFacetQueryAsync(sql, parameters, ct);
    }

    /// <summary>
    /// Get distinct logical source IDs with record counts.
    /// </summary>
    public async Task<FacetItem[]> QuerySourcesAsync(
        DateTimeOffset? startUtc = null,
        DateTimeOffset? endUtc = null,
        IReadOnlyList<string>? levels = null,
        CancellationToken ct = default)
    {
        var (whereClause, parameters) = BuildWhereClause(startUtc, endUtc, levels: levels);

        var sql = $"""
            SELECT logical_source_id, COUNT(*) AS cnt
            FROM logs
            {whereClause}
            GROUP BY logical_source_id
            ORDER BY cnt DESC
            """;

        return await ExecuteFacetQueryAsync(sql, parameters, ct);
    }

    private static (string WhereClause, List<object> Parameters) BuildWhereClause(
        DateTimeOffset? startUtc,
        DateTimeOffset? endUtc,
        IReadOnlyList<string>? levels = null,
        IReadOnlyList<string>? sourceIds = null)
    {
        var parameters = new List<object>();
        var clauses = new List<string>();

        if (startUtc.HasValue)
        {
            parameters.Add(startUtc.Value.UtcDateTime);
            clauses.Add($"timestamp_utc >= ${parameters.Count}");
        }
        if (endUtc.HasValue)
        {
            parameters.Add(endUtc.Value.UtcDateTime);
            clauses.Add($"timestamp_utc < ${parameters.Count}");
        }

        if (levels is { Count: > 0 })
        {
            var placeholders = new List<string>();
            foreach (var level in levels)
            {
                parameters.Add(level);
                placeholders.Add($"${parameters.Count}");
            }
            clauses.Add($"level IN ({string.Join(", ", placeholders)})");
        }

        if (sourceIds is { Count: > 0 })
        {
            var placeholders = new List<string>();
            foreach (var sourceId in sourceIds)
            {
                parameters.Add(sourceId);
                placeholders.Add($"${parameters.Count}");
            }
            clauses.Add($"logical_source_id IN ({string.Join(", ", placeholders)})");
        }

        var where = clauses.Count > 0
            ? "WHERE " + string.Join(" AND ", clauses)
            : "";

        return (where, parameters);
    }

    private async Task<FacetItem[]> ExecuteFacetQueryAsync(
        string sql,
        List<object> parameters,
        CancellationToken ct)
    {
        var conn = await _factory.GetConnectionAsync(ct);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters)
            cmd.Parameters.Add(new DuckDBParameter { Value = p });

        var items = new List<FacetItem>();
        using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            var value = reader.IsDBNull(0) ? "(none)" : reader.GetString(0);
            var count = reader.GetInt64(1);
            items.Add(new FacetItem(value, count));
        }

        return items.ToArray();
    }
}
