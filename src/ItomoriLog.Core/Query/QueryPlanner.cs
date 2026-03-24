using System.Text;

namespace ItomoriLog.Core.Query;

/// <summary>
/// Builds parameterized DuckDB SQL from a <see cref="FilterState"/>, optional keyset cursor,
/// and page direction. Never uses OFFSET — always keyset pagination.
/// </summary>
public sealed class QueryPlanner
{
    private readonly ITickCompiler _tickCompiler;
    private readonly TickSqlEmitter _tickEmitter;

    public QueryPlanner(ITickCompiler tickCompiler, TickSqlEmitter tickEmitter)
    {
        _tickCompiler = tickCompiler;
        _tickEmitter = tickEmitter;
    }

    public QueryPlanner() : this(new TickCompiler(), new TickSqlEmitter()) { }

    private const string SelectColumns = """
        timestamp_utc, timestamp_basis, timestamp_effective_offset_minutes,
        timestamp_original, logical_source_id, source_path,
        physical_file_id, segment_id, ingest_run_id,
        record_index, level, message, fields
        """;

    public QueryResult Plan(
        FilterState filter,
        PageCursor? cursor = null,
        PageDirection direction = PageDirection.Forward,
        int pageSize = 2000,
        TickContext? tickContext = null)
    {
        var parameters = new List<object>();
        var whereClauses = new List<string>();
        string? setupSql = null;

        // Time window filter
        if (filter.StartUtc.HasValue)
        {
            parameters.Add(filter.StartUtc.Value.UtcDateTime);
            whereClauses.Add($"timestamp_utc >= ${parameters.Count}");
        }
        if (filter.EndUtc.HasValue)
        {
            parameters.Add(filter.EndUtc.Value.UtcDateTime);
            whereClauses.Add($"timestamp_utc < ${parameters.Count}");
        }

        // Source filter
        if (filter.SourceIds is { Count: > 0 })
        {
            var placeholders = new List<string>();
            foreach (var sourceId in filter.SourceIds)
            {
                parameters.Add(sourceId);
                placeholders.Add($"${parameters.Count}");
            }
            whereClauses.Add($"logical_source_id IN ({string.Join(", ", placeholders)})");
        }

        // Level filter
        if (filter.Levels is { Count: > 0 })
        {
            var placeholders = new List<string>();
            foreach (var level in filter.Levels)
            {
                parameters.Add(level);
                placeholders.Add($"${parameters.Count}");
            }
            whereClauses.Add($"level IN ({string.Join(", ", placeholders)})");
        }

        // Text search (ILIKE)
        if (!string.IsNullOrWhiteSpace(filter.TextSearch))
        {
            parameters.Add($"%{filter.TextSearch}%");
            whereClauses.Add($"message ILIKE ${parameters.Count}");
        }

        // TICK expression integration
        if (!string.IsNullOrWhiteSpace(filter.TickExpression))
        {
            var ctx = tickContext ?? new TickContext(DateTimeOffset.UtcNow);
            var tickResult = _tickCompiler.Compile(filter.TickExpression, ctx);
            var emission = _tickEmitter.Emit(tickResult.Intervals);

            // Rebase emission parameter indices to account for existing parameters
            var rebasedWhere = RebaseParameterIndices(emission.WhereSql, parameters.Count);
            foreach (var p in emission.Parameters)
                parameters.Add(p);

            whereClauses.Add(rebasedWhere);
            if (emission.SetupSql is not null)
                setupSql = emission.SetupSql;
        }

        // Keyset cursor
        if (cursor is not null)
        {
            parameters.Add(cursor.TimestampUtc.UtcDateTime);
            var tsIdx = parameters.Count;
            parameters.Add(cursor.SegmentId);
            var segIdx = parameters.Count;
            parameters.Add(cursor.RecordIndex);
            var recIdx = parameters.Count;

            var op = direction == PageDirection.Forward ? ">" : "<";
            whereClauses.Add(
                $"(timestamp_utc, segment_id, record_index) {op} (${tsIdx}, ${segIdx}, ${recIdx})");
        }

        // Build SQL
        var sb = new StringBuilder();
        sb.Append($"SELECT {SelectColumns} FROM logs");

        if (whereClauses.Count > 0)
            sb.Append($" WHERE {string.Join(" AND ", whereClauses)}");

        var orderDir = direction == PageDirection.Forward ? "ASC" : "DESC";
        sb.Append($" ORDER BY timestamp_utc {orderDir}, segment_id {orderDir}, record_index {orderDir}");

        parameters.Add(pageSize);
        sb.Append($" LIMIT ${parameters.Count}");

        return new QueryResult(sb.ToString(), parameters, direction, setupSql);
    }

    /// <summary>
    /// Rebases $1, $2, ... parameter indices in SQL by the given offset so they
    /// don't collide with already-added parameters.
    /// </summary>
    internal static string RebaseParameterIndices(string sql, int offset)
    {
        if (offset == 0) return sql;

        // Replace $N with $(N+offset), processing from highest N downward to avoid double-replacement
        var maxParam = 0;
        for (int i = 0; i < sql.Length - 1; i++)
        {
            if (sql[i] == '$' && char.IsDigit(sql[i + 1]))
            {
                int j = i + 1;
                while (j < sql.Length && char.IsDigit(sql[j])) j++;
                var num = int.Parse(sql[(i + 1)..j]);
                if (num > maxParam) maxParam = num;
            }
        }

        var result = sql;
        for (int n = maxParam; n >= 1; n--)
            result = result.Replace($"${n}", $"${n + offset}");

        return result;
    }
}
