namespace ItomoriLog.Core.Query;

internal sealed class FilterSqlBuilder
{
    private readonly ITickCompiler _tickCompiler;
    private readonly TickSqlEmitter _tickEmitter;
    private readonly SearchQuerySqlEmitter _searchEmitter;

    public FilterSqlBuilder(
        ITickCompiler tickCompiler,
        TickSqlEmitter tickEmitter,
        SearchQuerySqlEmitter searchEmitter)
    {
        _tickCompiler = tickCompiler;
        _tickEmitter = tickEmitter;
        _searchEmitter = searchEmitter;
    }

    public SqlEmission Build(
        FilterState filter,
        TickContext? tickContext = null,
        string timestampColumn = "timestamp_utc",
        string sourceIdColumn = "logical_source_id",
        string levelColumn = "level",
        string messageColumn = "message")
    {
        var parameters = new List<object>();
        var whereClauses = new List<string>();
        string? setupSql = null;

        if (filter.StartUtc.HasValue) {
            parameters.Add(filter.StartUtc.Value.UtcDateTime);
            whereClauses.Add($"{timestampColumn} >= ${parameters.Count}");
        }

        if (filter.EndUtc.HasValue) {
            parameters.Add(filter.EndUtc.Value.UtcDateTime);
            whereClauses.Add($"{timestampColumn} < ${parameters.Count}");
        }

        if (filter.SourceIds is { Count: > 0 }) {
            var placeholders = new List<string>();
            foreach (var sourceId in filter.SourceIds) {
                parameters.Add(sourceId);
                placeholders.Add($"${parameters.Count}");
            }

            whereClauses.Add($"{sourceIdColumn} IN ({string.Join(", ", placeholders)})");
        }

        if (filter.ExcludedSourceIds is { Count: > 0 }) {
            var placeholders = new List<string>();
            foreach (var sourceId in filter.ExcludedSourceIds) {
                parameters.Add(sourceId);
                placeholders.Add($"${parameters.Count}");
            }

            whereClauses.Add($"{sourceIdColumn} NOT IN ({string.Join(", ", placeholders)})");
        }

        if (filter.Levels is { Count: > 0 }) {
            var placeholders = new List<string>();
            foreach (var level in filter.Levels) {
                parameters.Add(level);
                placeholders.Add($"${parameters.Count}");
            }

            whereClauses.Add($"{levelColumn} IN ({string.Join(", ", placeholders)})");
        }

        if (filter.ExcludedLevels is { Count: > 0 }) {
            var placeholders = new List<string>();
            foreach (var level in filter.ExcludedLevels) {
                parameters.Add(level);
                placeholders.Add($"${parameters.Count}");
            }

            whereClauses.Add($"({levelColumn} IS NULL OR {levelColumn} NOT IN ({string.Join(", ", placeholders)}))");
        }

        if (filter.TextSearchQuery is not null) {
            var emission = _searchEmitter.Emit(filter.TextSearchQuery, messageColumn);
            var rebasedWhere = QueryPlanner.RebaseParameterIndices(emission.WhereSql, parameters.Count);
            foreach (var parameter in emission.Parameters)
                parameters.Add(parameter);

            whereClauses.Add(rebasedWhere);
        } else if (!string.IsNullOrWhiteSpace(filter.TextSearch)) {
            parameters.Add($"%{filter.TextSearch}%");
            whereClauses.Add($"{messageColumn} ILIKE ${parameters.Count}");
        }

        if (!string.IsNullOrWhiteSpace(filter.TickExpression)) {
            var context = tickContext ?? new TickContext(DateTimeOffset.UtcNow);
            var tickResult = _tickCompiler.Compile(filter.TickExpression, context);
            if (!string.IsNullOrWhiteSpace(tickResult.Warning))
                throw new InvalidOperationException(tickResult.Warning);

            var emission = _tickEmitter.Emit(tickResult.Intervals, timestampColumn);
            var rebasedWhere = QueryPlanner.RebaseParameterIndices(emission.WhereSql, parameters.Count);
            foreach (var parameter in emission.Parameters)
                parameters.Add(parameter);

            whereClauses.Add(rebasedWhere);
            if (emission.SetupSql is not null)
                setupSql = emission.SetupSql;
        }

        return new SqlEmission(string.Join(" AND ", whereClauses), parameters, setupSql);
    }
}
