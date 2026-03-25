using DuckDB.NET.Data;

using ItomoriLog.Core.Storage;

namespace ItomoriLog.Core.Query;

/// <summary>
/// Queries DuckDB for time-bucketed record counts using <c>time_bucket</c>.
/// Supports progressive binning: coarse bins first, then refine on zoom.
/// </summary>
public sealed class TimelineQuery
{
    private readonly DuckLakeConnectionFactory _factory;
    private readonly FilterSqlBuilder _filterBuilder;

    public TimelineQuery(DuckLakeConnectionFactory factory)
    {
        _factory = factory;
        _filterBuilder = new FilterSqlBuilder(
            new TickCompiler(),
            new TickSqlEmitter(),
            new SearchQuerySqlEmitter());
    }

    /// <summary>
    /// Fetch timeline bins for the given time window and bin width.
    /// </summary>
    /// <param name="startUtc">Inclusive start of the time window, or null for unbounded.</param>
    /// <param name="endUtc">Exclusive end of the time window, or null for unbounded.</param>
    /// <param name="binWidth">Bin width as a TimeSpan (e.g. 1 hour, 5 minutes).</param>
    /// <param name="levels">Optional level filter (e.g. ["ERROR","WARN"]).</param>
    /// <param name="sourceIds">Optional source ID filter.</param>
    /// <param name="ct">Cancellation token.</param>
    public async Task<TimelineBin[]> QueryBinsAsync(
        DateTimeOffset? startUtc,
        DateTimeOffset? endUtc,
        TimeSpan binWidth,
        IReadOnlyList<string>? levels = null,
        IReadOnlyList<string>? sourceIds = null,
        FilterState? matchFilter = null,
        CancellationToken ct = default)
    {
        var parameters = new List<object>();
        var whereClauses = new List<string>();
        string? setupSql = null;

        if (startUtc.HasValue) {
            parameters.Add(startUtc.Value.UtcDateTime);
            whereClauses.Add($"timestamp_utc >= ${parameters.Count}");
        }
        if (endUtc.HasValue) {
            parameters.Add(endUtc.Value.UtcDateTime);
            whereClauses.Add($"timestamp_utc < ${parameters.Count}");
        }

        if (levels is { Count: > 0 }) {
            var placeholders = new List<string>();
            foreach (var level in levels) {
                parameters.Add(level);
                placeholders.Add($"${parameters.Count}");
            }
            whereClauses.Add($"level IN ({string.Join(", ", placeholders)})");
        }

        if (sourceIds is { Count: > 0 }) {
            var placeholders = new List<string>();
            foreach (var sourceId in sourceIds) {
                parameters.Add(sourceId);
                placeholders.Add($"${parameters.Count}");
            }
            whereClauses.Add($"logical_source_id IN ({string.Join(", ", placeholders)})");
        }

        var whereClause = whereClauses.Count > 0
            ? "WHERE " + string.Join(" AND ", whereClauses)
            : "";

        var intervalStr = FormatInterval(binWidth);
        var matchedCountSql = "CAST(0 AS BIGINT) AS matched_cnt";

        if (matchFilter is not null) {
            var filterEmission = _filterBuilder.Build(matchFilter);
            if (!string.IsNullOrWhiteSpace(filterEmission.WhereSql)) {
                var rebasedWhere = QueryPlanner.RebaseParameterIndices(filterEmission.WhereSql, parameters.Count);
                foreach (var parameter in filterEmission.Parameters)
                    parameters.Add(parameter);

                matchedCountSql = $"SUM(CASE WHEN {rebasedWhere} THEN 1 ELSE 0 END) AS matched_cnt";
                setupSql = filterEmission.SetupSql;
            }
        }

        var sql = $"""
            SELECT
                time_bucket(INTERVAL '{intervalStr}', timestamp_utc) AS bin_start,
                COUNT(*) AS cnt,
                mode(level) AS dominant_level,
                {matchedCountSql}
            FROM logs
            {whereClause}
            GROUP BY bin_start
            ORDER BY bin_start
            """;

        var conn = await _factory.GetConnectionAsync(ct);
        if (setupSql is not null) {
            using var setupCmd = conn.CreateCommand();
            setupCmd.CommandText = setupSql;
            await setupCmd.ExecuteNonQueryAsync(ct);
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters)
            cmd.Parameters.Add(new DuckDBParameter { Value = p });

        var bins = new List<TimelineBin>();
        using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct)) {
            var binStart = new DateTimeOffset(reader.GetDateTime(0), TimeSpan.Zero);
            var count = reader.GetInt64(1);
            var dominantLevel = reader.IsDBNull(2) ? null : reader.GetString(2);
            var matchedCount = reader.GetInt64(3);

            bins.Add(new TimelineBin(
                Start: binStart,
                End: binStart.Add(binWidth),
                Count: count,
                DominantLevel: dominantLevel,
                MatchedCount: matchedCount));
        }

        return bins.ToArray();
    }

    /// <summary>
    /// Gets the overall time range of logs matching optional filters.
    /// </summary>
    public async Task<(DateTimeOffset Min, DateTimeOffset Max)?> GetTimeRangeAsync(
        IReadOnlyList<string>? levels = null,
        IReadOnlyList<string>? sourceIds = null,
        CancellationToken ct = default)
    {
        var parameters = new List<object>();
        var whereClauses = new List<string>();

        if (levels is { Count: > 0 }) {
            var placeholders = new List<string>();
            foreach (var level in levels) {
                parameters.Add(level);
                placeholders.Add($"${parameters.Count}");
            }
            whereClauses.Add($"level IN ({string.Join(", ", placeholders)})");
        }

        if (sourceIds is { Count: > 0 }) {
            var placeholders = new List<string>();
            foreach (var sourceId in sourceIds) {
                parameters.Add(sourceId);
                placeholders.Add($"${parameters.Count}");
            }
            whereClauses.Add($"logical_source_id IN ({string.Join(", ", placeholders)})");
        }

        var whereClause = whereClauses.Count > 0
            ? "WHERE " + string.Join(" AND ", whereClauses)
            : "";

        var sql = $"SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM logs {whereClause}";

        var conn = await _factory.GetConnectionAsync(ct);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters)
            cmd.Parameters.Add(new DuckDBParameter { Value = p });

        using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct) && !reader.IsDBNull(0) && !reader.IsDBNull(1)) {
            var min = new DateTimeOffset(reader.GetDateTime(0), TimeSpan.Zero);
            var max = new DateTimeOffset(reader.GetDateTime(1), TimeSpan.Zero);
            return (min, max);
        }

        return null;
    }

    /// <summary>
    /// Choose an appropriate coarse bin width for a given time range.
    /// </summary>
    public static TimeSpan ChooseCoarseBinWidth(TimeSpan totalSpan)
    {
        if (totalSpan <= TimeSpan.FromMinutes(10))
            return TimeSpan.FromSeconds(10);
        if (totalSpan <= TimeSpan.FromHours(1))
            return TimeSpan.FromMinutes(1);
        if (totalSpan <= TimeSpan.FromHours(6))
            return TimeSpan.FromMinutes(5);
        if (totalSpan <= TimeSpan.FromDays(1))
            return TimeSpan.FromMinutes(30);
        if (totalSpan <= TimeSpan.FromDays(7))
            return TimeSpan.FromHours(1);
        if (totalSpan <= TimeSpan.FromDays(30))
            return TimeSpan.FromHours(6);
        return TimeSpan.FromDays(1);
    }

    /// <summary>
    /// Choose a finer bin width for zoomed-in views.
    /// </summary>
    public static TimeSpan ChooseFineBinWidth(TimeSpan visibleSpan)
    {
        if (visibleSpan <= TimeSpan.FromMinutes(2))
            return TimeSpan.FromSeconds(1);
        if (visibleSpan <= TimeSpan.FromMinutes(10))
            return TimeSpan.FromSeconds(5);
        if (visibleSpan <= TimeSpan.FromHours(1))
            return TimeSpan.FromSeconds(30);
        if (visibleSpan <= TimeSpan.FromHours(6))
            return TimeSpan.FromMinutes(1);
        if (visibleSpan <= TimeSpan.FromDays(1))
            return TimeSpan.FromMinutes(5);
        return ChooseCoarseBinWidth(visibleSpan);
    }

    internal static string FormatInterval(TimeSpan ts)
    {
        if (ts.TotalDays >= 1 && ts.TotalDays == Math.Floor(ts.TotalDays))
            return $"{(int)ts.TotalDays} days";
        if (ts.TotalHours >= 1 && ts.TotalHours == Math.Floor(ts.TotalHours))
            return $"{(int)ts.TotalHours} hours";
        if (ts.TotalMinutes >= 1 && ts.TotalMinutes == Math.Floor(ts.TotalMinutes))
            return $"{(int)ts.TotalMinutes} minutes";
        return $"{(int)ts.TotalSeconds} seconds";
    }
}
