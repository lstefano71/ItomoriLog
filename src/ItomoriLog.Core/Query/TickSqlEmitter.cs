namespace ItomoriLog.Core.Query;

public sealed class TickSqlEmitter
{
    public const int MaxOrChainIntervals = 64;

    public SqlEmission Emit(IReadOnlyList<UtcInterval> intervals, string timestampColumn = "timestamp_utc")
    {
        if (intervals.Count == 0)
            return new SqlEmission("FALSE", []);

        if (intervals.Count <= MaxOrChainIntervals)
            return EmitOrChain(intervals, timestampColumn);
        else
            return EmitTempTable(intervals, timestampColumn);
    }

    private static SqlEmission EmitOrChain(IReadOnlyList<UtcInterval> intervals, string col)
    {
        var clauses = new List<string>();
        var parameters = new List<object>();

        foreach (var interval in intervals)
        {
            var idx = parameters.Count;
            clauses.Add($"({col} >= ${idx + 1} AND {col} < ${idx + 2})");
            parameters.Add(interval.Start.UtcDateTime);
            parameters.Add(interval.ExclusiveEnd.UtcDateTime);
        }

        var sql = string.Join(" OR ", clauses);
        if (clauses.Count > 1) sql = $"({sql})";
        return new SqlEmission(sql, parameters);
    }

    private static SqlEmission EmitTempTable(IReadOnlyList<UtcInterval> intervals, string col)
    {
        var setupSql = new System.Text.StringBuilder();
        setupSql.AppendLine("CREATE OR REPLACE TEMP TABLE _q_intervals (start_ts TIMESTAMP, end_ts TIMESTAMP);");

        foreach (var interval in intervals)
        {
            setupSql.AppendLine(
                $"INSERT INTO _q_intervals VALUES ('{interval.Start.UtcDateTime:yyyy-MM-dd HH:mm:ss.ffffff}', '{interval.ExclusiveEnd.UtcDateTime:yyyy-MM-dd HH:mm:ss.ffffff}');");
        }

        var whereSql = $"EXISTS (SELECT 1 FROM _q_intervals qi WHERE {col} >= qi.start_ts AND {col} < qi.end_ts)";

        return new SqlEmission(whereSql, [], setupSql.ToString());
    }
}
