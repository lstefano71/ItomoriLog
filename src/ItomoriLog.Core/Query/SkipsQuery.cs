using DuckDB.NET.Data;

using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Query;

public sealed record SkipSegmentSummary(
    SkipReasonCode ReasonCode,
    long? StartOffset,
    long? EndOffset,
    long RecordCount,
    string? SamplePrefix);

public sealed record SkipGroup(
    string SourcePath,
    IReadOnlyList<SkipSegmentSummary> Segments);

public sealed class SkipsQuery
{
    private readonly DuckDBConnection _connection;

    public SkipsQuery(DuckDBConnection connection) => _connection = connection;

    public async Task<IReadOnlyList<SkipGroup>> QueryAsync(
        SkipReasonCode? reasonCodeFilter = null,
        string? sourcePathFilter = null,
        CancellationToken ct = default)
    {
        using var cmd = _connection.CreateCommand();

        var whereClauses = new List<string>();
        var paramIndex = 1;

        if (reasonCodeFilter is not null) {
            whereClauses.Add($"reason_code = ${paramIndex}");
            cmd.Parameters.Add(new DuckDBParameter { Value = reasonCodeFilter.Value.ToString() });
            paramIndex++;
        }

        if (sourcePathFilter is not null) {
            whereClauses.Add($"logical_source_id = ${paramIndex}");
            cmd.Parameters.Add(new DuckDBParameter { Value = sourcePathFilter });
            paramIndex++;
        }

        var whereClause = whereClauses.Count > 0
            ? "WHERE " + string.Join(" AND ", whereClauses)
            : "";

        cmd.CommandText = $"""
            SELECT
                logical_source_id,
                reason_code,
                MIN(start_offset) AS start_offset,
                MAX(end_offset) AS end_offset,
                COUNT(*) AS record_count,
                FIRST(CAST(sample_prefix AS VARCHAR)) AS sample_prefix
            FROM skips
            {whereClause}
            GROUP BY logical_source_id, reason_code, segment_id
            ORDER BY logical_source_id, reason_code
            """;

        using var reader = await cmd.ExecuteReaderAsync(ct);

        var groups = new Dictionary<string, List<SkipSegmentSummary>>();

        while (await reader.ReadAsync(ct)) {
            var sourcePath = reader.GetString(0);
            var reasonCode = Enum.Parse<SkipReasonCode>(reader.GetString(1));
            var startOffset = reader.IsDBNull(2) ? (long?)null : reader.GetInt64(2);
            var endOffset = reader.IsDBNull(3) ? (long?)null : reader.GetInt64(3);
            var recordCount = reader.GetInt64(4);
            var samplePrefix = reader.IsDBNull(5) ? null : reader.GetString(5);

            if (!groups.TryGetValue(sourcePath, out var list)) {
                list = [];
                groups[sourcePath] = list;
            }

            list.Add(new SkipSegmentSummary(reasonCode, startOffset, endOffset, recordCount, samplePrefix));
        }

        return groups
            .Select(g => new SkipGroup(g.Key, g.Value))
            .OrderBy(g => g.SourcePath)
            .ToList();
    }
}
