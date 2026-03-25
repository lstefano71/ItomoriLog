using DuckDB.NET.Data;

using ItomoriLog.Core.Model;

using System.Text;

namespace ItomoriLog.Core.Ingest;

public sealed class SkipBatchInserter
{
    private readonly DuckDBConnection _connection;
    private const int RowsPerStatement = 500;

    public SkipBatchInserter(DuckDBConnection connection)
    {
        _connection = connection;
    }

    public async Task InsertBatchAsync(IReadOnlyList<SkipRow> rows, string? sessionId = null, CancellationToken ct = default)
    {
        if (rows.Count == 0)
            return;

        for (int i = 0; i < rows.Count; i += RowsPerStatement) {
            var count = Math.Min(RowsPerStatement, rows.Count - i);
            await InsertChunkAsync(rows, i, count, sessionId, ct);
        }
    }

    private async Task InsertChunkAsync(IReadOnlyList<SkipRow> rows, int offset, int count, string? sessionId, CancellationToken ct)
    {
        var sql = new StringBuilder("""
            INSERT INTO skips (
                session_id, logical_source_id, physical_file_id, segment_id, segment_seq,
                start_line, end_line, start_offset, end_offset,
                reason_code, reason_detail, sample_prefix, detector_profile_id, utc_logged_at
            ) VALUES
            """);

        using var cmd = _connection.CreateCommand();

        for (int i = 0; i < count; i++) {
            ct.ThrowIfCancellationRequested();

            if (i > 0)
                sql.Append(',');

            var p = (i * 14) + 1;
            sql.Append($"(${p}, ${p + 1}, ${p + 2}, ${p + 3}, ${p + 4}, ${p + 5}, ${p + 6}, ${p + 7}, ${p + 8}, ${p + 9}, ${p + 10}, ${p + 11}, ${p + 12}, ${p + 13})");

            var row = rows[offset + i];
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)sessionId ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.LogicalSourceId });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.PhysicalFileId });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.SegmentId });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.SegmentSeq });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.StartLine.HasValue ? (object)row.StartLine.Value : DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.EndLine.HasValue ? (object)row.EndLine.Value : DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.StartOffset.HasValue ? (object)row.StartOffset.Value : DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.EndOffset.HasValue ? (object)row.EndOffset.Value : DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.ReasonCode.ToString() });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)row.ReasonDetail ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)row.SamplePrefix ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)row.DetectorProfileId ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.UtcLoggedAt.UtcDateTime });
        }

        cmd.CommandText = sql.ToString();
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
