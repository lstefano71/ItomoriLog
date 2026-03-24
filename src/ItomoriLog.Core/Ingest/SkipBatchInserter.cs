using DuckDB.NET.Data;
using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Ingest;

public sealed class SkipBatchInserter
{
    private readonly DuckDBConnection _connection;

    public SkipBatchInserter(DuckDBConnection connection)
    {
        _connection = connection;
    }

    public async Task InsertBatchAsync(IReadOnlyList<SkipRow> rows, string? sessionId = null, CancellationToken ct = default)
    {
        if (rows.Count == 0)
            return;

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO skips (
                session_id, logical_source_id, physical_file_id, segment_id, segment_seq,
                start_line, end_line, start_offset, end_offset,
                reason_code, reason_detail, sample_prefix, detector_profile_id, utc_logged_at
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12, $13, $14
            )
            """;

        foreach (var row in rows)
        {
            ct.ThrowIfCancellationRequested();

            cmd.Parameters.Clear();
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
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}
