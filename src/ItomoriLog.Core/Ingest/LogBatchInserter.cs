using DuckDB.NET.Data;
using ItomoriLog.Core.Model;
using System.Text;

namespace ItomoriLog.Core.Ingest;

public sealed class LogBatchInserter
{
    private readonly DuckDBConnection _connection;
    private const int RowsPerStatement = 5_000;

    public LogBatchInserter(DuckDBConnection connection)
    {
        _connection = connection;
    }

    public async Task InsertBatchAsync(IReadOnlyList<LogRow> rows, CancellationToken ct = default)
    {
        if (rows.Count == 0) return;

        for (int i = 0; i < rows.Count; i += RowsPerStatement)
        {
            var count = Math.Min(RowsPerStatement, rows.Count - i);
            await InsertChunkAsync(rows, i, count, ct);
        }
    }

    private async Task InsertChunkAsync(IReadOnlyList<LogRow> rows, int offset, int count, CancellationToken ct)
    {
        var sql = new StringBuilder("""
            INSERT INTO logs (
                timestamp_utc, timestamp_basis, timestamp_effective_offset_minutes,
                timestamp_original, logical_source_id, source_path,
                physical_file_id, segment_id, ingest_run_id,
                record_index, level, message, fields
            ) VALUES
            """);

        using var cmd = _connection.CreateCommand();

        for (int i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();

            if (i > 0)
                sql.Append(',');

            var parameterBase = (i * 13) + 1;
            sql.Append('(');
            sql.Append($"${parameterBase}, ${parameterBase + 1}, ${parameterBase + 2}, ${parameterBase + 3}, ");
            sql.Append($"${parameterBase + 4}, ${parameterBase + 5}, ${parameterBase + 6}, ${parameterBase + 7}, ");
            sql.Append($"${parameterBase + 8}, ${parameterBase + 9}, ${parameterBase + 10}, ${parameterBase + 11}, ${parameterBase + 12}");
            sql.Append(')');

            var row = rows[offset + i];
            cmd.Parameters.Add(new DuckDBParameter { Value = row.TimestampUtc.UtcDateTime });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.TimestampBasis.ToString() });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.TimestampEffectiveOffsetMinutes });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)row.TimestampOriginal ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.LogicalSourceId });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.SourcePath });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.PhysicalFileId });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.SegmentId });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.IngestRunId });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.RecordIndex });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)row.Level ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.Message });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)row.FieldsJson ?? DBNull.Value });
        }

        cmd.CommandText = sql.ToString();
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
