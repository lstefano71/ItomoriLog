using DuckDB.NET.Data;
using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Ingest;

public sealed class LogBatchInserter
{
    private readonly DuckDBConnection _connection;
    private const int BatchSize = 50_000;

    public LogBatchInserter(DuckDBConnection connection)
    {
        _connection = connection;
    }

    public async Task InsertBatchAsync(IReadOnlyList<LogRow> rows, CancellationToken ct = default)
    {
        if (rows.Count == 0) return;

        for (int i = 0; i < rows.Count; i += BatchSize)
        {
            var batch = rows.Skip(i).Take(BatchSize).ToList();
            await InsertChunkAsync(batch, ct);
        }
    }

    private async Task InsertChunkAsync(IReadOnlyList<LogRow> rows, CancellationToken ct)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO logs (
                timestamp_utc, timestamp_basis, timestamp_effective_offset_minutes,
                timestamp_original, logical_source_id, source_path,
                physical_file_id, segment_id, ingest_run_id,
                record_index, level, message, fields
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            )
            """;

        foreach (var row in rows)
        {
            ct.ThrowIfCancellationRequested();
            cmd.Parameters.Clear();
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
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}
