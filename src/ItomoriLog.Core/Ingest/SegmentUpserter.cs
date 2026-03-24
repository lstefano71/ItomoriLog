using DuckDB.NET.Data;

namespace ItomoriLog.Core.Ingest;

public sealed record SegmentUpsertRow(
    string SegmentId,
    string LogicalSourceId,
    string PhysicalFileId,
    DateTimeOffset? MinTsUtc,
    DateTimeOffset? MaxTsUtc,
    long RowCount,
    string LastIngestRunId,
    bool Active,
    long? LastByteOffset,
    string? SourcePath,
    long? FileSizeBytes,
    DateTimeOffset? LastModifiedUtc,
    string? FileHash);

public sealed class SegmentUpserter
{
    private readonly DuckDBConnection _connection;

    public SegmentUpserter(DuckDBConnection connection)
    {
        _connection = connection;
    }

    public async Task UpsertBatchAsync(IReadOnlyList<SegmentUpsertRow> segments, CancellationToken ct = default)
    {
        if (segments.Count == 0)
            return;

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO segments (
                segment_id, logical_source_id, physical_file_id,
                min_ts_utc, max_ts_utc, row_count, last_ingest_run_id, active,
                last_byte_offset, source_path, file_size_bytes, last_modified_utc, file_hash
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            )
            ON CONFLICT (segment_id) DO UPDATE SET
                logical_source_id  = EXCLUDED.logical_source_id,
                physical_file_id   = EXCLUDED.physical_file_id,
                min_ts_utc         = EXCLUDED.min_ts_utc,
                max_ts_utc         = EXCLUDED.max_ts_utc,
                row_count          = EXCLUDED.row_count,
                last_ingest_run_id = EXCLUDED.last_ingest_run_id,
                active             = EXCLUDED.active,
                last_byte_offset   = EXCLUDED.last_byte_offset,
                source_path        = EXCLUDED.source_path,
                file_size_bytes    = EXCLUDED.file_size_bytes,
                last_modified_utc  = EXCLUDED.last_modified_utc,
                file_hash          = EXCLUDED.file_hash
            """;

        foreach (var row in segments)
        {
            ct.ThrowIfCancellationRequested();

            cmd.Parameters.Clear();
            cmd.Parameters.Add(new DuckDBParameter { Value = row.SegmentId });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.LogicalSourceId });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.PhysicalFileId });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.MinTsUtc.HasValue ? (object)row.MinTsUtc.Value.UtcDateTime : DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.MaxTsUtc.HasValue ? (object)row.MaxTsUtc.Value.UtcDateTime : DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.RowCount });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.LastIngestRunId });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.Active });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.LastByteOffset.HasValue ? (object)row.LastByteOffset.Value : DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)row.SourcePath ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.FileSizeBytes.HasValue ? (object)row.FileSizeBytes.Value : DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = row.LastModifiedUtc.HasValue ? (object)row.LastModifiedUtc.Value.UtcDateTime : DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)row.FileHash ?? DBNull.Value });
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}
