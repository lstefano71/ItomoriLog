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
            MERGE INTO segments AS target
            USING (
                SELECT
                    $1 AS segment_id,
                    $2 AS logical_source_id,
                    $3 AS physical_file_id,
                    $4 AS min_ts_utc,
                    $5 AS max_ts_utc,
                    $6 AS row_count,
                    $7 AS last_ingest_run_id,
                    $8 AS active,
                    $9 AS last_byte_offset,
                    $10 AS source_path,
                    $11 AS file_size_bytes,
                    $12 AS last_modified_utc,
                    $13 AS file_hash
            ) AS source
            ON target.segment_id = source.segment_id
            WHEN MATCHED THEN UPDATE SET
                logical_source_id = source.logical_source_id,
                physical_file_id = source.physical_file_id,
                min_ts_utc = source.min_ts_utc,
                max_ts_utc = source.max_ts_utc,
                row_count = source.row_count,
                last_ingest_run_id = source.last_ingest_run_id,
                active = source.active,
                last_byte_offset = source.last_byte_offset,
                source_path = source.source_path,
                file_size_bytes = source.file_size_bytes,
                last_modified_utc = source.last_modified_utc,
                file_hash = source.file_hash
            WHEN NOT MATCHED THEN INSERT (
                segment_id,
                logical_source_id,
                physical_file_id,
                min_ts_utc,
                max_ts_utc,
                row_count,
                last_ingest_run_id,
                active,
                last_byte_offset,
                source_path,
                file_size_bytes,
                last_modified_utc,
                file_hash
            ) VALUES (
                source.segment_id,
                source.logical_source_id,
                source.physical_file_id,
                source.min_ts_utc,
                source.max_ts_utc,
                source.row_count,
                source.last_ingest_run_id,
                source.active,
                source.last_byte_offset,
                source.source_path,
                source.file_size_bytes,
                source.last_modified_utc,
                source.file_hash
            )
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
