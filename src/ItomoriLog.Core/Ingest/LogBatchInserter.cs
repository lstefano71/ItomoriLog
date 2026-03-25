using DuckDB.NET.Data;

using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Ingest;

public sealed class LogBatchInserter
{
    private readonly DuckDBConnection _connection;

    public LogBatchInserter(DuckDBConnection connection)
    {
        _connection = connection;
    }

    public Task InsertBatchAsync(IReadOnlyList<LogRow> rows, CancellationToken ct = default)
    {
        if (rows.Count == 0) return Task.CompletedTask;

        ct.ThrowIfCancellationRequested();

        // DuckDB Appender bypasses SQL parsing and parameter binding entirely.
        // It streams rows directly into DuckDB's columnar append path and flushes
        // on Dispose, participating in any active transaction on the connection.
        // Column values are appended in the same order as the DDL definition.
        using var appender = _connection.CreateAppender("logs");
        foreach (var row in rows) {
            ct.ThrowIfCancellationRequested();

            var r = appender.CreateRow();
            r.AppendValue(row.TimestampUtc.UtcDateTime);
            r.AppendValue(row.TimestampBasis.ToString());
            r.AppendValue(row.TimestampEffectiveOffsetMinutes);
            if (row.TimestampOriginal is null) r.AppendNullValue(); else r.AppendValue(row.TimestampOriginal);
            r.AppendValue(row.LogicalSourceId);
            r.AppendValue(row.SourcePath);
            r.AppendValue(row.PhysicalFileId);
            r.AppendValue(row.SegmentId);
            r.AppendValue(row.IngestRunId);
            r.AppendValue(row.RecordIndex);
            if (row.Level is null) r.AppendNullValue(); else r.AppendValue(row.Level);
            r.AppendValue(row.Message);
            if (row.FieldsJson is null) r.AppendNullValue(); else r.AppendValue(row.FieldsJson);
            r.EndRow();
        }

        return Task.CompletedTask;
    }
}
