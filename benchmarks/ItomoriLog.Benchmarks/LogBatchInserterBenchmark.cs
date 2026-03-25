using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Benchmarks;

[MemoryDiagnoser]
public class LogBatchInserterBenchmark
{
    private DuckDBConnection _connection = null!;
    private IReadOnlyList<LogRow> _rows = null!;
    private LogBatchInserter _inserter = null!;

    [GlobalSetup]
    public void Setup()
    {
        _connection = new DuckDBConnection("DataSource=:memory:");
        _connection.Open();
        SchemaInitializer.EnsureSchemaAsync(_connection).GetAwaiter().GetResult();
        _inserter = new LogBatchInserter(_connection);
        _rows = GenerateRows(50_000);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "DELETE FROM logs";
        cmd.ExecuteNonQuery();
    }

    [Benchmark]
    public Task InsertBatch_50k() => _inserter.InsertBatchAsync(_rows);

    [GlobalCleanup]
    public void Cleanup() => _connection.Dispose();

    private static List<LogRow> GenerateRows(int count)
    {
        var baseTime = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var rows = new List<LogRow>(count);
        for (int i = 0; i < count; i++) {
            rows.Add(new LogRow(
                TimestampUtc: baseTime.AddSeconds(i),
                TimestampBasis: TimeBasis.Utc,
                TimestampEffectiveOffsetMinutes: 0,
                TimestampOriginal: $"2026-01-01T{i / 3600:D2}:{(i / 60) % 60:D2}:{i % 60:D2}Z",
                LogicalSourceId: "bench-source",
                SourcePath: "/tmp/bench/test.log",
                PhysicalFileId: "abc123def456abcd",
                SegmentId: "seg001seg001seg0",
                IngestRunId: "run001run001run0",
                RecordIndex: i,
                Level: (i % 4) switch { 0 => "INFO", 1 => "WARN", 2 => "ERROR", _ => "DEBUG" },
                Message: $"Benchmark log message index {i} with representative content to simulate real log lines",
                FieldsJson: i % 3 == 0
                    ? $"{{\"trace_id\":\"{i:x8}\",\"user\":\"user{i % 100}\"}}"
                    : null));
        }
        return rows;
    }
}
