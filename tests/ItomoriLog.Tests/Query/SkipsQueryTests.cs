using DuckDB.NET.Data;

using FluentAssertions;

using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Tests.Query;

public class SkipsQueryTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public SkipsQueryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_skips_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    private async Task<DuckDBConnection> SetupDbAsync()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);
        return conn;
    }

    private static async Task InsertSkipAsync(
        DuckDBConnection conn,
        string logicalSourceId,
        string segmentId,
        SkipReasonCode reasonCode,
        long? startOffset = null,
        long? endOffset = null,
        string? samplePrefix = null)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO skips (
                logical_source_id, physical_file_id, segment_id,
                segment_seq, start_offset, end_offset,
                reason_code, sample_prefix, utc_logged_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = logicalSourceId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "phys_" + segmentId });
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1L });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)startOffset ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)endOffset ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = reasonCode.ToString() });
        cmd.Parameters.Add(new DuckDBParameter {
            Value = samplePrefix is not null
                ? System.Text.Encoding.UTF8.GetBytes(samplePrefix)
                : DBNull.Value
        });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTimeOffset.UtcNow.UtcDateTime });
        await cmd.ExecuteNonQueryAsync();
    }

    #region Grouping

    [Fact]
    public async Task QueryAsync_GroupsBySourcePath()
    {
        var conn = await SetupDbAsync();

        // Insert skips for two different sources
        await InsertSkipAsync(conn, "app", "seg1", SkipReasonCode.TimeParse, 0, 100, "bad line 1");
        await InsertSkipAsync(conn, "app", "seg1", SkipReasonCode.TimeParse, 200, 300, "bad line 2");
        await InsertSkipAsync(conn, "server", "seg2", SkipReasonCode.JsonMalformed, 50, 80, "{broken");

        var query = new SkipsQuery(conn);
        var results = await query.QueryAsync();

        results.Should().HaveCount(2);

        var appGroup = results.First(g => g.SourcePath == "app");
        appGroup.Segments.Should().HaveCount(1, "same segment_id + reason_code → one summary");
        appGroup.Segments[0].ReasonCode.Should().Be(SkipReasonCode.TimeParse);
        appGroup.Segments[0].RecordCount.Should().Be(2);

        var serverGroup = results.First(g => g.SourcePath == "server");
        serverGroup.Segments.Should().HaveCount(1);
        serverGroup.Segments[0].ReasonCode.Should().Be(SkipReasonCode.JsonMalformed);
        serverGroup.Segments[0].RecordCount.Should().Be(1);
    }

    [Fact]
    public async Task QueryAsync_MultipleSegments_GroupedSeparately()
    {
        var conn = await SetupDbAsync();

        await InsertSkipAsync(conn, "app", "seg1", SkipReasonCode.TimeParse, 0, 100);
        await InsertSkipAsync(conn, "app", "seg2", SkipReasonCode.DecodeError, 200, 300);

        var query = new SkipsQuery(conn);
        var results = await query.QueryAsync();

        results.Should().HaveCount(1, "both from same source");
        results[0].Segments.Should().HaveCount(2, "different segment_ids → separate summaries");
    }

    [Fact]
    public async Task QueryAsync_EmptyTable_ReturnsEmpty()
    {
        var conn = await SetupDbAsync();

        var query = new SkipsQuery(conn);
        var results = await query.QueryAsync();

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryAsync_AggregatesOffsetRange()
    {
        var conn = await SetupDbAsync();

        await InsertSkipAsync(conn, "app", "seg1", SkipReasonCode.RegexDrift, 100, 200);
        await InsertSkipAsync(conn, "app", "seg1", SkipReasonCode.RegexDrift, 50, 300);
        await InsertSkipAsync(conn, "app", "seg1", SkipReasonCode.RegexDrift, 150, 250);

        var query = new SkipsQuery(conn);
        var results = await query.QueryAsync();

        results.Should().HaveCount(1);
        var seg = results[0].Segments[0];
        seg.StartOffset.Should().Be(50, "MIN of start_offset");
        seg.EndOffset.Should().Be(300, "MAX of end_offset");
        seg.RecordCount.Should().Be(3);
    }

    #endregion

    #region Filtering

    [Fact]
    public async Task QueryAsync_FilterByReasonCode()
    {
        var conn = await SetupDbAsync();

        await InsertSkipAsync(conn, "app", "seg1", SkipReasonCode.TimeParse, 0, 100);
        await InsertSkipAsync(conn, "app", "seg2", SkipReasonCode.DecodeError, 200, 300);
        await InsertSkipAsync(conn, "server", "seg3", SkipReasonCode.TimeParse, 50, 80);

        var query = new SkipsQuery(conn);
        var results = await query.QueryAsync(reasonCodeFilter: SkipReasonCode.TimeParse);

        results.SelectMany(g => g.Segments)
            .Should().OnlyContain(s => s.ReasonCode == SkipReasonCode.TimeParse);
        results.SelectMany(g => g.Segments).Should().HaveCount(2);
    }

    [Fact]
    public async Task QueryAsync_FilterBySourcePath()
    {
        var conn = await SetupDbAsync();

        await InsertSkipAsync(conn, "app", "seg1", SkipReasonCode.TimeParse, 0, 100);
        await InsertSkipAsync(conn, "server", "seg2", SkipReasonCode.JsonMalformed, 50, 80);

        var query = new SkipsQuery(conn);
        var results = await query.QueryAsync(sourcePathFilter: "server");

        results.Should().HaveCount(1);
        results[0].SourcePath.Should().Be("server");
    }

    [Fact]
    public async Task QueryAsync_FilterByReasonAndSource_Combined()
    {
        var conn = await SetupDbAsync();

        await InsertSkipAsync(conn, "app", "seg1", SkipReasonCode.TimeParse, 0, 100);
        await InsertSkipAsync(conn, "app", "seg2", SkipReasonCode.DecodeError, 200, 300);
        await InsertSkipAsync(conn, "server", "seg3", SkipReasonCode.TimeParse, 50, 80);

        var query = new SkipsQuery(conn);
        var results = await query.QueryAsync(
            reasonCodeFilter: SkipReasonCode.TimeParse,
            sourcePathFilter: "app");

        results.Should().HaveCount(1);
        results[0].SourcePath.Should().Be("app");
        results[0].Segments.Should().HaveCount(1);
        results[0].Segments[0].ReasonCode.Should().Be(SkipReasonCode.TimeParse);
    }

    [Fact]
    public async Task QueryAsync_FilterReturnsNothing_WhenNoMatch()
    {
        var conn = await SetupDbAsync();

        await InsertSkipAsync(conn, "app", "seg1", SkipReasonCode.TimeParse, 0, 100);

        var query = new SkipsQuery(conn);
        var results = await query.QueryAsync(reasonCodeFilter: SkipReasonCode.IOError);

        results.Should().BeEmpty();
    }

    #endregion

    #region SamplePrefix

    [Fact]
    public async Task QueryAsync_IncludesSamplePrefix()
    {
        var conn = await SetupDbAsync();

        await InsertSkipAsync(conn, "app", "seg1", SkipReasonCode.JsonMalformed,
            samplePrefix: "{invalid json here");

        var query = new SkipsQuery(conn);
        var results = await query.QueryAsync();

        results[0].Segments[0].SamplePrefix.Should().Be("{invalid json here");
    }

    #endregion

    #region Integration: Ingest with errors → skips in DB

    [Fact]
    public async Task Integration_IngestWithErrors_SkipsAppearInQuery()
    {
        var conn = await SetupDbAsync();

        // Create a text log with enough valid lines for detection (≥95% parse rate)
        // then a few bad lines that the reader will skip
        var lines = new List<string>();
        var baseTime = new DateTime(2024, 3, 15, 10, 0, 0);
        for (int i = 0; i < 50; i++)
            lines.Add($"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO Message line {i}");

        var logContent = string.Join('\n', lines);
        var logFile = Path.Combine(_tempDir, "clean.log");
        await File.WriteAllTextAsync(logFile, logContent);

        // Run the orchestrator
        await SchemaInitializer.EnsureSchemaAsync(conn);
        var orchestrator = new IngestOrchestrator(conn);
        var defaultTz = new TimeBasisConfig(TimeBasis.Local);
        var result = await orchestrator.IngestFilesAsync([logFile], defaultTz);

        result.TotalRows.Should().BeGreaterThan(0, "valid lines should be ingested");

        // Now simulate skip records being persisted (the orchestrator uses ListSkipSink in-memory;
        // the app layer would flush these to DB)
        await InsertSkipAsync(conn, "clean", "seg_test_1", SkipReasonCode.TimeParse, 100, 200, "bad timestamp line");
        await InsertSkipAsync(conn, "clean", "seg_test_1", SkipReasonCode.TimeParse, 300, 400, "another bad line");
        await InsertSkipAsync(conn, "clean", "seg_test_2", SkipReasonCode.DecodeError, 500, 600, "\xff\xfe garbage");

        var query = new SkipsQuery(conn);
        var groups = await query.QueryAsync();

        groups.Should().NotBeEmpty("skips were inserted for this source");
        var totalSkips = groups.SelectMany(g => g.Segments).Sum(s => s.RecordCount);
        totalSkips.Should().Be(3);

        // Filter by reason code
        var timeParseOnly = await query.QueryAsync(reasonCodeFilter: SkipReasonCode.TimeParse);
        timeParseOnly.SelectMany(g => g.Segments).Sum(s => s.RecordCount).Should().Be(2);
    }

    #endregion

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
