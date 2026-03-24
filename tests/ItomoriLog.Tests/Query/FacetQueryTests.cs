using FluentAssertions;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Storage;
using ItomoriLog.Core.Ingest;

namespace ItomoriLog.Tests.Query;

public class FacetQueryTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public FacetQueryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_facet_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, SessionPaths.DefaultDbFileName);
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    private async Task SeedRowsAsync(int count, DateTimeOffset baseTs)
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var inserter = new LogBatchInserter(conn);
        var rows = new List<LogRow>();

        for (int i = 0; i < count; i++)
        {
            rows.Add(new LogRow(
                TimestampUtc: baseTs.AddSeconds(i),
                TimestampBasis: TimeBasis.Utc,
                TimestampEffectiveOffsetMinutes: 0,
                TimestampOriginal: baseTs.AddSeconds(i).ToString("O"),
                LogicalSourceId: $"source-{i % 3}",
                SourcePath: "/logs/test.log",
                PhysicalFileId: "file-1",
                SegmentId: $"seg-{i % 2}",
                IngestRunId: "run-1",
                RecordIndex: i,
                Level: (i % 4) switch
                {
                    0 => "ERROR",
                    1 => "WARN",
                    2 => "INFO",
                    _ => "DEBUG"
                },
                Message: $"Event #{i}",
                FieldsJson: null));
        }

        await inserter.InsertBatchAsync(rows);
    }

    [Fact]
    public async Task QueryLevels_ReturnsAllLevels()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var query = new FacetQuery(_factory);
        var levels = await query.QueryLevelsAsync();

        levels.Should().HaveCount(4);
        levels.Select(l => l.Value).Should().BeEquivalentTo(["ERROR", "WARN", "INFO", "DEBUG"]);
    }

    [Fact]
    public async Task QueryLevels_CorrectCounts()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var query = new FacetQuery(_factory);
        var levels = await query.QueryLevelsAsync();

        levels.Sum(l => l.Count).Should().Be(100);
        levels.Should().AllSatisfy(l => l.Count.Should().Be(25));
    }

    [Fact]
    public async Task QueryLevels_WithTimeWindow_FiltersCorrectly()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var query = new FacetQuery(_factory);
        var levels = await query.QueryLevelsAsync(
            startUtc: baseTs.AddSeconds(0),
            endUtc: baseTs.AddSeconds(20));

        levels.Sum(l => l.Count).Should().Be(20);
    }

    [Fact]
    public async Task QueryLevels_WithSourceFilter_FiltersCorrectly()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(99, baseTs); // 33 per source

        var query = new FacetQuery(_factory);
        var levels = await query.QueryLevelsAsync(sourceIds: ["source-0"]);

        levels.Sum(l => l.Count).Should().Be(33);
    }

    [Fact]
    public async Task QuerySources_ReturnsAllSources()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(99, baseTs);

        var query = new FacetQuery(_factory);
        var sources = await query.QuerySourcesAsync();

        sources.Should().HaveCount(3);
        sources.Select(s => s.Value).Should().BeEquivalentTo(["source-0", "source-1", "source-2"]);
    }

    [Fact]
    public async Task QuerySources_CorrectCounts()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(99, baseTs);

        var query = new FacetQuery(_factory);
        var sources = await query.QuerySourcesAsync();

        sources.Should().AllSatisfy(s => s.Count.Should().Be(33));
    }

    [Fact]
    public async Task QuerySources_WithLevelFilter_FiltersCorrectly()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var query = new FacetQuery(_factory);
        var sources = await query.QuerySourcesAsync(levels: ["ERROR"]);

        sources.Sum(s => s.Count).Should().Be(25);
    }

    [Fact]
    public async Task QuerySources_WithTimeWindow_FiltersCorrectly()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var query = new FacetQuery(_factory);
        var sources = await query.QuerySourcesAsync(
            startUtc: baseTs.AddSeconds(0),
            endUtc: baseTs.AddSeconds(30));

        sources.Sum(s => s.Count).Should().Be(30);
    }

    [Fact]
    public async Task QueryLevels_EmptyTable_ReturnsEmpty()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var query = new FacetQuery(_factory);
        var levels = await query.QueryLevelsAsync();
        levels.Should().BeEmpty();
    }

    [Fact]
    public async Task QuerySources_EmptyTable_ReturnsEmpty()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var query = new FacetQuery(_factory);
        var sources = await query.QuerySourcesAsync();
        sources.Should().BeEmpty();
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
