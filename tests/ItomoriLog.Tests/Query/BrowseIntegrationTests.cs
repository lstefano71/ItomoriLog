using FluentAssertions;

using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Tests.Query;

/// <summary>
/// Integration tests: ingest → query timeline bins → verify counts match facets.
/// </summary>
public class BrowseIntegrationTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public BrowseIntegrationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_browse_test_{Guid.NewGuid():N}");
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

        for (int i = 0; i < count; i++) {
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
                Level: (i % 4) switch {
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
    public async Task IngestAndTimeline_BinCountsMatchTotal()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        const int totalRows = 200;
        await SeedRowsAsync(totalRows, baseTs);

        var timelineQuery = new TimelineQuery(_factory);
        var bins = await timelineQuery.QueryBinsAsync(
            baseTs, baseTs.AddSeconds(totalRows),
            TimeSpan.FromSeconds(10));

        bins.Sum(b => b.Count).Should().Be(totalRows);
    }

    [Fact]
    public async Task IngestAndFacets_LevelCountsMatchTotal()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        const int totalRows = 200;
        await SeedRowsAsync(totalRows, baseTs);

        var facetQuery = new FacetQuery(_factory);
        var levels = await facetQuery.QueryLevelsAsync();

        levels.Sum(l => l.Count).Should().Be(totalRows);
    }

    [Fact]
    public async Task IngestAndFacets_SourceCountsMatchTotal()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        const int totalRows = 201; // divisible by 3
        await SeedRowsAsync(totalRows, baseTs);

        var facetQuery = new FacetQuery(_factory);
        var sources = await facetQuery.QuerySourcesAsync();

        sources.Sum(s => s.Count).Should().Be(totalRows);
        sources.Should().HaveCount(3);
    }

    [Fact]
    public async Task TimelineAndFacets_FilteredCountsConsistent()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(200, baseTs);

        var start = baseTs.AddSeconds(50);
        var end = baseTs.AddSeconds(150);

        var timelineQuery = new TimelineQuery(_factory);
        var bins = await timelineQuery.QueryBinsAsync(start, end, TimeSpan.FromSeconds(10));

        var facetQuery = new FacetQuery(_factory);
        var levels = await facetQuery.QueryLevelsAsync(start, end);
        var sources = await facetQuery.QuerySourcesAsync(start, end);

        var timelineTotal = bins.Sum(b => b.Count);
        var levelTotal = levels.Sum(l => l.Count);
        var sourceTotal = sources.Sum(s => s.Count);

        // All three should agree on the same filtered row count
        timelineTotal.Should().Be(100);
        levelTotal.Should().Be(100);
        sourceTotal.Should().Be(100);
    }

    [Fact]
    public async Task TimelineAndFacets_WithLevelFilter_Consistent()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(200, baseTs);

        var timelineQuery = new TimelineQuery(_factory);
        var bins = await timelineQuery.QueryBinsAsync(
            baseTs, baseTs.AddSeconds(200),
            TimeSpan.FromSeconds(10),
            levels: ["ERROR"]);

        var facetQuery = new FacetQuery(_factory);
        var sources = await facetQuery.QuerySourcesAsync(levels: ["ERROR"]);

        bins.Sum(b => b.Count).Should().Be(50); // every 4th row out of 200
        sources.Sum(s => s.Count).Should().Be(50);
    }

    [Fact]
    public async Task ProgressiveRefinement_CoarseThenFine_SameTotal()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        const int totalRows = 300;
        await SeedRowsAsync(totalRows, baseTs);

        var timelineQuery = new TimelineQuery(_factory);

        // Get time range
        var range = await timelineQuery.GetTimeRangeAsync();
        range.Should().NotBeNull();

        // Coarse bins
        var coarseBins = await timelineQuery.QueryBinsAsync(
            range!.Value.Min, range.Value.Max.AddSeconds(1),
            TimeSpan.FromMinutes(1));

        // Fine bins (zoom into first minute)
        var fineStart = range.Value.Min;
        var fineEnd = range.Value.Min.AddMinutes(1);
        var fineBins = await timelineQuery.QueryBinsAsync(
            fineStart, fineEnd,
            TimeSpan.FromSeconds(5));

        // Fine bins for first minute should sum to same as first coarse bin
        var coarseFirstMin = coarseBins.Where(b => b.Start < fineEnd).Sum(b => b.Count);
        var fineTotal = fineBins.Sum(b => b.Count);
        fineTotal.Should().Be(coarseFirstMin);
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
