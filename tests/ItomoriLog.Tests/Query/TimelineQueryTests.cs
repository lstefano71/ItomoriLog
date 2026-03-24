using FluentAssertions;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Storage;
using ItomoriLog.Core.Ingest;

namespace ItomoriLog.Tests.Query;

public class TimelineQueryTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public TimelineQueryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_timeline_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, SessionPaths.DefaultDbFileName);
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    private async Task SeedRowsAsync(int count, DateTimeOffset baseTs, TimeSpan spacing = default)
    {
        if (spacing == default) spacing = TimeSpan.FromSeconds(1);
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var inserter = new LogBatchInserter(conn);
        var rows = new List<LogRow>();

        for (int i = 0; i < count; i++)
        {
            rows.Add(new LogRow(
                TimestampUtc: baseTs.Add(spacing * i),
                TimestampBasis: TimeBasis.Utc,
                TimestampEffectiveOffsetMinutes: 0,
                TimestampOriginal: baseTs.Add(spacing * i).ToString("O"),
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
    public async Task QueryBins_CorrectTotalCount()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var query = new TimelineQuery(_factory);
        var bins = await query.QueryBinsAsync(
            baseTs, baseTs.AddSeconds(100),
            TimeSpan.FromSeconds(10));

        var totalCount = bins.Sum(b => b.Count);
        totalCount.Should().Be(100);
    }

    [Fact]
    public async Task QueryBins_CorrectBinCount()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        // 60 rows, 1 per second → 10-second bins → 6 bins
        await SeedRowsAsync(60, baseTs);

        var query = new TimelineQuery(_factory);
        var bins = await query.QueryBinsAsync(
            baseTs, baseTs.AddSeconds(60),
            TimeSpan.FromSeconds(10));

        bins.Should().HaveCount(6);
        bins.Should().AllSatisfy(b => b.Count.Should().Be(10));
    }

    [Fact]
    public async Task QueryBins_WithLevelFilter_OnlyMatchingRows()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var query = new TimelineQuery(_factory);
        var bins = await query.QueryBinsAsync(
            baseTs, baseTs.AddSeconds(100),
            TimeSpan.FromSeconds(10),
            levels: ["ERROR"]);

        var totalCount = bins.Sum(b => b.Count);
        totalCount.Should().Be(25); // every 4th row
    }

    [Fact]
    public async Task QueryBins_WithSourceFilter_OnlyMatchingRows()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(99, baseTs); // 99 rows: 33 per source

        var query = new TimelineQuery(_factory);
        var bins = await query.QueryBinsAsync(
            baseTs, baseTs.AddSeconds(99),
            TimeSpan.FromSeconds(10),
            sourceIds: ["source-0"]);

        var totalCount = bins.Sum(b => b.Count);
        totalCount.Should().Be(33);
    }

    [Fact]
    public async Task QueryBins_ProgressiveRefinement_FinerBinsHaveMoreEntries()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(120, baseTs);

        var query = new TimelineQuery(_factory);

        // Coarse: 1 minute bins
        var coarseBins = await query.QueryBinsAsync(
            baseTs, baseTs.AddSeconds(120),
            TimeSpan.FromMinutes(1));

        // Fine: 10 second bins
        var fineBins = await query.QueryBinsAsync(
            baseTs, baseTs.AddSeconds(120),
            TimeSpan.FromSeconds(10));

        coarseBins.Length.Should().BeLessThan(fineBins.Length);
        coarseBins.Sum(b => b.Count).Should().Be(fineBins.Sum(b => b.Count));
    }

    [Fact]
    public async Task QueryBins_HasDominantLevel()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var query = new TimelineQuery(_factory);
        var bins = await query.QueryBinsAsync(
            baseTs, baseTs.AddSeconds(100),
            TimeSpan.FromSeconds(10));

        bins.Should().AllSatisfy(b => b.DominantLevel.Should().NotBeNull());
    }

    [Fact]
    public async Task QueryBins_EmptyResult_ReturnsEmptyArray()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var query = new TimelineQuery(_factory);
        var bins = await query.QueryBinsAsync(
            DateTimeOffset.UtcNow.AddHours(-1),
            DateTimeOffset.UtcNow,
            TimeSpan.FromMinutes(5));

        bins.Should().BeEmpty();
    }

    [Fact]
    public async Task GetTimeRange_ReturnsMinMax()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var query = new TimelineQuery(_factory);
        var range = await query.GetTimeRangeAsync();

        range.Should().NotBeNull();
        range!.Value.Min.Should().Be(baseTs);
        range.Value.Max.Should().Be(baseTs.AddSeconds(99));
    }

    [Fact]
    public async Task GetTimeRange_EmptyTable_ReturnsNull()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var query = new TimelineQuery(_factory);
        var range = await query.GetTimeRangeAsync();
        range.Should().BeNull();
    }

    [Fact]
    public void ChooseCoarseBinWidth_SelectsAppropriateWidth()
    {
        TimelineQuery.ChooseCoarseBinWidth(TimeSpan.FromMinutes(5))
            .Should().Be(TimeSpan.FromSeconds(10));

        TimelineQuery.ChooseCoarseBinWidth(TimeSpan.FromMinutes(30))
            .Should().Be(TimeSpan.FromMinutes(1));

        TimelineQuery.ChooseCoarseBinWidth(TimeSpan.FromHours(3))
            .Should().Be(TimeSpan.FromMinutes(5));

        TimelineQuery.ChooseCoarseBinWidth(TimeSpan.FromHours(12))
            .Should().Be(TimeSpan.FromMinutes(30));

        TimelineQuery.ChooseCoarseBinWidth(TimeSpan.FromDays(3))
            .Should().Be(TimeSpan.FromHours(1));

        TimelineQuery.ChooseCoarseBinWidth(TimeSpan.FromDays(14))
            .Should().Be(TimeSpan.FromHours(6));

        TimelineQuery.ChooseCoarseBinWidth(TimeSpan.FromDays(60))
            .Should().Be(TimeSpan.FromDays(1));
    }

    [Fact]
    public void FormatInterval_FormatsCorrectly()
    {
        TimelineQuery.FormatInterval(TimeSpan.FromDays(1)).Should().Be("1 days");
        TimelineQuery.FormatInterval(TimeSpan.FromHours(2)).Should().Be("2 hours");
        TimelineQuery.FormatInterval(TimeSpan.FromMinutes(5)).Should().Be("5 minutes");
        TimelineQuery.FormatInterval(TimeSpan.FromSeconds(30)).Should().Be("30 seconds");
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
