using FluentAssertions;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Storage;
using ItomoriLog.Core.Ingest;
using DuckDB.NET.Data;

namespace ItomoriLog.Tests.Query;

public class RowPagerTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public RowPagerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_pager_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, SessionPaths.DefaultDbFileName);
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    private async Task SeedRowsAsync(int count)
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var inserter = new LogBatchInserter(conn);
        var rows = new List<LogRow>();
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);

        for (int i = 0; i < count; i++)
        {
            rows.Add(new LogRow(
                TimestampUtc: baseTs.AddSeconds(i),
                TimestampBasis: TimeBasis.Utc,
                TimestampEffectiveOffsetMinutes: 0,
                TimestampOriginal: null,
                LogicalSourceId: i % 2 == 0 ? "app.log" : "svc.log",
                SourcePath: "/logs/test.log",
                PhysicalFileId: "file-1",
                SegmentId: "seg-1",
                IngestRunId: "run-1",
                RecordIndex: i,
                Level: i % 3 == 0 ? "ERROR" : (i % 3 == 1 ? "WARN" : "INFO"),
                Message: $"Test message {i}",
                FieldsJson: null));
        }

        await inserter.InsertBatchAsync(rows);
    }

    [Fact]
    public async Task FetchPage_EmptyTable_ReturnsEmptyResult()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 100);

        var result = await pager.FetchPageAsync(FilterState.Empty);

        result.Rows.Should().BeEmpty();
        result.Cursors.First.Should().BeNull();
        result.Cursors.Last.Should().BeNull();
    }

    [Fact]
    public async Task FetchPage_FirstPage_ReturnsPageSizeRows()
    {
        await SeedRowsAsync(50);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 20);

        var result = await pager.FetchPageAsync(FilterState.Empty);

        result.Rows.Should().HaveCount(20);
        result.Cursors.First.Should().NotBeNull();
        result.Cursors.Last.Should().NotBeNull();
    }

    [Fact]
    public async Task FetchPage_SecondPage_ViaKeysetCursor()
    {
        await SeedRowsAsync(50);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 20);

        var page1 = await pager.FetchPageAsync(FilterState.Empty);
        var page2 = await pager.FetchPageAsync(FilterState.Empty, page1.Cursors.Last);

        page2.Rows.Should().HaveCount(20);
        // No overlap between pages
        var lastPage1Idx = page1.Rows[^1].RecordIndex;
        var firstPage2Idx = page2.Rows[0].RecordIndex;
        firstPage2Idx.Should().BeGreaterThan(lastPage1Idx);
    }

    [Fact]
    public async Task FetchPage_LastPage_FewerThanPageSize()
    {
        await SeedRowsAsync(50);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 20);

        var page1 = await pager.FetchPageAsync(FilterState.Empty);
        var page2 = await pager.FetchPageAsync(FilterState.Empty, page1.Cursors.Last);
        var page3 = await pager.FetchPageAsync(FilterState.Empty, page2.Cursors.Last);

        page3.Rows.Should().HaveCount(10); // 50 - 20 - 20 = 10
    }

    [Fact]
    public async Task FetchPage_WithLevelFilter_FiltersCorrectly()
    {
        await SeedRowsAsync(30);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 100);
        var filter = new FilterState { Levels = ["ERROR"] };

        var result = await pager.FetchPageAsync(filter);

        result.Rows.Should().AllSatisfy(r => r.Level.Should().Be("ERROR"));
        result.Rows.Count.Should().Be(10); // every 3rd row (0, 3, 6, ..., 27) = 10 rows
    }

    [Fact]
    public async Task FetchPage_WithTextSearch_FiltersMessages()
    {
        await SeedRowsAsync(30);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 100);
        var filter = new FilterState { TextSearch = "message 5" };

        var result = await pager.FetchPageAsync(filter);

        result.Rows.Should().NotBeEmpty();
        result.Rows.Should().AllSatisfy(r => r.Message.Should().Contain("message 5"));
    }

    [Fact]
    public async Task FetchPage_BackwardDirection_ReturnsAscendingOrder()
    {
        await SeedRowsAsync(50);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 20);

        // Get a cursor from the middle
        var page1 = await pager.FetchPageAsync(FilterState.Empty);
        var page2 = await pager.FetchPageAsync(FilterState.Empty, page1.Cursors.Last);

        // Now go backward from the first cursor of page2
        var backResult = await pager.FetchPageAsync(
            FilterState.Empty,
            page2.Cursors.First,
            PageDirection.Backward);

        // Should still be in ascending order (reversed from DESC query)
        backResult.Rows.Should().NotBeEmpty();
        for (int i = 1; i < backResult.Rows.Count; i++)
        {
            backResult.Rows[i].RecordIndex.Should().BeGreaterThan(
                backResult.Rows[i - 1].RecordIndex);
        }
    }

    [Fact]
    public async Task FetchPage_SourceFilter_FiltersCorrectly()
    {
        await SeedRowsAsync(30);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 100);
        var filter = new FilterState { SourceIds = ["app.log"] };

        var result = await pager.FetchPageAsync(filter);

        result.Rows.Should().AllSatisfy(r => r.LogicalSourceId.Should().Be("app.log"));
        result.Rows.Count.Should().Be(15); // even indices: 0, 2, 4, ..., 28 = 15 rows
    }

    [Fact]
    public async Task FetchPage_CachesResult_ReturnsSameOnSecondCall()
    {
        await SeedRowsAsync(10);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 100);

        var result1 = await pager.FetchPageAsync(FilterState.Empty);
        var result2 = await pager.FetchPageAsync(FilterState.Empty);

        // Same object from cache
        result1.Should().BeSameAs(result2);
    }

    [Fact]
    public async Task FetchPage_ClearCache_ReturnsNewResult()
    {
        await SeedRowsAsync(10);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 100);

        var result1 = await pager.FetchPageAsync(FilterState.Empty);
        pager.ClearCache();
        var result2 = await pager.FetchPageAsync(FilterState.Empty);

        result1.Should().NotBeSameAs(result2);
        result1.Rows.Count.Should().Be(result2.Rows.Count);
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
