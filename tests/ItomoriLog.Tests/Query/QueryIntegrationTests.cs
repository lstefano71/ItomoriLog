using FluentAssertions;

using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Tests.Query;

/// <summary>
/// End-to-end integration: ingest → query plan → paginate through results.
/// </summary>
public class QueryIntegrationTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public QueryIntegrationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_integration_test_{Guid.NewGuid():N}");
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
                Message: $"Event #{i}: something happened at step {i * 10}",
                FieldsJson: i % 5 == 0 ? """{"key":"value"}""" : null));
        }

        await inserter.InsertBatchAsync(rows);
    }

    [Fact]
    public async Task EndToEnd_IngestAndPageThrough_AllRecordsCovered()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 30);

        var allRows = new List<LogRow>();
        PageCursor? cursor = null;

        // Page through all records
        for (int pageNum = 0; pageNum < 10; pageNum++) {
            var result = await pager.FetchPageAsync(FilterState.Empty, cursor);
            allRows.AddRange(result.Rows);

            if (result.Rows.Count < pager.PageSize)
                break;

            cursor = result.Cursors.Last;
        }

        allRows.Should().HaveCount(100);
        // Verify ordering
        for (int i = 1; i < allRows.Count; i++) {
            allRows[i].TimestampUtc.Should().BeOnOrAfter(allRows[i - 1].TimestampUtc);
        }
    }

    [Fact]
    public async Task EndToEnd_FilteredPagination_OnlyMatchingRows()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 50);
        var filter = new FilterState { Levels = ["ERROR"] };

        var result = await pager.FetchPageAsync(filter);

        result.Rows.Should().AllSatisfy(r => r.Level.Should().Be("ERROR"));
        result.Rows.Count.Should().Be(25); // every 4th row
    }

    [Fact]
    public async Task EndToEnd_TimeWindowFilter_ClipsResults()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 200);
        var filter = new FilterState {
            StartUtc = baseTs.AddSeconds(10),
            EndUtc = baseTs.AddSeconds(20)
        };

        var result = await pager.FetchPageAsync(filter);

        result.Rows.Should().HaveCount(10); // seconds 10..19
        result.Rows.Should().AllSatisfy(r => {
            r.TimestampUtc.Should().BeOnOrAfter(baseTs.AddSeconds(10));
            r.TimestampUtc.Should().BeBefore(baseTs.AddSeconds(20));
        });
    }

    [Fact]
    public async Task EndToEnd_TickExpression_FiltersToInterval()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 200);

        // TICK expression for "today" from the perspective of baseTs
        var tickCtx = new TickContext(baseTs);
        var filter = new FilterState { TickExpression = "$today" };

        var result = await pager.FetchPageAsync(filter, tickContext: tickCtx);

        // All rows are on 2025-06-15 starting at 12:00:00, and $today = [2025-06-15T00:00Z, 2025-06-16T00:00Z)
        result.Rows.Should().HaveCount(100);
    }

    [Fact]
    public async Task EndToEnd_TickWithNarrowWindow_FiltersCorrectly()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 200);

        // $now-30s..$now from the perspective of baseTs+50s (= 12:00:50Z)
        // This means [12:00:20Z, 12:00:50Z)
        var tickCtx = new TickContext(baseTs.AddSeconds(50));
        var filter = new FilterState { TickExpression = "$now-30s..$now" };

        var result = await pager.FetchPageAsync(filter, tickContext: tickCtx);

        result.Rows.Should().HaveCount(30); // seconds 20..49
        result.Rows[0].RecordIndex.Should().Be(20);
        result.Rows[^1].RecordIndex.Should().Be(49);
    }

    [Fact]
    public async Task EndToEnd_TextSearch_MatchesSubstring()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(100, baseTs);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 200);
        var filter = new FilterState { TextSearch = "step 50" };

        var result = await pager.FetchPageAsync(filter);

        // "step 50" matches "step 500" (record 50) and "step 50" exactly doesn't exist,
        // but "step 500" = record 50, "step 50" is substring of many...
        // Actually: "step 50" matches messages containing that substring:
        // Event #5: something happened at step 50
        result.Rows.Should().NotBeEmpty();
        result.Rows.Should().AllSatisfy(r => r.Message.Should().ContainEquivalentOf("step 50"));
    }

    [Fact]
    public async Task EndToEnd_CombinedFiltersAndPagination()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(200, baseTs);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 10);
        var filter = new FilterState {
            Levels = ["ERROR"],
            SourceIds = ["source-0"]
        };

        var allRows = new List<LogRow>();
        PageCursor? cursor = null;

        for (int pageNum = 0; pageNum < 50; pageNum++) {
            var result = await pager.FetchPageAsync(filter, cursor);
            allRows.AddRange(result.Rows);

            if (result.Rows.Count < pager.PageSize)
                break;

            cursor = result.Cursors.Last;
        }

        // ERROR = every 4th row (i%4==0), source-0 = every 3rd row (i%3==0)
        // Both: i%12==0 → rows 0, 12, 24, ..., 192 = 17 rows
        allRows.Should().HaveCount(17);
        allRows.Should().AllSatisfy(r => {
            r.Level.Should().Be("ERROR");
            r.LogicalSourceId.Should().Be("source-0");
        });
    }

    [Fact]
    public async Task EndToEnd_MessageQueryNode_RespectsBooleanSemantics()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(30, baseTs);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 200);
        var filter = new FilterState {
            TextSearchQuery = new MessageAndNode(
                new MessageTermNode("Event"),
                new MessageOrNode(new MessageTermNode("#1"), new MessageTermNode("#2")))
        };

        var result = await pager.FetchPageAsync(filter);

        result.Rows.Should().NotBeEmpty();
        result.Rows.Should().AllSatisfy(r => {
            r.Message.Should().Contain("Event");
            (r.Message.Contains("#1", StringComparison.OrdinalIgnoreCase)
                || r.Message.Contains("#2", StringComparison.OrdinalIgnoreCase)).Should().BeTrue();
        });
    }

    [Fact]
    public async Task EndToEnd_ExcludedLevel_FilterRemovesRows()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(40, baseTs);

        var planner = new QueryPlanner();
        var pager = new RowPager(_factory, planner, pageSize: 200);
        var filter = new FilterState { ExcludedLevels = ["DEBUG"] };

        var result = await pager.FetchPageAsync(filter);

        result.Rows.Should().NotBeEmpty();
        result.Rows.Should().OnlyContain(r => r.Level != "DEBUG");
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
