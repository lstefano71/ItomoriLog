using FluentAssertions;
using ItomoriLog.Core.Query;

namespace ItomoriLog.Tests.Query;

public class QueryPlannerTests
{
    private readonly QueryPlanner _planner = new();

    // -----------------------------------------------------------------------
    // Basic query (no filters, no cursor)
    // -----------------------------------------------------------------------

    [Fact]
    public void Plan_NoFilters_NoCursor_ProducesBasicSelect()
    {
        var result = _planner.Plan(FilterState.Empty);

        result.Sql.Should().Contain("SELECT");
        result.Sql.Should().Contain("FROM logs");
        result.Sql.Should().Contain("ORDER BY timestamp_utc ASC");
        result.Sql.Should().Contain("LIMIT $1");
        result.Parameters.Should().HaveCount(1);
        result.Parameters[0].Should().Be(2000);
        result.Direction.Should().Be(PageDirection.Forward);
        result.Sql.Should().NotContain("WHERE");
    }

    // -----------------------------------------------------------------------
    // Time window filter
    // -----------------------------------------------------------------------

    [Fact]
    public void Plan_TimeWindow_ProducesTimestampClauses()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var end = new DateTimeOffset(2025, 1, 2, 0, 0, 0, TimeSpan.Zero);
        var filter = new FilterState { StartUtc = start, EndUtc = end };

        var result = _planner.Plan(filter);

        result.Sql.Should().Contain("timestamp_utc >= $1");
        result.Sql.Should().Contain("timestamp_utc < $2");
        result.Parameters[0].Should().Be(start.UtcDateTime);
        result.Parameters[1].Should().Be(end.UtcDateTime);
        // LIMIT param is $3
        result.Sql.Should().Contain("LIMIT $3");
    }

    // -----------------------------------------------------------------------
    // Source filter
    // -----------------------------------------------------------------------

    [Fact]
    public void Plan_SourceFilter_ProducesInClause()
    {
        var filter = new FilterState { SourceIds = ["app.log", "svc.log"] };

        var result = _planner.Plan(filter);

        result.Sql.Should().Contain("logical_source_id IN ($1, $2)");
        result.Parameters[0].Should().Be("app.log");
        result.Parameters[1].Should().Be("svc.log");
    }

    // -----------------------------------------------------------------------
    // Level filter
    // -----------------------------------------------------------------------

    [Fact]
    public void Plan_LevelFilter_ProducesInClause()
    {
        var filter = new FilterState { Levels = ["ERROR", "WARN"] };

        var result = _planner.Plan(filter);

        result.Sql.Should().Contain("level IN ($1, $2)");
        result.Parameters[0].Should().Be("ERROR");
        result.Parameters[1].Should().Be("WARN");
    }

    // -----------------------------------------------------------------------
    // Text search
    // -----------------------------------------------------------------------

    [Fact]
    public void Plan_TextSearch_ProducesIlike()
    {
        var filter = new FilterState { TextSearch = "timeout" };

        var result = _planner.Plan(filter);

        result.Sql.Should().Contain("message ILIKE $1");
        result.Parameters[0].Should().Be("%timeout%");
    }

    [Fact]
    public void Plan_TextSearchQuery_ProducesBooleanExpression()
    {
        var filter = new FilterState
        {
            TextSearchQuery = new MessageAndNode(
                new MessageTermNode("error"),
                new MessageOrNode(new MessageTermNode("timeout"), new MessageTermNode("retry")))
        };

        var result = _planner.Plan(filter);

        result.Sql.Should().Contain("(message ILIKE $1 ESCAPE '\\'");
        result.Sql.Should().Contain("AND");
        result.Sql.Should().Contain("OR");
        result.Parameters.Should().Contain("%error%");
        result.Parameters.Should().Contain("%timeout%");
        result.Parameters.Should().Contain("%retry%");
    }

    // -----------------------------------------------------------------------
    // Combined filters
    // -----------------------------------------------------------------------

    [Fact]
    public void Plan_CombinedFilters_AllClausesPresent()
    {
        var start = new DateTimeOffset(2025, 3, 1, 0, 0, 0, TimeSpan.Zero);
        var filter = new FilterState
        {
            StartUtc = start,
            Levels = ["ERROR"],
            TextSearch = "fail",
            SourceIds = ["app.log"]
        };

        var result = _planner.Plan(filter);

        result.Sql.Should().Contain("timestamp_utc >= $1");
        result.Sql.Should().Contain("logical_source_id IN ($2)");
        result.Sql.Should().Contain("level IN ($3)");
        result.Sql.Should().Contain("message ILIKE $4");
        result.Sql.Should().Contain("AND");
    }

    // -----------------------------------------------------------------------
    // Keyset cursor — forward
    // -----------------------------------------------------------------------

    [Fact]
    public void Plan_ForwardCursor_ProducesGreaterThan()
    {
        var cursor = new PageCursor(
            new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero),
            "seg-1",
            42);

        var result = _planner.Plan(FilterState.Empty, cursor, PageDirection.Forward);

        result.Sql.Should().Contain("(timestamp_utc, segment_id, record_index) > ($1, $2, $3)");
        result.Sql.Should().Contain("ORDER BY timestamp_utc ASC, segment_id ASC, record_index ASC");
        result.Parameters[0].Should().Be(cursor.TimestampUtc.UtcDateTime);
        result.Parameters[1].Should().Be("seg-1");
        result.Parameters[2].Should().Be(42L);
    }

    // -----------------------------------------------------------------------
    // Keyset cursor — backward
    // -----------------------------------------------------------------------

    [Fact]
    public void Plan_BackwardCursor_ProducesLessThan()
    {
        var cursor = new PageCursor(
            new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero),
            "seg-1",
            42);

        var result = _planner.Plan(FilterState.Empty, cursor, PageDirection.Backward);

        result.Sql.Should().Contain("(timestamp_utc, segment_id, record_index) < ($1, $2, $3)");
        result.Sql.Should().Contain("ORDER BY timestamp_utc DESC, segment_id DESC, record_index DESC");
        result.Direction.Should().Be(PageDirection.Backward);
    }

    // -----------------------------------------------------------------------
    // Page size
    // -----------------------------------------------------------------------

    [Fact]
    public void Plan_CustomPageSize_AppliedToLimit()
    {
        var result = _planner.Plan(FilterState.Empty, pageSize: 500);

        result.Parameters.Should().Contain(500);
    }

    // -----------------------------------------------------------------------
    // TICK expression integration
    // -----------------------------------------------------------------------

    [Fact]
    public void Plan_TickExpression_EmitsIntervalSql()
    {
        var ctx = new TickContext(new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero));
        var filter = new FilterState { TickExpression = "$now-1h..$now" };

        var result = _planner.Plan(filter, tickContext: ctx);

        // Should contain timestamp_utc >= and timestamp_utc < from TICK emission
        result.Sql.Should().Contain("timestamp_utc >=");
        result.Sql.Should().Contain("timestamp_utc <");
        result.Parameters.Count.Should().BeGreaterThan(1); // interval params + LIMIT param
    }

    [Fact]
    public void Plan_TickWithOtherFilters_MergesCorrectly()
    {
        var ctx = new TickContext(new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero));
        var filter = new FilterState
        {
            TickExpression = "$today",
            Levels = ["ERROR"]
        };

        var result = _planner.Plan(filter, tickContext: ctx);

        result.Sql.Should().Contain("level IN");
        result.Sql.Should().Contain("timestamp_utc >=");
        result.Sql.Should().Contain("AND");
    }

    // -----------------------------------------------------------------------
    // No OFFSET ever
    // -----------------------------------------------------------------------

    [Fact]
    public void Plan_NeverUsesOffset()
    {
        var filter = new FilterState { Levels = ["INFO"], TextSearch = "test" };
        var cursor = new PageCursor(
            new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero),
            "seg-1", 10);

        var result = _planner.Plan(filter, cursor, PageDirection.Forward);

        // Check that the SQL never uses OFFSET as a keyword (not as part of column names)
        result.Sql.Should().NotMatchRegex(@"\bOFFSET\s+\d");
    }

    // -----------------------------------------------------------------------
    // Cursor combined with filters
    // -----------------------------------------------------------------------

    [Fact]
    public void Plan_CursorWithFilters_BothPresent()
    {
        var filter = new FilterState { Levels = ["ERROR"] };
        var cursor = new PageCursor(
            new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero),
            "seg-1", 5);

        var result = _planner.Plan(filter, cursor, PageDirection.Forward);

        result.Sql.Should().Contain("level IN ($1)");
        result.Sql.Should().Contain("(timestamp_utc, segment_id, record_index) > ($2, $3, $4)");
        result.Sql.Should().Contain("LIMIT $5");
    }

    [Fact]
    public void Plan_ExcludeFilters_ProduceNotInClauses()
    {
        var filter = new FilterState
        {
            ExcludedLevels = ["DEBUG"],
            ExcludedSourceIds = ["src-a"]
        };

        var result = _planner.Plan(filter);

        result.Sql.Should().Contain("logical_source_id NOT IN ($1)");
        result.Sql.Should().Contain("level IS NULL OR level NOT IN ($2)");
    }

    // -----------------------------------------------------------------------
    // RebaseParameterIndices
    // -----------------------------------------------------------------------

    [Fact]
    public void RebaseParameterIndices_ShiftsCorrectly()
    {
        var sql = "(timestamp_utc >= $1 AND timestamp_utc < $2)";
        var rebased = QueryPlanner.RebaseParameterIndices(sql, 3);

        rebased.Should().Be("(timestamp_utc >= $4 AND timestamp_utc < $5)");
    }

    [Fact]
    public void RebaseParameterIndices_ZeroOffset_NoChange()
    {
        var sql = "(timestamp_utc >= $1 AND timestamp_utc < $2)";
        var rebased = QueryPlanner.RebaseParameterIndices(sql, 0);

        rebased.Should().Be(sql);
    }
}
