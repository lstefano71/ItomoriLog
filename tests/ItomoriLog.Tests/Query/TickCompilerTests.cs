using ItomoriLog.Core.Query;

namespace ItomoriLog.Tests.Query;

public class TickCompilerTests
{
    // Fixed reference time for deterministic tests: 2025-06-15T12:00:00Z
    private static readonly DateTimeOffset Now =
        new(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);

    private static readonly TickContext Ctx = new(Now);

    private readonly TickCompiler _compiler = new();

    // Helper: compile and return the single (first) interval
    private UtcInterval CompileSingle(string expr, TickContext? ctx = null)
    {
        var result = _compiler.Compile(expr, ctx ?? Ctx);
        Assert.Null(result.Warning);
        Assert.NotEmpty(result.Intervals);
        return result.Intervals[0];
    }

    // Helper: compile and return all intervals
    private IReadOnlyList<UtcInterval> CompileMulti(string expr, TickContext? ctx = null)
    {
        var result = _compiler.Compile(expr, ctx ?? Ctx);
        Assert.Null(result.Warning);
        return result.Intervals;
    }

    // -----------------------------------------------------------------------
    // Variable anchors
    // -----------------------------------------------------------------------

    [Fact]
    public void Compile_NowVariable_ResolvesToNow()
    {
        var r = CompileSingle("$now");
        Assert.Equal(Now, r.Start);
        Assert.Equal(Now, r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_TodayVariable_ResolvesToMidnight()
    {
        var r = CompileSingle("$today");
        var expectedStart = new DateTimeOffset(2025, 6, 15, 0, 0, 0, TimeSpan.Zero);
        var expectedEnd = new DateTimeOffset(2025, 6, 16, 0, 0, 0, TimeSpan.Zero);
        Assert.Equal(expectedStart, r.Start);
        Assert.Equal(expectedEnd, r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_YesterdayVariable_ResolvesToPreviousMidnight()
    {
        var r = CompileSingle("$yesterday");
        var expected = new DateTimeOffset(2025, 6, 14, 0, 0, 0, TimeSpan.Zero);
        Assert.Equal(expected, r.Start);
    }

    [Fact]
    public void Compile_TomorrowVariable_ResolvesToNextMidnight()
    {
        var r = CompileSingle("$tomorrow");
        var expected = new DateTimeOffset(2025, 6, 16, 0, 0, 0, TimeSpan.Zero);
        Assert.Equal(expected, r.Start);
    }

    // -----------------------------------------------------------------------
    // ISO-8601 literal anchors
    // -----------------------------------------------------------------------

    [Fact]
    public void Compile_IsoDateOnly_ResolvesToFullDay()
    {
        var r = CompileSingle("2025-01-10");
        Assert.Equal(new DateTimeOffset(2025, 1, 10, 0, 0, 0, TimeSpan.Zero), r.Start);
        Assert.Equal(new DateTimeOffset(2025, 1, 11, 0, 0, 0, TimeSpan.Zero), r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_IsoDateTimeFull_Resolves()
    {
        var r = CompileSingle("2025-01-10T09:30:15.123");
        var expected = new DateTimeOffset(2025, 1, 10, 9, 30, 15, 123, TimeSpan.Zero);
        Assert.Equal(expected, r.Start);
    }

    [Fact]
    public void Compile_IsoDateTimeMinuteOnly_Resolves()
    {
        var r = CompileSingle("2025-03-20T14:45");
        var expected = new DateTimeOffset(2025, 3, 20, 14, 45, 0, TimeSpan.Zero);
        Assert.Equal(expected, r.Start);
    }

    // -----------------------------------------------------------------------
    // Arithmetic offsets
    // -----------------------------------------------------------------------

    [Fact]
    public void Compile_NowMinus5Minutes_SubtractsCorrectly()
    {
        var r = CompileSingle("$now - 5m");
        Assert.Equal(Now.AddMinutes(-5), r.Start);
    }

    [Fact]
    public void Compile_NowPlus2Hours_AddsCorrectly()
    {
        var r = CompileSingle("$now + 2h");
        Assert.Equal(Now.AddHours(2), r.Start);
    }

    [Fact]
    public void Compile_CompoundDuration_1h30m()
    {
        var r = CompileSingle("$now - 1h30m");
        Assert.Equal(Now.AddHours(-1).AddMinutes(-30), r.Start);
    }

    [Fact]
    public void Compile_MultipleOffsets_SubtractAndAdd()
    {
        var r = CompileSingle("$now - 2h + 15m");
        Assert.Equal(Now.AddHours(-2).AddMinutes(15), r.Start);
    }

    [Fact]
    public void Compile_IsoLiteralWithOffset()
    {
        var r = CompileSingle("2025-01-10T12:00:00 - 30m");
        var expected = new DateTimeOffset(2025, 1, 10, 11, 30, 0, TimeSpan.Zero);
        Assert.Equal(expected, r.Start);
    }

    // -----------------------------------------------------------------------
    // Duration units coverage
    // -----------------------------------------------------------------------

    [Theory]
    [InlineData("$now - 1y", -365)]
    [InlineData("$now - 1M", -30)]
    [InlineData("$now - 1w", -7)]
    [InlineData("$now - 1d", -1)]
    public void Compile_DayBasedUnits(string expr, int expectedDays)
    {
        var r = CompileSingle(expr);
        Assert.Equal(Now.AddDays(expectedDays), r.Start);
    }

    [Fact]
    public void Compile_MillisecondUnit_T()
    {
        var r = CompileSingle("$now - 500T");
        Assert.Equal(Now.AddMilliseconds(-500), r.Start);
    }

    [Fact]
    public void Compile_MillisecondUnit_ms()
    {
        var r = CompileSingle("$now - 500ms");
        Assert.Equal(Now.AddMilliseconds(-500), r.Start);
    }

    [Fact]
    public void Compile_MicrosecondUnit_u()
    {
        var r = CompileSingle("$now - 100u");
        Assert.Equal(Now.AddTicks(-1000), r.Start);
    }

    // -----------------------------------------------------------------------
    // Range expressions (anchor..anchor)
    // -----------------------------------------------------------------------

    [Fact]
    public void Compile_SimpleRange_NowMinus1hToNow()
    {
        var r = CompileSingle("$now - 1h..$now");
        Assert.Equal(Now.AddHours(-1), r.Start);
        Assert.Equal(Now, r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_Range_YesterdayToToday()
    {
        var r = CompileSingle("$yesterday..$today");
        Assert.Equal(new DateTimeOffset(2025, 6, 14, 0, 0, 0, TimeSpan.Zero), r.Start);
        // Half-open: end is midnight of next day after $today
        Assert.Equal(new DateTimeOffset(2025, 6, 16, 0, 0, 0, TimeSpan.Zero), r.ExclusiveEnd);
    }

    [Theory]
    [InlineData("2025-01-10@UTC", 0)]
    [InlineData("2025-01-10@Z", 0)]
    [InlineData("2025-01-10@+02:00", 2)]
    [InlineData("2025-01-10@+03", 3)]
    [InlineData("2025-01-10@-0500", -5)]
    public void Compile_DateOnly_WithTimezoneOffset(string expr, int expectedHours)
    {
        var r = CompileSingle(expr);
        Assert.Equal(TimeSpan.FromHours(expectedHours), r.Start.Offset);
        Assert.Equal(new DateTimeOffset(2025, 1, 10, 0, 0, 0, TimeSpan.FromHours(expectedHours)), r.Start);
        // Half-open: next day midnight
        Assert.Equal(new DateTimeOffset(2025, 1, 11, 0, 0, 0, TimeSpan.FromHours(expectedHours)), r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_Today_WithTimezoneOffset_UsesTimezoneDay()
    {
        var r = CompileSingle("$today@+02:00");
        Assert.Equal(TimeSpan.FromHours(2), r.Start.Offset);
        Assert.Equal(new DateTimeOffset(2025, 6, 15, 0, 0, 0, TimeSpan.FromHours(2)), r.Start);
    }

    [Fact]
    public void Compile_Today_WithTimeSuffix_ResolvesToSpecificTime()
    {
        var r = CompileSingle("$todayT19:30");
        Assert.Equal(new DateTimeOffset(2025, 6, 15, 19, 30, 0, TimeSpan.Zero), r.Start);
        Assert.Equal(r.Start, r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_BracketedRangeWithTimezoneSuffix_Works()
    {
        var r = CompileSingle("[$now - 2h..$now]@America/New_York");
        Assert.Equal(new DateTimeOffset(2025, 6, 15, 6, 0, 0, TimeSpan.FromHours(-4)), r.Start);
        Assert.Equal(new DateTimeOffset(2025, 6, 15, 8, 0, 0, TimeSpan.FromHours(-4)), r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_DateOnly_WithIanaTimezone_ResolvesToZoneOffset()
    {
        var r = CompileSingle("2025-01-10@Europe/Rome");
        Assert.Equal(new DateTimeOffset(2025, 1, 10, 0, 0, 0, TimeSpan.FromHours(1)), r.Start);
        // Half-open: next day midnight in same zone
        Assert.Equal(new DateTimeOffset(2025, 1, 11, 0, 0, 0, TimeSpan.FromHours(1)), r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_Now_WithIanaTimezone_ConvertsInstant()
    {
        var r = CompileSingle("$now@Europe/Rome");
        Assert.Equal(new DateTimeOffset(2025, 6, 15, 14, 0, 0, TimeSpan.FromHours(2)), r.Start);
    }

    [Fact]
    public void Compile_IanaTimezone_DstSpringForward_InvalidLocalTime_ShiftsForwardToValid()
    {
        var r = CompileSingle("2025-03-30T02:30@Europe/Rome");
        // 02:30 does not exist on DST spring-forward day; shifts to 03:00 CEST.
        Assert.Equal(new DateTimeOffset(2025, 3, 30, 3, 0, 0, TimeSpan.FromHours(2)), r.Start);
        Assert.Equal(r.Start, r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_IanaTimezone_DstFallBack_AmbiguousLocalTime_ChoosesSmallerOffset()
    {
        var r = CompileSingle("2025-10-26T02:30@Europe/Rome");
        // 02:30 is ambiguous on DST fall-back day; picks smaller offset (+01:00 = standard time).
        Assert.Equal(new DateTimeOffset(2025, 10, 26, 2, 30, 0, TimeSpan.FromHours(1)), r.Start);
        Assert.Equal(r.Start, r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_DateList_WithPerElementTimezone_Works()
    {
        var intervals = CompileMulti("[2024-01-15@UTC,2024-01-17@+02:00]");
        Assert.Equal(2, intervals.Count);
        Assert.Equal(TimeSpan.Zero, intervals[0].Start.Offset);
        Assert.Equal(TimeSpan.FromHours(2), intervals[1].Start.Offset);
    }

    [Fact]
    public void Compile_TimeListWithDuration_MergesOverlappingIntervals()
    {
        var intervals = CompileMulti("2026-03-05T[09:00,10:30];2h");
        Assert.Single(intervals);
        Assert.Equal(new DateTimeOffset(2026, 3, 5, 9, 0, 0, TimeSpan.Zero), intervals[0].Start);
        Assert.Equal(new DateTimeOffset(2026, 3, 5, 12, 30, 0, TimeSpan.Zero), intervals[0].ExclusiveEnd);
    }

    [Fact]
    public void Compile_TimeListWithPerElementTimezonesAndDuration_Works()
    {
        var intervals = CompileMulti("2024-01-15T[09:30@America/New_York,08:00@Europe/London,09:00@Asia/Tokyo];6h");
        Assert.Equal(3, intervals.Count);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 9, 0, 0, TimeSpan.FromHours(9)), intervals[0].Start);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 8, 0, 0, TimeSpan.Zero), intervals[1].Start);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 9, 30, 0, TimeSpan.FromHours(-5)), intervals[2].Start);
    }

    [Fact]
    public void Compile_Range_IsoLiterals()
    {
        var r = CompileSingle("2025-01-10T09:00:00..2025-01-10T17:00:00");
        Assert.Equal(new DateTimeOffset(2025, 1, 10, 9, 0, 0, TimeSpan.Zero), r.Start);
        Assert.Equal(new DateTimeOffset(2025, 1, 10, 17, 0, 0, TimeSpan.Zero), r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_Range_MixedAnchors()
    {
        var r = CompileSingle("2025-06-15T10:00:00..$now");
        Assert.Equal(new DateTimeOffset(2025, 6, 15, 10, 0, 0, TimeSpan.Zero), r.Start);
        Assert.Equal(Now, r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_Range_BothAnchorsWithArithmetic()
    {
        var r = CompileSingle("$now - 2h..$now - 30m");
        Assert.Equal(Now.AddHours(-2), r.Start);
        Assert.Equal(Now.AddMinutes(-30), r.ExclusiveEnd);
    }

    // -----------------------------------------------------------------------
    // Duration expressions (anchor;duration)
    // -----------------------------------------------------------------------

    [Fact]
    public void Compile_Duration_NowMinus1hSpan30m()
    {
        var r = CompileSingle("$now - 1h;30m");
        Assert.Equal(Now.AddHours(-1), r.Start);
        Assert.Equal(Now.AddHours(-1).AddMinutes(30), r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_Duration_IsoLiteralSpan2h()
    {
        var r = CompileSingle("2025-01-10T09:00:00;2h");
        Assert.Equal(new DateTimeOffset(2025, 1, 10, 9, 0, 0, TimeSpan.Zero), r.Start);
        Assert.Equal(new DateTimeOffset(2025, 1, 10, 11, 0, 0, TimeSpan.Zero), r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_Duration_CompoundDuration()
    {
        var r = CompileSingle("$today;1h30m");
        var midnight = new DateTimeOffset(2025, 6, 15, 0, 0, 0, TimeSpan.Zero);
        Assert.Equal(midnight, r.Start);
        Assert.Equal(midnight.AddHours(1).AddMinutes(30), r.ExclusiveEnd);
    }

    // -----------------------------------------------------------------------
    // TryParseDuration (public helper)
    // -----------------------------------------------------------------------

    [Theory]
    [InlineData("1h", 60)]
    [InlineData("30m", 30)]
    [InlineData("1h30m", 90)]
    [InlineData("2d", 2 * 24 * 60)]
    [InlineData("500ms", 0)]
    public void TryParseDuration_ValidInputs(string input, int expectedMinutes)
    {
        Assert.True(TickCompiler.TryParseDuration(input, out var duration));
        if (expectedMinutes > 0)
            Assert.Equal(TimeSpan.FromMinutes(expectedMinutes), duration);
        else
            Assert.True(duration > TimeSpan.Zero);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("abc")]
    [InlineData("$now")]
    public void TryParseDuration_InvalidInputs(string input)
    {
        Assert.False(TickCompiler.TryParseDuration(input, out _));
    }

    // -----------------------------------------------------------------------
    // Edge cases / error handling
    // -----------------------------------------------------------------------

    [Fact]
    public void Compile_EmptyString_ReturnsWarning()
    {
        var result = _compiler.Compile("", Ctx);
        Assert.Empty(result.Intervals);
        Assert.NotNull(result.Warning);
    }

    [Fact]
    public void Compile_NullString_ReturnsWarning()
    {
        var result = _compiler.Compile(null!, Ctx);
        Assert.Empty(result.Intervals);
        Assert.NotNull(result.Warning);
    }

    [Fact]
    public void Compile_GarbageInput_ReturnsWarning()
    {
        var result = _compiler.Compile("not a tick expression", Ctx);
        Assert.Empty(result.Intervals);
        Assert.NotNull(result.Warning);
    }

    [Fact]
    public void Compile_IncompleteRange_ReturnsWarning()
    {
        var result = _compiler.Compile("$now..", Ctx);
        Assert.Empty(result.Intervals);
        Assert.NotNull(result.Warning);
    }

    [Fact]
    public void Compile_VariablesCaseInsensitive()
    {
        var r = CompileSingle("$NOW - 5m..$NOW");
        Assert.Equal(Now.AddMinutes(-5), r.Start);
        Assert.Equal(Now, r.ExclusiveEnd);
    }

    // -----------------------------------------------------------------------
    // FormatTimestamp
    // -----------------------------------------------------------------------

    [Fact]
    public void FormatTimestamp_ProducesDuckDbCompatibleFormat()
    {
        var ts = new DateTimeOffset(2025, 1, 10, 9, 30, 15, 123, TimeSpan.Zero);
        var result = TickCompiler.FormatTimestamp(ts);
        Assert.Equal("2025-01-10 09:30:15.123000", result);
    }

    // =======================================================================
    // Bracket expansion
    // =======================================================================

    [Fact]
    public void Compile_BracketExpansion_SingleValues()
    {
        var intervals = CompileMulti("2024-01-[10,15,20]");
        Assert.Equal(3, intervals.Count);
        Assert.Equal(new DateTimeOffset(2024, 1, 10, 0, 0, 0, TimeSpan.Zero), intervals[0].Start);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 0, 0, 0, TimeSpan.Zero), intervals[1].Start);
        Assert.Equal(new DateTimeOffset(2024, 1, 20, 0, 0, 0, TimeSpan.Zero), intervals[2].Start);
    }

    [Fact]
    public void Compile_BracketExpansion_NumericRange()
    {
        // Half-open: consecutive days [10,11,12,13] merge into single [Jan10..Jan14)
        var intervals = CompileMulti("2024-01-[10..13]");
        Assert.Single(intervals);
        Assert.Equal(new DateTimeOffset(2024, 1, 10, 0, 0, 0, TimeSpan.Zero), intervals[0].Start);
        Assert.Equal(new DateTimeOffset(2024, 1, 14, 0, 0, 0, TimeSpan.Zero), intervals[0].ExclusiveEnd);
    }

    [Fact]
    public void Compile_BracketExpansion_MixedValuesAndRanges()
    {
        // Half-open: day 5 standalone, days 10-12 merge into [10..13), day 20 standalone
        var intervals = CompileMulti("2024-01-[5,10..12,20]");
        Assert.Equal(3, intervals.Count);
        Assert.Equal(new DateTimeOffset(2024, 1, 5, 0, 0, 0, TimeSpan.Zero), intervals[0].Start);
        Assert.Equal(new DateTimeOffset(2024, 1, 10, 0, 0, 0, TimeSpan.Zero), intervals[1].Start);
        Assert.Equal(new DateTimeOffset(2024, 1, 13, 0, 0, 0, TimeSpan.Zero), intervals[1].ExclusiveEnd);
        Assert.Equal(new DateTimeOffset(2024, 1, 20, 0, 0, 0, TimeSpan.Zero), intervals[2].Start);
    }

    [Fact]
    public void Compile_BracketExpansion_MonthExpansion()
    {
        var intervals = CompileMulti("2024-[01,06]-15");
        Assert.Equal(2, intervals.Count);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 0, 0, 0, TimeSpan.Zero), intervals[0].Start);
        Assert.Equal(new DateTimeOffset(2024, 6, 15, 0, 0, 0, TimeSpan.Zero), intervals[1].Start);
    }

    // =======================================================================
    // Cartesian product (multiple brackets)
    // =======================================================================

    [Fact]
    public void Compile_Cartesian_MonthAndDay()
    {
        var intervals = CompileMulti("2024-[01,06]-[10,15]");
        Assert.Equal(4, intervals.Count);
        Assert.Equal(new DateTimeOffset(2024, 1, 10, 0, 0, 0, TimeSpan.Zero), intervals[0].Start);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 0, 0, 0, TimeSpan.Zero), intervals[1].Start);
        Assert.Equal(new DateTimeOffset(2024, 6, 10, 0, 0, 0, TimeSpan.Zero), intervals[2].Start);
        Assert.Equal(new DateTimeOffset(2024, 6, 15, 0, 0, 0, TimeSpan.Zero), intervals[3].Start);
    }

    // =======================================================================
    // Bracket expansion with duration suffix
    // =======================================================================

    [Fact]
    public void Compile_BracketExpansion_WithDuration()
    {
        var intervals = CompileMulti("2024-01-[10,15];1h");
        Assert.Equal(2, intervals.Count);
        Assert.Equal(new DateTimeOffset(2024, 1, 10, 0, 0, 0, TimeSpan.Zero), intervals[0].Start);
        Assert.Equal(new DateTimeOffset(2024, 1, 10, 1, 0, 0, TimeSpan.Zero), intervals[0].ExclusiveEnd);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 0, 0, 0, TimeSpan.Zero), intervals[1].Start);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 1, 0, 0, TimeSpan.Zero), intervals[1].ExclusiveEnd);
    }

    // =======================================================================
    // Date list (top-level brackets)
    // =======================================================================

    [Fact]
    public void Compile_DateList_MultipleIsoLiterals()
    {
        var intervals = CompileMulti("[2024-01-15,2024-03-20]");
        Assert.Equal(2, intervals.Count);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 0, 0, 0, TimeSpan.Zero), intervals[0].Start);
        Assert.Equal(new DateTimeOffset(2024, 3, 20, 0, 0, 0, TimeSpan.Zero), intervals[1].Start);
    }

    [Fact]
    public void Compile_DateList_VariablesAndLiterals()
    {
        // Half-open: $yesterday [Jun14..Jun15) and $today [Jun15..Jun16) are adjacent → merge
        var intervals = CompileMulti("[$today,$yesterday,2024-01-15]");
        Assert.Equal(2, intervals.Count);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 0, 0, 0, TimeSpan.Zero), intervals[0].Start);
        Assert.Equal(new DateTimeOffset(2025, 6, 14, 0, 0, 0, TimeSpan.Zero), intervals[1].Start);
        Assert.Equal(new DateTimeOffset(2025, 6, 16, 0, 0, 0, TimeSpan.Zero), intervals[1].ExclusiveEnd);
    }

    [Fact]
    public void Compile_DateList_WithDuration()
    {
        var intervals = CompileMulti("[$today,$yesterday];1h");
        Assert.Equal(2, intervals.Count);
        // Sorted by start
        Assert.Equal(new DateTimeOffset(2025, 6, 14, 0, 0, 0, TimeSpan.Zero), intervals[0].Start);
        Assert.Equal(new DateTimeOffset(2025, 6, 14, 1, 0, 0, TimeSpan.Zero), intervals[0].ExclusiveEnd);
        Assert.Equal(new DateTimeOffset(2025, 6, 15, 0, 0, 0, TimeSpan.Zero), intervals[1].Start);
        Assert.Equal(new DateTimeOffset(2025, 6, 15, 1, 0, 0, TimeSpan.Zero), intervals[1].ExclusiveEnd);
    }

    // =======================================================================
    // Interval merging
    // =======================================================================

    [Fact]
    public void MergeIntervals_OverlappingPairs()
    {
        var raw = new List<UtcInterval>
        {
            new(new DateTimeOffset(2024, 1, 15, 9, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2024, 1, 15, 11, 0, 0, TimeSpan.Zero)),
            new(new DateTimeOffset(2024, 1, 15, 10, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2024, 1, 15, 12, 0, 0, TimeSpan.Zero)),
        };

        var merged = TickCompiler.MergeIntervals(raw);
        Assert.Single(merged);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 9, 0, 0, TimeSpan.Zero), merged[0].Start);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 12, 0, 0, TimeSpan.Zero), merged[0].ExclusiveEnd);
    }

    [Fact]
    public void MergeIntervals_NonOverlapping_PreservedSorted()
    {
        var raw = new List<UtcInterval>
        {
            new(new DateTimeOffset(2024, 1, 15, 14, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2024, 1, 15, 15, 0, 0, TimeSpan.Zero)),
            new(new DateTimeOffset(2024, 1, 15, 9, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2024, 1, 15, 10, 0, 0, TimeSpan.Zero)),
        };

        var merged = TickCompiler.MergeIntervals(raw);
        Assert.Equal(2, merged.Count);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 9, 0, 0, TimeSpan.Zero), merged[0].Start);
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 14, 0, 0, TimeSpan.Zero), merged[1].Start);
    }

    // =======================================================================
    // Multi-interval API check
    // =======================================================================

    [Fact]
    public void Compile_ReturnsMultipleIntervals_ForBracketExpansion()
    {
        var result = _compiler.Compile("2024-01-[10,20]", Ctx);
        Assert.Null(result.Warning);
        Assert.Equal(2, result.Intervals.Count);
    }

    [Fact]
    public void Compile_ReturnsSingleInterval_ForSimpleRange()
    {
        var result = _compiler.Compile("$now - 1h..$now", Ctx);
        Assert.Null(result.Warning);
        Assert.Single(result.Intervals);
    }

    // =======================================================================
    // NEW: $first / $latest variables
    // =======================================================================

    [Fact]
    public void Compile_FirstVariable_ResolvesFromContext()
    {
        var first = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var ctx = new TickContext(Now, FirstTimestamp: first);
        var r = CompileSingle("$first", ctx);
        Assert.Equal(first, r.Start);
    }

    [Fact]
    public void Compile_LatestVariable_ResolvesFromContext()
    {
        var latest = new DateTimeOffset(2025, 6, 15, 11, 59, 0, TimeSpan.Zero);
        var ctx = new TickContext(Now, LatestTimestamp: latest);
        var r = CompileSingle("$latest", ctx);
        Assert.Equal(latest, r.Start);
    }

    [Fact]
    public void Compile_FirstToLatest_FullSessionRange()
    {
        var first = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var latest = new DateTimeOffset(2025, 6, 15, 11, 59, 0, TimeSpan.Zero);
        var ctx = new TickContext(Now, first, latest);
        var r = CompileSingle("$first..$latest", ctx);
        Assert.Equal(first, r.Start);
        Assert.Equal(latest, r.ExclusiveEnd);
    }

    [Fact]
    public void Compile_StartAlias_ResolvesFromContext()
    {
        var first = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var ctx = new TickContext(Now, FirstTimestamp: first);
        var r = CompileSingle("$start", ctx);
        Assert.Equal(first, r.Start);
    }

    [Fact]
    public void Compile_EndAlias_ResolvesFromContext()
    {
        var latest = new DateTimeOffset(2025, 6, 15, 11, 59, 0, TimeSpan.Zero);
        var ctx = new TickContext(Now, LatestTimestamp: latest);
        var r = CompileSingle("$end", ctx);
        Assert.Equal(latest, r.Start);
    }

    [Fact]
    public void Compile_UnknownVariable_ReturnsWarning()
    {
        var result = _compiler.Compile("$startx", Ctx);
        Assert.Empty(result.Intervals);
        Assert.Contains("Unknown variable", result.Warning);
    }

    // =======================================================================
    // NEW: Error handling
    // =======================================================================

    [Fact]
    public void Compile_InvalidInput_ReturnsWarning()
    {
        var result = _compiler.Compile("not a tick", Ctx);
        Assert.Empty(result.Intervals);
        Assert.NotNull(result.Warning);
    }

    [Fact]
    public void Compile_EmptyInput_ReturnsWarning()
    {
        var result = _compiler.Compile("   ", Ctx);
        Assert.Empty(result.Intervals);
        Assert.NotNull(result.Warning);
    }

    // =======================================================================
    // NEW: SQL Emitter
    // =======================================================================

    [Fact]
    public void SqlEmitter_SingleInterval_EmitsOrClause()
    {
        var emitter = new TickSqlEmitter();
        var intervals = new List<UtcInterval>
        {
            new(new DateTimeOffset(2025, 1, 10, 9, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2025, 1, 10, 17, 0, 0, TimeSpan.Zero))
        };

        var result = emitter.Emit(intervals);
        Assert.Contains("timestamp_utc >= $1", result.WhereSql);
        Assert.Contains("timestamp_utc < $2", result.WhereSql);
        Assert.Equal(2, result.Parameters.Count);
        Assert.Null(result.SetupSql);
    }

    [Fact]
    public void SqlEmitter_MultipleIntervals_EmitsOrChain()
    {
        var emitter = new TickSqlEmitter();
        var intervals = new List<UtcInterval>
        {
            new(new DateTimeOffset(2025, 1, 10, 9, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2025, 1, 10, 17, 0, 0, TimeSpan.Zero)),
            new(new DateTimeOffset(2025, 1, 11, 9, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2025, 1, 11, 17, 0, 0, TimeSpan.Zero)),
        };

        var result = emitter.Emit(intervals);
        Assert.Contains("OR", result.WhereSql);
        Assert.Equal(4, result.Parameters.Count);
        Assert.Null(result.SetupSql);
    }

    [Fact]
    public void SqlEmitter_Over64Intervals_EmitsTempTable()
    {
        var emitter = new TickSqlEmitter();
        var intervals = new List<UtcInterval>();
        var baseDate = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        for (int i = 0; i < 65; i++) {
            intervals.Add(new UtcInterval(baseDate.AddDays(i), baseDate.AddDays(i).AddHours(1)));
        }

        var result = emitter.Emit(intervals);
        Assert.Contains("EXISTS", result.WhereSql);
        Assert.Contains("_q_intervals", result.WhereSql);
        Assert.NotNull(result.SetupSql);
        Assert.Contains("CREATE OR REPLACE TEMP TABLE", result.SetupSql);
    }

    [Fact]
    public void SqlEmitter_EmptyIntervals_EmitsFalse()
    {
        var emitter = new TickSqlEmitter();
        var result = emitter.Emit([]);
        Assert.Equal("FALSE", result.WhereSql);
    }

    // =======================================================================
    // NEW: Half-open interval semantics
    // =======================================================================

    [Fact]
    public void HalfOpenInterval_DateOnly_EndsAtMidnight()
    {
        var r = CompileSingle("2025-03-15");
        Assert.Equal(new DateTimeOffset(2025, 3, 15, 0, 0, 0, TimeSpan.Zero), r.Start);
        // Half-open: ends at midnight of next day, NOT 23:59:59.999...
        Assert.Equal(new DateTimeOffset(2025, 3, 16, 0, 0, 0, TimeSpan.Zero), r.ExclusiveEnd);
    }
}
