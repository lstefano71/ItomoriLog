using FluentAssertions;

using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.Tests.Query;

public class TimelineViewModelTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public TimelineViewModelTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_timeline_vm_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, SessionPaths.DefaultDbFileName);
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    [Fact]
    public async Task RefreshDataAsync_PreserveViewport_KeepsZoomedWindowAndSelection()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(300, baseTs);

        var vm = new TimelineViewModel(_factory);
        await vm.LoadCoarseBinsAsync();

        var visibleStart = baseTs.AddMinutes(1);
        var visibleEnd = visibleStart.AddMinutes(1);
        var selectedStart = visibleStart.AddSeconds(10);
        var selectedEnd = visibleStart.AddSeconds(20);

        vm.VisibleStart = visibleStart;
        vm.VisibleEnd = visibleEnd;
        await vm.RefineVisibleAsync();
        vm.SelectTimeRange(selectedStart, selectedEnd);

        await SeedRowsAsync(120, baseTs, startIndex: 300);

        await vm.RefreshDataAsync(preserveViewport: true);

        vm.VisibleStart.Should().Be(visibleStart);
        vm.VisibleEnd.Should().Be(visibleEnd);
        vm.SelectedStart.Should().Be(selectedStart);
        vm.SelectedEnd.Should().Be(selectedEnd);
        vm.SessionEnd.Should().BeAfter(visibleEnd);
    }

    [Fact]
    public async Task RefreshDataAsync_WhenShowingFullSession_ExpandsVisibleRange()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(60, baseTs);

        var vm = new TimelineViewModel(_factory);
        await vm.LoadCoarseBinsAsync();
        var initialVisibleEnd = vm.VisibleEnd;

        await SeedRowsAsync(60, baseTs, startIndex: 60);

        await vm.RefreshDataAsync(preserveViewport: true);

        initialVisibleEnd.Should().NotBeNull();
        vm.VisibleEnd.Should().NotBeNull();
        vm.VisibleStart.Should().Be(vm.SessionStart);
        vm.VisibleEnd.Should().Be(vm.SessionEnd);
        vm.VisibleEnd!.Value.Should().BeAfter(initialVisibleEnd!.Value);
    }

    [Fact]
    public async Task ApplyMatchFilterAsync_PreservesViewportAndLoadsMatchedCounts()
    {
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await SeedRowsAsync(120, baseTs);

        var vm = new TimelineViewModel(_factory);
        await vm.LoadCoarseBinsAsync();

        var visibleStart = baseTs;
        var visibleEnd = baseTs.AddSeconds(20);
        vm.VisibleStart = visibleStart;
        vm.VisibleEnd = visibleEnd;
        await vm.RefineVisibleAsync();

        await vm.ApplyMatchFilterAsync(new ItomoriLog.Core.Query.FilterState { TextSearch = "#1" });

        vm.HasActiveMatchFilter.Should().BeTrue();
        vm.VisibleStart.Should().Be(visibleStart);
        vm.VisibleEnd.Should().Be(visibleEnd);
        vm.Bins.Sum(bin => bin.MatchedCount).Should().Be(11);
    }

    private async Task SeedRowsAsync(int count, DateTimeOffset baseTs, int startIndex = 0)
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var inserter = new LogBatchInserter(conn);
        var rows = new List<LogRow>();

        for (var i = 0; i < count; i++) {
            var actualIndex = startIndex + i;
            var timestamp = baseTs.AddSeconds(actualIndex);
            rows.Add(new LogRow(
                TimestampUtc: timestamp,
                TimestampBasis: TimeBasis.Utc,
                TimestampEffectiveOffsetMinutes: 0,
                TimestampOriginal: timestamp.ToString("O"),
                LogicalSourceId: "source-1",
                SourcePath: @"C:\logs\timeline.log",
                PhysicalFileId: "file-1",
                SegmentId: "seg-1",
                IngestRunId: "run-1",
                RecordIndex: actualIndex,
                Level: "INFO",
                Message: $"Event #{actualIndex}",
                FieldsJson: null));
        }

        await inserter.InsertBatchAsync(rows);
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
