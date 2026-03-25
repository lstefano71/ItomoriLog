using System.Collections.ObjectModel;
using System.Reactive.Concurrency;
using System.Reactive;
using FluentAssertions;
using ReactiveUI;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.Tests.Query;

public class LogsPageViewModelTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public LogsPageViewModelTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_logs_vm_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, SessionPaths.DefaultDbFileName);
        _factory = new DuckLakeConnectionFactory(_dbPath);
        RxApp.MainThreadScheduler = CurrentThreadScheduler.Instance;
    }

    [Fact]
    public async Task RefreshResultsAsync_LoadsRowsIntoCurrentPage()
    {
        await SeedRowsAsync(25);

        var vm = new LogsPageViewModel(_factory, "UTC");
        await vm.RefreshResultsAsync(invalidateCache: true);

        vm.CurrentPage.Should().HaveCount(25);
        vm.StatusText.Should().Contain("Loaded 25");
        vm.CurrentPage[0].Message.Should().Be("Event #0");
    }

    [Fact]
    public async Task LoadMoreCommand_AppendsAdditionalRows()
    {
        await SeedRowsAsync(2500);

        var vm = new LogsPageViewModel(_factory, "UTC");
        await vm.RefreshResultsAsync(invalidateCache: true);

        vm.CurrentPage.Should().HaveCount(2000);
        vm.HasNextPage.Should().BeTrue();

        await ExecuteCommandAsync(vm.LoadMoreCommand);

        vm.CurrentPage.Should().HaveCount(2500);
        vm.CurrentPage[^1].Message.Should().Be("Event #2499");
        vm.HasNextPage.Should().BeFalse();
        vm.StatusText.Should().Contain("Loaded 2,500");
    }

    [Fact]
    public async Task RefreshResultsAsync_TickQueryUsesSessionBounds()
    {
        await SeedRowsAsync(25);

        var vm = new LogsPageViewModel(_factory, "UTC")
        {
            QueryText = """timestamp in '$start..$latest'"""
        };

        await vm.RefreshResultsAsync(invalidateCache: true);

        vm.QueryParseError.Should().BeNull();
        vm.CurrentPage.Should().HaveCount(24);
        vm.CurrentPage[0].Message.Should().Be("Event #0");
        vm.StatusText.Should().Contain("Loaded 24");
    }

    [Fact]
    public async Task ClearQueryCommand_ClearsQueryText()
    {
        var vm = new LogsPageViewModel(_factory, "UTC")
        {
            QueryText = "timeout"
        };

        await ExecuteCommandAsync(vm.ClearQueryCommand);

        vm.QueryText.Should().BeEmpty();
    }

    [Fact]
    public void BuildCurrentTimelineMatchFilterState_OmitsSelectionWindow()
    {
        var start = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        var end = start.AddMinutes(5);
        var vm = new LogsPageViewModel(_factory, "UTC")
        {
            QueryText = "timeout",
            StartUtc = start,
            EndUtc = end,
            SelectedLevels = new ObservableCollection<string>(["ERROR"])
        };

        var filter = vm.BuildCurrentTimelineMatchFilterState();

        filter.StartUtc.Should().BeNull();
        filter.EndUtc.Should().BeNull();
        filter.Levels.Should().ContainSingle().Which.Should().Be("ERROR");
        filter.TextSearchQuery.Should().NotBeNull();
    }

    [Fact]
    public async Task LoadToEndCommand_LoadsAllAvailableRows()
    {
        await SeedRowsAsync(4500);

        var vm = new LogsPageViewModel(_factory, "UTC");
        await vm.RefreshResultsAsync(invalidateCache: true);

        await ExecuteCommandAsync(vm.LoadToEndCommand);

        vm.CurrentPage.Should().HaveCount(4500);
        vm.HasNextPage.Should().BeFalse();
        vm.CurrentPage[^1].Message.Should().Be("Event #4499");
    }

    [Fact]
    public async Task RefreshResultsAsync_PreserveLoadedRowsAndSelection_KeepsBrowseContext()
    {
        await SeedRowsAsync(2500);

        var vm = new LogsPageViewModel(_factory, "UTC");
        await vm.RefreshResultsAsync(invalidateCache: true);
        vm.CurrentPage.Should().HaveCount(2000);

        await ExecuteCommandAsync(vm.LoadMoreCommand);
        vm.CurrentPage.Should().HaveCount(2500);
        vm.SelectedRow = vm.CurrentPage[1499];

        await SeedRowsAsync(500, startIndex: 2500);

        await vm.RefreshResultsAsync(invalidateCache: true, preserveLoadedRowCount: true);

        vm.CurrentPage.Count.Should().BeGreaterThanOrEqualTo(2500);
        vm.SelectedRow.Should().NotBeNull();
        vm.SelectedRow!.Message.Should().Be("Event #1499");
    }

    private async Task SeedRowsAsync(int count, int startIndex = 0)
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var inserter = new LogBatchInserter(conn);
        var rows = new List<LogRow>();
        var baseTs = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);

        for (var i = 0; i < count; i++)
        {
            var actualIndex = startIndex + i;
            var timestamp = baseTs.AddSeconds(actualIndex);
            rows.Add(new LogRow(
                TimestampUtc: timestamp,
                TimestampBasis: TimeBasis.Utc,
                TimestampEffectiveOffsetMinutes: 0,
                TimestampOriginal: timestamp.ToString("O"),
                LogicalSourceId: "source-1",
                SourcePath: @"C:\logs\test.log",
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

    private static Task ExecuteCommandAsync(ReactiveCommand<Unit, Unit> command)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        command.Execute().Subscribe(
            _ => { },
            ex => tcs.TrySetException(ex),
            () => tcs.TrySetResult());
        return tcs.Task;
    }
}
