using System.Collections.ObjectModel;
using System.Globalization;
using System.Reactive.Concurrency;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using ReactiveUI;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.UI.ViewModels;

public class LogsPageViewModel : ViewModelBase
{
    private readonly DuckLakeConnectionFactory _factory;
    private readonly RowPager _pager;
    private readonly QueryPlanner _planner;
    private readonly TimelineQuery _timelineQuery;
    private readonly SearchQueryParser _searchParser = new();

    private string _queryText = "";
    private string? _textSearch;
    private string? _queryParseError;
    private ObservableCollection<string> _selectedSources = [];
    private ObservableCollection<string> _excludedSources = [];
    private ObservableCollection<string> _selectedLevels = [];
    private ObservableCollection<string> _excludedLevels = [];
    private DateTimeOffset? _startUtc;
    private DateTimeOffset? _endUtc;
    private ObservableCollection<LogRowDto> _currentPage = [];
    private LogRowDto? _selectedRow;
    private bool _isDetailOpen;
    private bool _isLoading;
    private string _statusText = "Ready";
    private int _currentPageIndex;
    private bool _suppressAutoRefresh;

    private PageCursor? _nextCursor;

    private string _defaultTimezone;
    private (DateTimeOffset Min, DateTimeOffset Max)? _cachedSessionTimeRange;

    public LogsPageViewModel(DuckLakeConnectionFactory factory, string defaultTimezone)
    {
        _factory = factory;
        _defaultTimezone = defaultTimezone;
        _planner = new QueryPlanner();
        _pager = new RowPager(factory, _planner);
        _timelineQuery = new TimelineQuery(factory);

        NextPageCommand = ReactiveCommand.CreateFromTask(
            LoadMoreAsync,
            this.WhenAnyValue(x => x.HasNextPage, x => x.IsLoading, (hasNext, isLoading) => hasNext && !isLoading));
        LoadToEndCommand = ReactiveCommand.CreateFromTask(
            LoadToEndAsync,
            this.WhenAnyValue(x => x.HasNextPage, x => x.IsLoading, (hasNext, isLoading) => hasNext && !isLoading));
        PreviousPageCommand = ReactiveCommand.Create(
            () => { },
            Observable.Return(false));
        LoadMoreCommand = NextPageCommand;
        ToggleDetailCommand = ReactiveCommand.Create(() => { IsDetailOpen = !IsDetailOpen; });
        RefreshCommand = ReactiveCommand.CreateFromTask(RefreshAsync);
        ClearQueryCommand = ReactiveCommand.Create(
            () => { QueryText = string.Empty; },
            this.WhenAnyValue(x => x.QueryText, queryText => !string.IsNullOrWhiteSpace(queryText)));

        this.WhenAnyValue(
                x => x.TextSearch,
                x => x.StartUtc,
                x => x.EndUtc,
                x => x.QueryText,
                x => x.SelectedSources,
                x => x.ExcludedSources,
                x => x.SelectedLevels,
                x => x.ExcludedLevels,
                (_, _, _, _, _, _, _, _) => Unit.Default)
            .Where(_ => !_suppressAutoRefresh)
            .Throttle(TimeSpan.FromMilliseconds(300))
            .ObserveOn(RxApp.MainThreadScheduler)
            .InvokeCommand(RefreshCommand);
    }

    public string QueryText
    {
        get => _queryText;
        set => this.RaiseAndSetIfChanged(ref _queryText, value);
    }

    public string? TextSearch
    {
        get => _textSearch;
        set => this.RaiseAndSetIfChanged(ref _textSearch, value);
    }

    public string? QueryParseError
    {
        get => _queryParseError;
        private set => this.RaiseAndSetIfChanged(ref _queryParseError, value);
    }

    public ObservableCollection<string> SelectedSources
    {
        get => _selectedSources;
        set => this.RaiseAndSetIfChanged(ref _selectedSources, value);
    }

    public ObservableCollection<string> ExcludedSources
    {
        get => _excludedSources;
        set => this.RaiseAndSetIfChanged(ref _excludedSources, value);
    }

    public ObservableCollection<string> SelectedLevels
    {
        get => _selectedLevels;
        set => this.RaiseAndSetIfChanged(ref _selectedLevels, value);
    }

    public ObservableCollection<string> ExcludedLevels
    {
        get => _excludedLevels;
        set => this.RaiseAndSetIfChanged(ref _excludedLevels, value);
    }

    public DateTimeOffset? StartUtc
    {
        get => _startUtc;
        set => this.RaiseAndSetIfChanged(ref _startUtc, value);
    }

    public DateTimeOffset? EndUtc
    {
        get => _endUtc;
        set => this.RaiseAndSetIfChanged(ref _endUtc, value);
    }

    public ObservableCollection<LogRowDto> CurrentPage
    {
        get => _currentPage;
        set => this.RaiseAndSetIfChanged(ref _currentPage, value);
    }

    public LogRowDto? SelectedRow
    {
        get => _selectedRow;
        set => this.RaiseAndSetIfChanged(ref _selectedRow, value);
    }

    public bool IsDetailOpen
    {
        get => _isDetailOpen;
        set => this.RaiseAndSetIfChanged(ref _isDetailOpen, value);
    }

    public bool IsLoading
    {
        get => _isLoading;
        set => this.RaiseAndSetIfChanged(ref _isLoading, value);
    }

    public string StatusText
    {
        get => _statusText;
        set => this.RaiseAndSetIfChanged(ref _statusText, value);
    }

    public int CurrentPageIndex
    {
        get => _currentPageIndex;
        private set => this.RaiseAndSetIfChanged(ref _currentPageIndex, value);
    }

    private bool _hasNextPage;
    public bool HasNextPage
    {
        get => _hasNextPage;
        private set => this.RaiseAndSetIfChanged(ref _hasNextPage, value);
    }

    private bool _hasPreviousPage;
    public bool HasPreviousPage
    {
        get => _hasPreviousPage;
        private set => this.RaiseAndSetIfChanged(ref _hasPreviousPage, value);
    }

    public ReactiveCommand<Unit, Unit> NextPageCommand { get; }
    public ReactiveCommand<Unit, Unit> PreviousPageCommand { get; }
    public ReactiveCommand<Unit, Unit> LoadMoreCommand { get; }
    public ReactiveCommand<Unit, Unit> LoadToEndCommand { get; }
    public ReactiveCommand<Unit, Unit> ToggleDetailCommand { get; }
    public ReactiveCommand<Unit, Unit> RefreshCommand { get; }
    public ReactiveCommand<Unit, Unit> ClearQueryCommand { get; }

    public event Action<FilterState>? TimelineFilterChanged;

    public FilterState BuildCurrentFilterState() => BuildFilter();
    public FilterState BuildCurrentTimelineMatchFilterState() => BuildTimelineMatchFilter(BuildFilter());

    public void InvalidateCache()
    {
        _pager.ClearCache();
        _cachedSessionTimeRange = null;
    }

    public void SetDisplayTimezone(string defaultTimezone)
    {
        _defaultTimezone = defaultTimezone;
        if (CurrentPage.Count > 0)
        {
            var projected = new ObservableCollection<LogRowDto>(CurrentPage.Select(ProjectToDtoFromCurrent));
            if (RxApp.MainThreadScheduler == CurrentThreadScheduler.Instance)
            {
                CurrentPage = projected;
                return;
            }

            RxApp.MainThreadScheduler.Schedule(Unit.Default, (_, _) =>
            {
                CurrentPage = projected;
                return Disposable.Empty;
            });
        }
    }

    public async Task RefreshResultsAsync(bool invalidateCache = false, bool preserveLoadedRowCount = false)
    {
        if (invalidateCache)
            InvalidateCache();

        await RefreshAsync(preserveLoadedRowCount);
    }

    public async Task ResetFiltersAndRefreshAsync(bool invalidateCache = false)
    {
        _suppressAutoRefresh = true;
        try
        {
            await RunOnMainThreadAsync(() =>
            {
                QueryText = "";
                TextSearch = null;
                StartUtc = null;
                EndUtc = null;
                SelectedSources = [];
                ExcludedSources = [];
                SelectedLevels = [];
                ExcludedLevels = [];
                QueryParseError = null;
                SelectedRow = null;
            });
        }
        finally
        {
            _suppressAutoRefresh = false;
        }

        await RefreshResultsAsync(invalidateCache);
    }

    private FilterState BuildFilter()
    {
        var parsed = _searchParser.Parse(QueryText);
        QueryParseError = parsed.Error;
        if (parsed.Error is not null)
        {
            return new FilterState
            {
                StartUtc = StartUtc,
                EndUtc = EndUtc,
                SourceIds = SelectedSources.ToList(),
                ExcludedSourceIds = ExcludedSources.ToList(),
                Levels = SelectedLevels.ToList(),
                ExcludedLevels = ExcludedLevels.ToList(),
                TextSearch = TextSearch,
                TickExpression = null
            };
        }

        return new FilterState
        {
            StartUtc = StartUtc,
            EndUtc = EndUtc,
            SourceIds = SelectedSources.ToList(),
            ExcludedSourceIds = ExcludedSources.ToList(),
            Levels = SelectedLevels.ToList(),
            ExcludedLevels = ExcludedLevels.ToList(),
            TextSearch = parsed.MessageQuery is null ? TextSearch : null,
            TextSearchQuery = parsed.MessageQuery,
            TickExpression = parsed.TickExpression
        };
    }

    private Task RefreshAsync()
        => RefreshAsync(preserveLoadedRowCount: false);

    private async Task RefreshAsync(bool preserveLoadedRowCount)
    {
        var targetRowCount = preserveLoadedRowCount ? CurrentPage.Count : 0;
        var selectedRow = SelectedRow;
        _nextCursor = null;
        await RunOnMainThreadAsync(() => CurrentPageIndex = 0);
        await ExecuteQueryAsync(null, PageDirection.Forward, append: false);

        if (preserveLoadedRowCount)
        {
            while (HasNextPage && CurrentPage.Count < targetRowCount)
            {
                if (_nextCursor is null)
                    break;

                await ExecuteQueryAsync(_nextCursor, PageDirection.Forward, append: true);
                await RunOnMainThreadAsync(() => CurrentPageIndex++);
            }
        }

        await RunOnMainThreadAsync(() => SelectedRow = FindMatchingRow(CurrentPage, selectedRow));
        TimelineFilterChanged?.Invoke(BuildCurrentTimelineMatchFilterState());
    }

    private async Task LoadMoreAsync()
    {
        if (_nextCursor is null)
            return;

        await ExecuteQueryAsync(_nextCursor, PageDirection.Forward, append: true);
        CurrentPageIndex++;
    }

    private async Task LoadToEndAsync()
    {
        while (HasNextPage)
            await LoadMoreAsync();
    }

    private async Task ExecuteQueryAsync(PageCursor? cursor, PageDirection direction, bool append)
    {
        IsLoading = true;
        try
        {
            var filter = BuildFilter();
            if (QueryParseError is not null)
            {
                await RunOnMainThreadAsync(() =>
                {
                    StatusText = $"Query parse error: {QueryParseError}";
                    CurrentPage = [];
                    _nextCursor = null;
                    HasNextPage = false;
                    HasPreviousPage = false;
                });
                return;
            }

            var tickContext = !string.IsNullOrWhiteSpace(filter.TickExpression)
                ? await GetTickContextAsync()
                : null;

            var result = await _pager.FetchPageAsync(filter, cursor, direction, tickContext);

            var dtos = result.Rows.Select(ProjectToDto).ToList();
            await RunOnMainThreadAsync(() =>
            {
                if (append && CurrentPage.Count > 0)
                {
                    foreach (var dto in dtos)
                        CurrentPage.Add(dto);
                }
                else
                {
                    CurrentPage = new ObservableCollection<LogRowDto>(dtos);
                    SelectedRow = null;
                }

                _nextCursor = result.Cursors.Last;
                HasNextPage = result.Rows.Count == _pager.PageSize && result.Cursors.Last is not null;
                HasPreviousPage = false;
                StatusText = CurrentPage.Count > 0
                    ? $"Loaded {CurrentPage.Count.ToString("N0", CultureInfo.InvariantCulture)} rows"
                    : "No results";
            });

            if (_nextCursor is not null && HasNextPage)
                _pager.PrefetchNext(filter, _nextCursor, tickContext);
        }
        catch (InvalidOperationException ex)
        {
            await RunOnMainThreadAsync(() =>
            {
                QueryParseError = ex.Message;
                StatusText = $"Query parse error: {ex.Message}";
                CurrentPage = [];
                _nextCursor = null;
                HasNextPage = false;
                HasPreviousPage = false;
            });
        }
        catch (Exception ex)
        {
            await RunOnMainThreadAsync(() => StatusText = $"Query error: {ex.Message}");
        }
        finally
        {
            await RunOnMainThreadAsync(() => IsLoading = false);
        }
    }

    private static Task RunOnMainThreadAsync(Action action)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        RxApp.MainThreadScheduler.Schedule(Unit.Default, (_, _) =>
        {
            try
            {
                action();
                tcs.SetResult();
            }
            catch (Exception ex)
            {
                tcs.SetException(ex);
            }

            return Disposable.Empty;
        });

        return tcs.Task;
    }

    private LogRowDto ProjectToDto(LogRow row)
    {
        var displayTz = GetDisplayTimeZone();
        var localTs = TimeZoneInfo.ConvertTime(row.TimestampUtc, displayTz);
        var formatted = localTs.ToString("yyyy-MM-dd HH:mm:ss.fff zzz");

        return new LogRowDto(
            Timestamp: formatted,
            Level: row.Level,
            Source: row.LogicalSourceId,
            Message: row.Message,
            FieldsJson: row.FieldsJson,
            TimestampUtc: row.TimestampUtc,
            SegmentId: row.SegmentId,
            RecordIndex: row.RecordIndex,
            TimestampOriginal: row.TimestampOriginal,
            SourcePath: row.SourcePath,
            TimestampBasis: row.TimestampBasis.ToString(),
            OffsetMinutes: row.TimestampEffectiveOffsetMinutes);
    }

    private LogRowDto ProjectToDtoFromCurrent(LogRowDto row)
    {
        var displayTz = GetDisplayTimeZone();
        var localTs = TimeZoneInfo.ConvertTime(row.TimestampUtc, displayTz);
        return row with { Timestamp = localTs.ToString("yyyy-MM-dd HH:mm:ss.fff zzz") };
    }

    private TimeZoneInfo GetDisplayTimeZone()
    {
        if (!string.IsNullOrEmpty(_defaultTimezone))
        {
            try { return TimeZoneInfo.FindSystemTimeZoneById(_defaultTimezone); }
            catch { }
        }

        return TimeZoneInfo.Local;
    }

    private static LogRowDto? FindMatchingRow(IEnumerable<LogRowDto> rows, LogRowDto? selectedRow)
    {
        if (selectedRow is null)
            return null;

        return rows.FirstOrDefault(row =>
            row.TimestampUtc == selectedRow.TimestampUtc
            && string.Equals(row.SegmentId, selectedRow.SegmentId, StringComparison.Ordinal)
            && row.RecordIndex == selectedRow.RecordIndex);
    }

    private async Task<TickContext> GetTickContextAsync()
    {
        if (!_cachedSessionTimeRange.HasValue)
            _cachedSessionTimeRange = await _timelineQuery.GetTimeRangeAsync();

        var now = DateTimeOffset.UtcNow;
        return _cachedSessionTimeRange.HasValue
            ? new TickContext(now, _cachedSessionTimeRange.Value.Min, _cachedSessionTimeRange.Value.Max)
            : new TickContext(now);
    }

    private static FilterState BuildTimelineMatchFilter(FilterState filter) =>
        filter with
        {
            StartUtc = null,
            EndUtc = null
        };
}
