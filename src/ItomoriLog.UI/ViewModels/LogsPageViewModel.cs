using System.Collections.ObjectModel;
using System.Reactive;
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

    private PageCursor? _nextCursor;
    private PageCursor? _prevCursor;
    private readonly Stack<PageCursor> _backwardCursors = new();

    private readonly string _defaultTimezone;

    public LogsPageViewModel(DuckLakeConnectionFactory factory, string defaultTimezone)
    {
        _factory = factory;
        _defaultTimezone = defaultTimezone;
        _planner = new QueryPlanner();
        _pager = new RowPager(factory, _planner);

        NextPageCommand = ReactiveCommand.CreateFromTask(
            NextPageAsync,
            this.WhenAnyValue(x => x.HasNextPage));
        PreviousPageCommand = ReactiveCommand.CreateFromTask(
            PreviousPageAsync,
            this.WhenAnyValue(x => x.HasPreviousPage));
        ToggleDetailCommand = ReactiveCommand.Create(() => { IsDetailOpen = !IsDetailOpen; });
        RefreshCommand = ReactiveCommand.CreateFromTask(RefreshAsync);

        // Debounced query execution on filter changes
        this.WhenAnyValue(
                x => x.TextSearch,
                x => x.StartUtc,
                x => x.EndUtc,
                x => x.QueryText)
            .Throttle(TimeSpan.FromMilliseconds(300))
            .ObserveOn(RxApp.MainThreadScheduler)
            .Select(_ => Unit.Default)
            .InvokeCommand(RefreshCommand);
    }

    // --- Properties ---

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

    // --- Commands ---

    public ReactiveCommand<Unit, Unit> NextPageCommand { get; }
    public ReactiveCommand<Unit, Unit> PreviousPageCommand { get; }
    public ReactiveCommand<Unit, Unit> ToggleDetailCommand { get; }
    public ReactiveCommand<Unit, Unit> RefreshCommand { get; }

    // --- Methods ---

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

    public FilterState BuildCurrentFilterState() => BuildFilter();

    private async Task RefreshAsync()
    {
        _backwardCursors.Clear();
        _nextCursor = null;
        _prevCursor = null;
        CurrentPageIndex = 0;
        await ExecuteQueryAsync(null, PageDirection.Forward);
    }

    private async Task NextPageAsync()
    {
        if (_nextCursor is null) return;
        if (_prevCursor is not null)
            _backwardCursors.Push(_prevCursor);
        await ExecuteQueryAsync(_nextCursor, PageDirection.Forward);
        CurrentPageIndex++;
    }

    private async Task PreviousPageAsync()
    {
        if (!_backwardCursors.TryPop(out var cursor))
        {
            // Go back to first page
            await RefreshAsync();
            return;
        }
        await ExecuteQueryAsync(cursor, PageDirection.Forward);
        CurrentPageIndex--;
    }

    private async Task ExecuteQueryAsync(PageCursor? cursor, PageDirection direction)
    {
        IsLoading = true;
        try
        {
            var filter = BuildFilter();
            if (QueryParseError is not null)
            {
                StatusText = $"Query parse error: {QueryParseError}";
                CurrentPage = [];
                _prevCursor = null;
                _nextCursor = null;
                HasNextPage = false;
                HasPreviousPage = CurrentPageIndex > 0 || _backwardCursors.Count > 0;
                return;
            }

            var result = await _pager.FetchPageAsync(filter, cursor, direction);

            var dtos = result.Rows.Select(ProjectToDto).ToList();
            CurrentPage = new ObservableCollection<LogRowDto>(dtos);

            _prevCursor = result.Cursors.First;
            _nextCursor = result.Cursors.Last;
            HasNextPage = result.Rows.Count == _pager.PageSize;
            HasPreviousPage = CurrentPageIndex > 0 || _backwardCursors.Count > 0;

            StatusText = result.Rows.Count > 0
                ? $"Page {CurrentPageIndex + 1} — {result.Rows.Count} rows"
                : "No results";

            // Prefetch next page
            if (_nextCursor is not null && HasNextPage)
                _pager.PrefetchNext(filter, _nextCursor);
        }
        catch (Exception ex)
        {
            StatusText = $"Query error: {ex.Message}";
        }
        finally
        {
            IsLoading = false;
        }
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

    private TimeZoneInfo GetDisplayTimeZone()
    {
        if (!string.IsNullOrEmpty(_defaultTimezone))
        {
            try { return TimeZoneInfo.FindSystemTimeZoneById(_defaultTimezone); }
            catch { /* fall through */ }
        }
        return TimeZoneInfo.Local;
    }
}
