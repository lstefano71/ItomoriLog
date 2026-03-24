using System.Collections.ObjectModel;
using System.Globalization;
using System.Reactive;
using System.Reactive.Linq;
using ReactiveUI;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.UI.ViewModels;

/// <summary>
/// ViewModel for the timeline canvas. Manages coarse→fine progressive bin loading,
/// visible window state, and cancellation on zoom/pan.
/// </summary>
public class TimelineViewModel : ViewModelBase
{
    private readonly DuckLakeConnectionFactory _factory;
    private readonly TimelineQuery _query;

    private TimelineBin[] _bins = [];
    private DateTimeOffset? _sessionStart;
    private DateTimeOffset? _sessionEnd;
    private DateTimeOffset? _visibleStart;
    private DateTimeOffset? _visibleEnd;
    private DateTimeOffset? _selectedStart;
    private DateTimeOffset? _selectedEnd;
    private TimeSpan _currentBinWidth = TimeSpan.FromHours(1);
    private bool _isLoading;

    private CancellationTokenSource? _pendingCts;
    private readonly object _ctsLock = new();

    public TimelineViewModel(DuckLakeConnectionFactory factory)
    {
        _factory = factory;
        _query = new TimelineQuery(factory);

        LoadCoarseCommand = ReactiveCommand.CreateFromTask(LoadCoarseBinsAsync);
        RefineCommand = ReactiveCommand.CreateFromTask(RefineVisibleAsync);
        ZoomInCommand = ReactiveCommand.CreateFromTask(async () =>
        {
            Zoom(1.5);
            await RefineVisibleAsync();
        });
        ZoomOutCommand = ReactiveCommand.CreateFromTask(async () =>
        {
            Zoom(1.0 / 1.5);
            await RefineVisibleAsync();
        });
        ResetViewCommand = ReactiveCommand.CreateFromTask(ResetViewAsync);
        ClearSelectionCommand = ReactiveCommand.Create(ClearSelectionAndNotify);
    }

    // --- Properties ---

    public TimelineBin[] Bins
    {
        get => _bins;
        set
        {
            this.RaiseAndSetIfChanged(ref _bins, value);
            RaiseAxisProperties();
        }
    }

    public DateTimeOffset? SessionStart
    {
        get => _sessionStart;
        set => this.RaiseAndSetIfChanged(ref _sessionStart, value);
    }

    public DateTimeOffset? SessionEnd
    {
        get => _sessionEnd;
        set => this.RaiseAndSetIfChanged(ref _sessionEnd, value);
    }

    public DateTimeOffset? VisibleStart
    {
        get => _visibleStart;
        set
        {
            this.RaiseAndSetIfChanged(ref _visibleStart, value);
            RaiseAxisProperties();
        }
    }

    public DateTimeOffset? VisibleEnd
    {
        get => _visibleEnd;
        set
        {
            this.RaiseAndSetIfChanged(ref _visibleEnd, value);
            RaiseAxisProperties();
        }
    }

    public DateTimeOffset? SelectedStart
    {
        get => _selectedStart;
        set
        {
            this.RaiseAndSetIfChanged(ref _selectedStart, value);
            RaiseSelectionStateProperties();
        }
    }

    public DateTimeOffset? SelectedEnd
    {
        get => _selectedEnd;
        set
        {
            this.RaiseAndSetIfChanged(ref _selectedEnd, value);
            RaiseSelectionStateProperties();
        }
    }

    public TimeSpan CurrentBinWidth
    {
        get => _currentBinWidth;
        set => this.RaiseAndSetIfChanged(ref _currentBinWidth, value);
    }

    public bool IsLoading
    {
        get => _isLoading;
        set => this.RaiseAndSetIfChanged(ref _isLoading, value);
    }

    public bool HasSelection => SelectedStart.HasValue && SelectedEnd.HasValue;

    public long MaxBinCount => Bins.Length == 0 ? 0 : Bins.Max(bin => bin.Count);

    public string YAxisTopLabel => MaxBinCount.ToString("N0", CultureInfo.InvariantCulture);

    public string YAxisMidLabel => (MaxBinCount / 2).ToString("N0", CultureInfo.InvariantCulture);

    public string VisibleStartLabel => FormatAxisTimestamp(VisibleStart);

    public string VisibleMidLabel =>
        VisibleStart.HasValue && VisibleEnd.HasValue
            ? FormatAxisTimestamp(VisibleStart.Value + TimeSpan.FromTicks((VisibleEnd.Value - VisibleStart.Value).Ticks / 2))
            : string.Empty;

    public string VisibleEndLabel => FormatAxisTimestamp(VisibleEnd);

    public string SelectionDisplay =>
        HasSelection
            ? $"{SelectedStart:yyyy-MM-dd HH:mm:ss} - {SelectedEnd:yyyy-MM-dd HH:mm:ss}"
            : string.Empty;

    // --- Commands ---

    public ReactiveCommand<Unit, Unit> LoadCoarseCommand { get; }
    public ReactiveCommand<Unit, Unit> RefineCommand { get; }
    public ReactiveCommand<Unit, Unit> ZoomInCommand { get; }
    public ReactiveCommand<Unit, Unit> ZoomOutCommand { get; }
    public ReactiveCommand<Unit, Unit> ResetViewCommand { get; }
    public ReactiveCommand<Unit, Unit> ClearSelectionCommand { get; }

    /// <summary>
    /// Raised when the user selects a time range on the timeline.
    /// </summary>
    public event Action<DateTimeOffset, DateTimeOffset>? TimeRangeSelected;
    public event Action? TimeRangeCleared;

    // --- Methods ---

    /// <summary>
    /// Load coarse bins covering the full session time range.
    /// </summary>
    public async Task LoadCoarseBinsAsync()
    {
        var ct = ResetCancellation();
        IsLoading = true;
        try
        {
            var range = await _query.GetTimeRangeAsync(ct: ct);
            if (range is null)
            {
                Bins = [];
                ClearSelection();
                return;
            }

            SessionStart = range.Value.Min;
            SessionEnd = range.Value.Max.AddSeconds(1); // inclusive end
            VisibleStart = SessionStart;
            VisibleEnd = SessionEnd;

            var totalSpan = SessionEnd.Value - SessionStart.Value;
            CurrentBinWidth = TimelineQuery.ChooseCoarseBinWidth(totalSpan);

            var bins = await _query.QueryBinsAsync(
                SessionStart, SessionEnd, CurrentBinWidth, ct: ct);

            ct.ThrowIfCancellationRequested();
            Bins = bins;
        }
        catch (OperationCanceledException) { }
        finally
        {
            IsLoading = false;
        }
    }

    /// <summary>
    /// Refine bins for the current visible window (called on zoom/pan).
    /// </summary>
    public async Task RefineVisibleAsync()
    {
        if (VisibleStart is null || VisibleEnd is null) return;

        var ct = ResetCancellation();
        IsLoading = true;
        try
        {
            var visibleSpan = VisibleEnd.Value - VisibleStart.Value;
            CurrentBinWidth = TimelineQuery.ChooseFineBinWidth(visibleSpan);

            var bins = await _query.QueryBinsAsync(
                VisibleStart, VisibleEnd, CurrentBinWidth, ct: ct);

            ct.ThrowIfCancellationRequested();
            Bins = bins;
        }
        catch (OperationCanceledException) { }
        finally
        {
            IsLoading = false;
        }
    }

    /// <summary>
    /// Apply zoom (positive = zoom in, negative = zoom out) centered on the visible midpoint.
    /// </summary>
    public void Zoom(double factor)
    {
        if (VisibleStart is null || VisibleEnd is null) return;

        var mid = VisibleStart.Value.Add((VisibleEnd.Value - VisibleStart.Value) / 2);
        var halfSpan = (VisibleEnd.Value - VisibleStart.Value) / 2 * (1.0 / factor);

        // Clamp to session bounds
        var newStart = mid - halfSpan;
        var newEnd = mid + halfSpan;

        if (SessionStart.HasValue && newStart < SessionStart.Value)
            newStart = SessionStart.Value;
        if (SessionEnd.HasValue && newEnd > SessionEnd.Value)
            newEnd = SessionEnd.Value;

        if (newEnd - newStart < TimeSpan.FromSeconds(2))
            return; // don't zoom in further than 2s visible

        VisibleStart = newStart;
        VisibleEnd = newEnd;
    }

    /// <summary>
    /// Pan the visible window by a fraction of the current width (positive = right).
    /// </summary>
    public void Pan(double fraction)
    {
        if (VisibleStart is null || VisibleEnd is null) return;

        var span = VisibleEnd.Value - VisibleStart.Value;
        var delta = span * fraction;

        var newStart = VisibleStart.Value + delta;
        var newEnd = VisibleEnd.Value + delta;

        if (SessionStart.HasValue && newStart < SessionStart.Value)
        {
            newStart = SessionStart.Value;
            newEnd = newStart + span;
        }
        if (SessionEnd.HasValue && newEnd > SessionEnd.Value)
        {
            newEnd = SessionEnd.Value;
            newStart = newEnd - span;
        }

        VisibleStart = newStart;
        VisibleEnd = newEnd;
    }

    /// <summary>
    /// Report a time range selection from the canvas.
    /// </summary>
    public void SelectTimeRange(DateTimeOffset start, DateTimeOffset end)
    {
        if (start > end) (start, end) = (end, start);
        SelectedStart = start;
        SelectedEnd = end;
        TimeRangeSelected?.Invoke(start, end);
    }

    public bool TrySelectBinContaining(DateTimeOffset timestamp)
    {
        var selectedBin = Bins.FirstOrDefault(bin => timestamp >= bin.Start && timestamp < bin.End);
        if (selectedBin is null && Bins.Length > 0 && timestamp == Bins[^1].End)
            selectedBin = Bins[^1];

        if (selectedBin is null)
            return false;

        if (selectedBin.Count <= 0)
            return false;

        SelectTimeRange(selectedBin.Start, selectedBin.End);
        return true;
    }

    public void ClearSelection()
    {
        ClearSelection(notifyListeners: false);
    }

    public void ClearSelection(bool notifyListeners)
    {
        SelectedStart = null;
        SelectedEnd = null;
        if (notifyListeners)
            TimeRangeCleared?.Invoke();
    }

    public async Task ResetViewAsync()
    {
        if (SessionStart is null || SessionEnd is null)
            return;

        VisibleStart = SessionStart;
        VisibleEnd = SessionEnd;
        await LoadCoarseBinsAsync();
    }

    private CancellationToken ResetCancellation()
    {
        lock (_ctsLock)
        {
            _pendingCts?.Cancel();
            _pendingCts?.Dispose();
            _pendingCts = new CancellationTokenSource();
            return _pendingCts.Token;
        }
    }

    private void ClearSelectionAndNotify() => ClearSelection(notifyListeners: true);

    private void RaiseSelectionStateProperties()
    {
        this.RaisePropertyChanged(nameof(HasSelection));
        this.RaisePropertyChanged(nameof(SelectionDisplay));
    }

    private void RaiseAxisProperties()
    {
        this.RaisePropertyChanged(nameof(MaxBinCount));
        this.RaisePropertyChanged(nameof(YAxisTopLabel));
        this.RaisePropertyChanged(nameof(YAxisMidLabel));
        this.RaisePropertyChanged(nameof(VisibleStartLabel));
        this.RaisePropertyChanged(nameof(VisibleMidLabel));
        this.RaisePropertyChanged(nameof(VisibleEndLabel));
    }

    private string FormatAxisTimestamp(DateTimeOffset? timestamp)
    {
        if (!timestamp.HasValue || !VisibleStart.HasValue || !VisibleEnd.HasValue)
            return string.Empty;

        var visibleSpan = VisibleEnd.Value - VisibleStart.Value;
        return visibleSpan switch
        {
            _ when visibleSpan <= TimeSpan.FromHours(6) => timestamp.Value.ToString("HH:mm:ss"),
            _ when visibleSpan <= TimeSpan.FromDays(2) => timestamp.Value.ToString("MM-dd HH:mm"),
            _ when visibleSpan <= TimeSpan.FromDays(45) => timestamp.Value.ToString("MM-dd"),
            _ => timestamp.Value.ToString("yyyy-MM-dd")
        };
    }
}
