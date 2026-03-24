using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Reactive;
using System.Reactive.Linq;
using ReactiveUI;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.UI.ViewModels;

/// <summary>
/// ViewModel for the facet sidebar panel. Manages level and source facets
/// with tri-state selection, count display, caching, and debounced queries.
/// </summary>
public class FacetPanelViewModel : ViewModelBase
{
    private readonly DuckLakeConnectionFactory _factory;
    private readonly FacetQuery _query;

    private ObservableCollection<FacetItemViewModel> _levelFacets = [];
    private ObservableCollection<FacetItemViewModel> _sourceFacets = [];
    private DateTimeOffset? _filterStart;
    private DateTimeOffset? _filterEnd;
    private bool _isLoading;

    // In-memory cache: key = "levels|start|end" or "sources|start|end", value = FacetItem[]
    private readonly ConcurrentDictionary<string, FacetItem[]> _cache = new();
    private CancellationTokenSource? _debounceCts;
    private readonly object _debounceLock = new();

    public FacetPanelViewModel(DuckLakeConnectionFactory factory)
    {
        _factory = factory;
        _query = new FacetQuery(factory);

        RefreshCommand = ReactiveCommand.CreateFromTask(RefreshAsync);
    }

    // --- Properties ---

    public ObservableCollection<FacetItemViewModel> LevelFacets
    {
        get => _levelFacets;
        set => this.RaiseAndSetIfChanged(ref _levelFacets, value);
    }

    public ObservableCollection<FacetItemViewModel> SourceFacets
    {
        get => _sourceFacets;
        set => this.RaiseAndSetIfChanged(ref _sourceFacets, value);
    }

    public DateTimeOffset? FilterStart
    {
        get => _filterStart;
        set => this.RaiseAndSetIfChanged(ref _filterStart, value);
    }

    public DateTimeOffset? FilterEnd
    {
        get => _filterEnd;
        set => this.RaiseAndSetIfChanged(ref _filterEnd, value);
    }

    public bool IsLoading
    {
        get => _isLoading;
        set => this.RaiseAndSetIfChanged(ref _isLoading, value);
    }

    public ReactiveCommand<Unit, Unit> RefreshCommand { get; }

    /// <summary>
    /// Raised when facet selections change (levels, sources).
    /// </summary>
    public event Action<IReadOnlyList<string>, IReadOnlyList<string>>? SelectionChanged;

    // --- Methods ---

    /// <summary>
    /// Refresh facet counts with debouncing (200ms). Invalidates cache on filter change.
    /// </summary>
    public void RefreshDebounced()
    {
        lock (_debounceLock)
        {
            _debounceCts?.Cancel();
            _debounceCts?.Dispose();
            _debounceCts = new CancellationTokenSource();
            var ct = _debounceCts.Token;

            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(200, ct);
                    await RefreshAsync(ct);
                }
                catch (OperationCanceledException) { }
            });
        }
    }

    /// <summary>
    /// Directly refresh (no debounce). Used for initial load.
    /// </summary>
    public async Task RefreshAsync(CancellationToken ct = default)
    {
        IsLoading = true;
        try
        {
            // Get currently included sources for level query
            var includedSources = GetIncludedValues(SourceFacets);
            // Get currently included levels for source query
            var includedLevels = GetIncludedValues(LevelFacets);

            var levelCacheKey = BuildCacheKey("levels", FilterStart, FilterEnd, includedSources);
            var sourceCacheKey = BuildCacheKey("sources", FilterStart, FilterEnd, includedLevels);

            // Query levels
            FacetItem[] levelItems;
            if (!_cache.TryGetValue(levelCacheKey, out levelItems!))
            {
                levelItems = await _query.QueryLevelsAsync(
                    FilterStart, FilterEnd, includedSources.Count > 0 ? includedSources : null, ct);
                _cache[levelCacheKey] = levelItems;
            }

            // Query sources
            FacetItem[] sourceItems;
            if (!_cache.TryGetValue(sourceCacheKey, out sourceItems!))
            {
                sourceItems = await _query.QuerySourcesAsync(
                    FilterStart, FilterEnd, includedLevels.Count > 0 ? includedLevels : null, ct);
                _cache[sourceCacheKey] = sourceItems;
            }

            ct.ThrowIfCancellationRequested();

            // Merge with existing selection states
            LevelFacets = MergeWithExisting(levelItems, LevelFacets);
            SourceFacets = MergeWithExisting(sourceItems, SourceFacets);
        }
        catch (OperationCanceledException) { }
        finally
        {
            IsLoading = false;
        }
    }

    /// <summary>
    /// Invalidate cached facet counts (call when filters change).
    /// </summary>
    public void InvalidateCache() => _cache.Clear();

    /// <summary>
    /// Update filter window and trigger debounced refresh.
    /// </summary>
    public void UpdateTimeWindow(DateTimeOffset? start, DateTimeOffset? end)
    {
        FilterStart = start;
        FilterEnd = end;
        InvalidateCache();
        RefreshDebounced();
    }

    /// <summary>
    /// Get the currently selected (Include) levels.
    /// </summary>
    public IReadOnlyList<string> GetSelectedLevels() => GetIncludedValues(LevelFacets);

    /// <summary>
    /// Get the currently selected (Include) sources.
    /// </summary>
    public IReadOnlyList<string> GetSelectedSources() => GetIncludedValues(SourceFacets);

    /// <summary>
    /// Notify listeners that selection has changed.
    /// </summary>
    public void NotifySelectionChanged()
    {
        InvalidateCache();
        SelectionChanged?.Invoke(GetSelectedLevels(), GetSelectedSources());
    }

    private static List<string> GetIncludedValues(ObservableCollection<FacetItemViewModel> facets)
    {
        return facets
            .Where(f => f.State == FacetSelectionState.Include)
            .Select(f => f.Value)
            .ToList();
    }

    private static ObservableCollection<FacetItemViewModel> MergeWithExisting(
        FacetItem[] newItems,
        ObservableCollection<FacetItemViewModel> existing)
    {
        var existingStates = existing.ToDictionary(f => f.Value, f => f.State);
        var result = new ObservableCollection<FacetItemViewModel>();

        foreach (var item in newItems)
        {
            var state = existingStates.GetValueOrDefault(item.Value, FacetSelectionState.Ignore);
            result.Add(new FacetItemViewModel(item.Value, item.Count) { State = state });
        }

        return result;
    }

    private static string BuildCacheKey(
        string type,
        DateTimeOffset? start,
        DateTimeOffset? end,
        IReadOnlyList<string> filters)
    {
        return $"{type}|{start?.Ticks}|{end?.Ticks}|{string.Join(",", filters)}";
    }
}
