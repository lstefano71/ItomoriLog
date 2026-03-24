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
    /// Raised when facet selections change:
    /// (includedLevels, excludedLevels, includedSources, excludedSources).
    /// </summary>
    public event Action<IReadOnlyList<string>, IReadOnlyList<string>, IReadOnlyList<string>, IReadOnlyList<string>>? SelectionChanged;

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
            LevelFacets = MergeWithExisting(levelItems, LevelFacets, OnLevelFacetStateChanged);
            SourceFacets = MergeWithExisting(sourceItems, SourceFacets, OnSourceFacetStateChanged);
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

    public void ResetFilters()
    {
        FilterStart = null;
        FilterEnd = null;

        foreach (var facet in LevelFacets)
            facet.State = FacetSelectionState.Ignore;

        foreach (var facet in SourceFacets)
            facet.State = FacetSelectionState.Ignore;

        NotifySelectionChanged();
    }

    /// <summary>
    /// Get the currently selected (Include) levels.
    /// </summary>
    public IReadOnlyList<string> GetSelectedLevels() => GetIncludedValues(LevelFacets);

    /// <summary>
    /// Get the currently selected (Exclude) levels.
    /// </summary>
    public IReadOnlyList<string> GetExcludedLevels() => GetExcludedValues(LevelFacets);

    /// <summary>
    /// Get the currently selected (Include) sources.
    /// </summary>
    public IReadOnlyList<string> GetSelectedSources() => GetIncludedValues(SourceFacets);

    /// <summary>
    /// Get the currently selected (Exclude) sources.
    /// </summary>
    public IReadOnlyList<string> GetExcludedSources() => GetExcludedValues(SourceFacets);

    /// <summary>
    /// Notify listeners that selection has changed.
    /// </summary>
    public void NotifySelectionChanged()
    {
        InvalidateCache();
        SelectionChanged?.Invoke(
            GetSelectedLevels(),
            GetExcludedLevels(),
            GetSelectedSources(),
            GetExcludedSources());
    }

    private void OnLevelFacetStateChanged(FacetItemViewModel changed)
    {
        NormalizeFacetStates(LevelFacets, changed);
        NotifySelectionChanged();
        RefreshDebounced();
    }

    private void OnSourceFacetStateChanged(FacetItemViewModel changed)
    {
        NormalizeFacetStates(SourceFacets, changed);
        NotifySelectionChanged();
        RefreshDebounced();
    }

    private static List<string> GetIncludedValues(ObservableCollection<FacetItemViewModel> facets)
    {
        return facets
            .Where(f => f.State == FacetSelectionState.Include)
            .Select(f => f.Value)
            .ToList();
    }

    private static List<string> GetExcludedValues(ObservableCollection<FacetItemViewModel> facets)
    {
        return facets
            .Where(f => f.State == FacetSelectionState.Exclude)
            .Select(f => f.Value)
            .ToList();
    }

    private ObservableCollection<FacetItemViewModel> MergeWithExisting(
        FacetItem[] newItems,
        ObservableCollection<FacetItemViewModel> existing,
        Action<FacetItemViewModel> onStateChanged)
    {
        var existingStates = existing.ToDictionary(f => f.Value, f => f.State);
        var result = new ObservableCollection<FacetItemViewModel>();

        foreach (var item in newItems)
        {
            var state = existingStates.GetValueOrDefault(item.Value, FacetSelectionState.Ignore);
            result.Add(new FacetItemViewModel(item.Value, item.Count, onStateChanged)
            {
                State = state
            });
        }

        return result;
    }

    private static void NormalizeFacetStates(
        ObservableCollection<FacetItemViewModel> facets,
        FacetItemViewModel changed)
    {
        if (changed.State == FacetSelectionState.Ignore)
            return;

        var conflictingState = changed.State == FacetSelectionState.Include
            ? FacetSelectionState.Exclude
            : FacetSelectionState.Include;

        foreach (var facet in facets)
        {
            if (!ReferenceEquals(facet, changed) && facet.State == conflictingState)
                facet.State = FacetSelectionState.Ignore;
        }
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
