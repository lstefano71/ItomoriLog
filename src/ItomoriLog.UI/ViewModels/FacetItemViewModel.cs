using ReactiveUI;

namespace ItomoriLog.UI.ViewModels;

/// <summary>
/// ViewModel for a single facet item (level or source) with tri-state selection.
/// </summary>
public class FacetItemViewModel : ViewModelBase
{
    private FacetSelectionState _state = FacetSelectionState.Ignore;
    private long _count;

    public FacetItemViewModel(string value, long count)
    {
        Value = value;
        Count = count;
    }

    public string Value { get; }

    public long Count
    {
        get => _count;
        set => this.RaiseAndSetIfChanged(ref _count, value);
    }

    public FacetSelectionState State
    {
        get => _state;
        set => this.RaiseAndSetIfChanged(ref _state, value);
    }

    /// <summary>
    /// Cycle through Ignore → Include → Exclude → Ignore.
    /// </summary>
    public void CycleState()
    {
        State = State switch
        {
            FacetSelectionState.Ignore => FacetSelectionState.Include,
            FacetSelectionState.Include => FacetSelectionState.Exclude,
            FacetSelectionState.Exclude => FacetSelectionState.Ignore,
            _ => FacetSelectionState.Ignore
        };
    }
}
