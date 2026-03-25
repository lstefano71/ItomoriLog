using ReactiveUI;

using System.Reactive;

namespace ItomoriLog.UI.ViewModels;

/// <summary>
/// ViewModel for a single facet item (level or source) with tri-state selection.
/// </summary>
public class FacetItemViewModel : ViewModelBase
{
    private FacetSelectionState _state = FacetSelectionState.Ignore;
    private long _count;
    private readonly Action<FacetItemViewModel>? _onStateChanged;

    public FacetItemViewModel(string value, long count, Action<FacetItemViewModel>? onStateChanged = null)
    {
        Value = value;
        Count = count;
        _onStateChanged = onStateChanged;
        CycleStateCommand = ReactiveCommand.Create(() => {
            CycleState();
            _onStateChanged?.Invoke(this);
        });
    }

    public string Value { get; }

    public long Count {
        get => _count;
        set => this.RaiseAndSetIfChanged(ref _count, value);
    }

    public FacetSelectionState State {
        get => _state;
        set => this.RaiseAndSetIfChanged(ref _state, value);
    }

    public ReactiveCommand<Unit, Unit> CycleStateCommand { get; }

    /// <summary>
    /// Cycle through Ignore → Include → Exclude → Ignore.
    /// </summary>
    public void CycleState()
    {
        State = State switch {
            FacetSelectionState.Ignore => FacetSelectionState.Include,
            FacetSelectionState.Include => FacetSelectionState.Exclude,
            FacetSelectionState.Exclude => FacetSelectionState.Ignore,
            _ => FacetSelectionState.Ignore
        };
    }
}
