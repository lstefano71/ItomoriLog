using System.Collections.ObjectModel;
using System.Reactive;
using ReactiveUI;

namespace ItomoriLog.UI.ViewModels;

public sealed record PaletteCommand(
    string Name,
    string Description,
    string? KeyboardShortcut,
    Action Action);

public class CommandPaletteViewModel : ViewModelBase
{
    private string _searchText = "";
    private bool _isOpen;
    private PaletteCommand? _selectedCommand;
    private readonly List<PaletteCommand> _allCommands;

    public CommandPaletteViewModel(IEnumerable<PaletteCommand> commands)
    {
        _allCommands = commands.ToList();
        FilteredCommands = new ObservableCollection<PaletteCommand>(_allCommands);

        ExecuteSelectedCommand = ReactiveCommand.Create(ExecuteSelected);
        DismissCommand = ReactiveCommand.Create(() => { IsOpen = false; });

        this.WhenAnyValue(x => x.SearchText)
            .Subscribe(_ => ApplyFilter());
    }

    public string SearchText
    {
        get => _searchText;
        set => this.RaiseAndSetIfChanged(ref _searchText, value);
    }

    public bool IsOpen
    {
        get => _isOpen;
        set => this.RaiseAndSetIfChanged(ref _isOpen, value);
    }

    public PaletteCommand? SelectedCommand
    {
        get => _selectedCommand;
        set => this.RaiseAndSetIfChanged(ref _selectedCommand, value);
    }

    public ObservableCollection<PaletteCommand> FilteredCommands { get; }

    public ReactiveCommand<Unit, Unit> ExecuteSelectedCommand { get; }
    public ReactiveCommand<Unit, Unit> DismissCommand { get; }

    public void Toggle()
    {
        if (IsOpen)
        {
            IsOpen = false;
        }
        else
        {
            SearchText = "";
            IsOpen = true;
        }
    }

    private void ApplyFilter()
    {
        var filtered = string.IsNullOrEmpty(SearchText)
            ? _allCommands
            : _allCommands.Where(c => FuzzyMatch(c.Name, SearchText)).ToList();

        FilteredCommands.Clear();
        foreach (var cmd in filtered)
            FilteredCommands.Add(cmd);

        SelectedCommand = FilteredCommands.FirstOrDefault();
    }

    private void ExecuteSelected()
    {
        if (SelectedCommand is null) return;
        IsOpen = false;
        SelectedCommand.Action();
    }

    public static bool FuzzyMatch(string text, string query)
    {
        if (string.IsNullOrEmpty(query)) return true;

        int textIndex = 0;
        var lowerText = text.ToLowerInvariant();
        var lowerQuery = query.ToLowerInvariant();

        foreach (var ch in lowerQuery)
        {
            var found = lowerText.IndexOf(ch, textIndex);
            if (found < 0) return false;
            textIndex = found + 1;
        }

        return true;
    }
}
