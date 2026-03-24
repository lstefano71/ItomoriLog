using Avalonia.Controls;
using Avalonia.Input;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.UI.Views;

public partial class CommandPaletteView : UserControl
{
    public CommandPaletteView()
    {
        InitializeComponent();
    }

    protected override void OnPropertyChanged(Avalonia.AvaloniaPropertyChangedEventArgs change)
    {
        base.OnPropertyChanged(change);

        if (change.Property.Name == nameof(IsVisible) && IsVisible)
        {
            // Focus the search box when palette opens
            SearchBox?.Focus();
        }
    }

    private void OnOverlayPressed(object? sender, PointerPressedEventArgs e)
    {
        if (DataContext is CommandPaletteViewModel vm)
            vm.IsOpen = false;
    }

    private void OnSearchBoxKeyDown(object? sender, KeyEventArgs e)
    {
        if (DataContext is not CommandPaletteViewModel vm) return;

        switch (e.Key)
        {
            case Key.Escape:
                vm.IsOpen = false;
                e.Handled = true;
                break;
            case Key.Enter:
                vm.ExecuteSelectedCommand.Execute().Subscribe();
                e.Handled = true;
                break;
            case Key.Down:
                MoveSelection(1);
                e.Handled = true;
                break;
            case Key.Up:
                MoveSelection(-1);
                e.Handled = true;
                break;
        }
    }

    private void MoveSelection(int delta)
    {
        if (DataContext is not CommandPaletteViewModel vm) return;
        if (vm.FilteredCommands.Count == 0) return;

        var currentIndex = vm.SelectedCommand is not null
            ? vm.FilteredCommands.IndexOf(vm.SelectedCommand)
            : -1;

        var newIndex = Math.Clamp(currentIndex + delta, 0, vm.FilteredCommands.Count - 1);
        vm.SelectedCommand = vm.FilteredCommands[newIndex];
    }
}
