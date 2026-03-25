using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Threading;

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
            Dispatcher.UIThread.Post(FocusSearchBox, DispatcherPriority.Input);
    }

    private void OnOverlayPressed(object? sender, PointerPressedEventArgs e)
    {
        if (DataContext is CommandPaletteViewModel vm)
            vm.Close();
    }

    private void OnSearchBoxKeyDown(object? sender, KeyEventArgs e)
    {
        if (DataContext is not CommandPaletteViewModel vm) return;

        switch (e.Key) {
            case Key.Escape:
                vm.Close();
                e.Handled = true;
                break;
            case Key.Enter:
                vm.ExecuteSelectedCommand.Execute().Subscribe();
                e.Handled = true;
                break;
            case Key.Down:
                MoveSelection(vm, 1);
                e.Handled = true;
                break;
            case Key.Up:
                MoveSelection(vm, -1);
                e.Handled = true;
                break;
            case Key.PageDown:
                MoveSelection(vm, GetPageStep());
                e.Handled = true;
                break;
            case Key.PageUp:
                MoveSelection(vm, -GetPageStep());
                e.Handled = true;
                break;
        }
    }

    private void MoveSelection(CommandPaletteViewModel vm, int delta)
    {
        vm.MoveSelectionBy(delta);
        FocusSearchBox();
    }

    private void FocusSearchBox()
    {
        if (SearchBox is null)
            return;

        SearchBox.Focus();
        SearchBox.CaretIndex = SearchBox.Text?.Length ?? 0;
    }

    private int GetPageStep()
    {
        if (CommandList is null || CommandList.Bounds.Height <= 0)
            return 7;

        return Math.Max(1, (int)(CommandList.Bounds.Height / 44));
    }
}
