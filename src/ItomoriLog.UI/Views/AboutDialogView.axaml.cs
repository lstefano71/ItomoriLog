using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Threading;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.UI.Views;

public partial class AboutDialogView : UserControl
{
    public AboutDialogView()
    {
        InitializeComponent();
    }

    protected override void OnPropertyChanged(Avalonia.AvaloniaPropertyChangedEventArgs change)
    {
        base.OnPropertyChanged(change);

        if (change.Property.Name == nameof(IsVisible) && IsVisible)
            Dispatcher.UIThread.Post(FocusCloseButton, DispatcherPriority.Input);
    }

    private void OnOverlayPressed(object? sender, PointerPressedEventArgs e)
    {
        if (DataContext is AboutDialogViewModel vm)
            vm.Close();
    }

    private void OnDialogKeyDown(object? sender, KeyEventArgs e)
    {
        if (DataContext is not AboutDialogViewModel vm)
            return;

        if (e.Key is Key.Escape or Key.Enter)
        {
            vm.Close();
            e.Handled = true;
        }
    }

    private void FocusCloseButton() => CloseButton?.Focus();
}
