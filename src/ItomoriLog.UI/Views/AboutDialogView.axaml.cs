using Avalonia.Controls;
using Avalonia.Input;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.UI.Views;

public partial class AboutDialogView : UserControl
{
    public AboutDialogView()
    {
        InitializeComponent();
    }

    private void OnOverlayPressed(object? sender, PointerPressedEventArgs e)
    {
        if (DataContext is AboutDialogViewModel vm)
            vm.IsOpen = false;
    }
}
