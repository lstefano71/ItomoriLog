using Avalonia.Controls;
using Avalonia.Input;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.UI.Views;

public partial class ExportDialogView : UserControl
{
    public ExportDialogView()
    {
        InitializeComponent();
    }

    private void OnOverlayPressed(object? sender, PointerPressedEventArgs e)
    {
        if (DataContext is ExportDialogViewModel vm && !vm.IsExporting)
            vm.IsOpen = false;
    }
}
