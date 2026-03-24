using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Interactivity;
using Avalonia.Platform.Storage;
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

    private async void OnBrowseClick(object? sender, RoutedEventArgs e)
    {
        if (DataContext is not ExportDialogViewModel vm)
            return;

        var topLevel = TopLevel.GetTopLevel(this);
        if (topLevel is null)
            return;

        var ext = vm.SelectedFormat switch
        {
            Core.Export.ExportFormat.Csv => "csv",
            Core.Export.ExportFormat.JsonLines => "jsonl",
            Core.Export.ExportFormat.Parquet => "parquet",
            _ => "csv"
        };

        var files = await topLevel.StorageProvider.SaveFilePickerAsync(new FilePickerSaveOptions
        {
            Title = "Export logs",
            DefaultExtension = ext
        });

        var localPath = files?.TryGetLocalPath();
        if (!string.IsNullOrWhiteSpace(localPath))
            vm.OutputPath = localPath;
    }
}
