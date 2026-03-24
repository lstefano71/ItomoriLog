using Avalonia.Controls;
using Avalonia.Interactivity;
using Avalonia.Platform.Storage;
using ReactiveUI;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.UI.Views;

public partial class SessionShellView : UserControl
{
    public SessionShellView()
    {
        InitializeComponent();
        AddFilesButton.Click += OnAddFilesClick;
    }

    private async void OnAddFilesClick(object? sender, RoutedEventArgs e)
    {
        var topLevel = TopLevel.GetTopLevel(this);
        if (topLevel is null) return;

        var files = await topLevel.StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
        {
            Title = "Select log files to ingest",
            AllowMultiple = true
        });

        if (files.Count == 0) return;

        var paths = files
            .Select(f => f.TryGetLocalPath())
            .Where(p => p is not null)
            .Cast<string>()
            .ToList();

        if (paths.Count > 0 && DataContext is SessionShellViewModel vm)
        {
            vm.IngestFilesCommand.Execute(paths).Subscribe();
        }
    }
}
