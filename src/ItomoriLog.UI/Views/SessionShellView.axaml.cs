using Avalonia.Controls;
using Avalonia.Input;
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
        AddFolderButton.Click += OnAddFolderClick;
        AddHandler(DragDrop.DropEvent, OnDrop);
        AddHandler(DragDrop.DragOverEvent, OnDragOver);
        DragDrop.SetAllowDrop(this, true);
    }

    public async void OpenFilePicker()
    {
        await PickAndStageFilesAsync();
    }

    private async void OnAddFilesClick(object? sender, RoutedEventArgs e)
    {
        await PickAndStageFilesAsync();
    }

    private async void OnAddFolderClick(object? sender, RoutedEventArgs e)
    {
        await PickAndStageFoldersAsync();
    }

    private async Task PickAndStageFilesAsync()
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
            vm.StageFilesCommand.Execute(paths).Subscribe();
    }

    private async Task PickAndStageFoldersAsync()
    {
        var topLevel = TopLevel.GetTopLevel(this);
        if (topLevel is null) return;

        var folders = await topLevel.StorageProvider.OpenFolderPickerAsync(new FolderPickerOpenOptions
        {
            Title = "Select folders to stage for ingestion",
            AllowMultiple = true
        });

        var paths = folders
            .Select(f => f.TryGetLocalPath())
            .Where(p => p is not null)
            .Cast<string>()
            .ToList();

        if (paths.Count > 0 && DataContext is SessionShellViewModel vm)
            vm.StageFilesCommand.Execute(paths).Subscribe();
    }

    private void OnDragOver(object? sender, DragEventArgs e)
    {
        if (e.Data.Contains(DataFormats.Files))
            e.DragEffects = DragDropEffects.Copy;
        else
            e.DragEffects = DragDropEffects.None;
        e.Handled = true;
    }

    private void OnDrop(object? sender, DragEventArgs e)
    {
        if (!e.Data.Contains(DataFormats.Files))
            return;

        var files = e.Data.GetFiles();
        if (files is null || DataContext is not SessionShellViewModel vm)
            return;

        var paths = files
            .Select(f => f.TryGetLocalPath())
            .Where(p => !string.IsNullOrWhiteSpace(p))
            .Cast<string>()
            .ToList();

        if (paths.Count > 0)
            vm.StageFilesCommand.Execute(paths).Subscribe();
        e.Handled = true;
    }
}
