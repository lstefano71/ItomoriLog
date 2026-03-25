using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Interactivity;
using Avalonia.Platform.Storage;
using ReactiveUI;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.UI.Views;

public partial class SessionShellView : UserControl
{
    private SessionShellViewModel? _boundSession;

    public SessionShellView()
    {
        InitializeComponent();
        AddFilesButton.Click += OnAddFilesClick;
        AddFolderButton.Click += OnAddFolderClick;
        ClearQueryButton.Click += OnClearQueryClick;
        QueryTextBox.KeyDown += OnQueryTextBoxKeyDown;
        AddHandler(DragDrop.DropEvent, OnDrop);
        AddHandler(DragDrop.DragOverEvent, OnDragOver);
        DragDrop.SetAllowDrop(this, true);
        DataContextChanged += OnDataContextChanged;
    }

    public async void OpenFilePicker()
    {
        await PickAndStageFilesAsync();
    }

    private void OnDataContextChanged(object? sender, EventArgs e)
    {
        if (_boundSession is not null)
            _boundSession.OpenFilePickerRequested -= OnOpenFilePickerRequested;

        _boundSession = DataContext as SessionShellViewModel;
        if (_boundSession is not null)
            _boundSession.OpenFilePickerRequested += OnOpenFilePickerRequested;
    }

    private async void OnOpenFilePickerRequested()
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

    private void OnClearQueryClick(object? sender, RoutedEventArgs e)
    {
        QueryTextBox.Focus();
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
        if (e.DataTransfer.Contains(DataFormat.File))
            e.DragEffects = DragDropEffects.Copy;
        else
            e.DragEffects = DragDropEffects.None;
        e.Handled = true;
    }

    private void OnDrop(object? sender, DragEventArgs e)
    {
        if (!e.DataTransfer.Contains(DataFormat.File))
            return;

        var files = e.DataTransfer.TryGetFiles();
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

    private void OnQueryTextBoxKeyDown(object? sender, KeyEventArgs e)
    {
        if (e.Key != Key.Escape
            || DataContext is not SessionShellViewModel { LogsPage: { } logsPage }
            || string.IsNullOrWhiteSpace(logsPage.QueryText))
        {
            return;
        }

        logsPage.ClearQueryCommand.Execute().Subscribe();
        QueryTextBox.Focus();
        e.Handled = true;
    }
}
