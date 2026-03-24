using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Platform.Storage;
using ReactiveUI;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.UI.Views;

public partial class WelcomeView : UserControl
{
    public WelcomeView()
    {
        InitializeComponent();
        DragDrop.SetAllowDrop(this, true);
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
        if (files is null || DataContext is not WelcomeViewModel vm)
            return;

        var paths = files
            .Select(f => f.TryGetLocalPath())
            .Where(p => !string.IsNullOrWhiteSpace(p))
            .Cast<string>()
            .ToList();

        if (paths.Count > 0)
            vm.CreateSessionFromDroppedPathsCommand.Execute(paths).Subscribe();
        e.Handled = true;
    }
}
