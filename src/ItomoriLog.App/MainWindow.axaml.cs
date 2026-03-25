using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.VisualTree;

using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.App;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
    }

    protected override void OnKeyDown(KeyEventArgs e)
    {
        base.OnKeyDown(e);

        if (DataContext is not MainWindowViewModel vm) return;

        // Ctrl+Shift+P: Command palette
        if (e.Key == Key.P
            && e.KeyModifiers.HasFlag(KeyModifiers.Control)
            && e.KeyModifiers.HasFlag(KeyModifiers.Shift)) {
            vm.CommandPalette.Toggle();
            e.Handled = true;
            return;
        }

        // Ctrl+E: Export dialog
        if (e.Key == Key.E && e.KeyModifiers == KeyModifiers.Control) {
            if (vm.CurrentView is SessionShellViewModel session)
                vm.ExportDialog.BindSession(session);
            vm.ExportDialog.Open();
            e.Handled = true;
            return;
        }

        // Ctrl+F: Focus filter/search box
        if (e.Key == Key.F && e.KeyModifiers == KeyModifiers.Control) {
            // Try to find the query text box in the current session view
            var searchBox = this.GetVisualDescendants()
                .OfType<TextBox>()
                .FirstOrDefault(tb => tb.Name == "FilterTextBox" || tb.Name == "QueryTextBox");
            searchBox?.Focus();
            e.Handled = true;
            return;
        }

        // Ctrl+O: Open/Add files in active session
        if (e.Key == Key.O && e.KeyModifiers == KeyModifiers.Control) {
            if (vm.CurrentView is SessionShellViewModel) {
                var shell = this.GetVisualDescendants()
                    .OfType<ItomoriLog.UI.Views.SessionShellView>()
                    .FirstOrDefault();
                shell?.OpenFilePicker();
                e.Handled = true;
                return;
            }
        }

        // Ctrl+I: Start staged ingestion in active session
        if (e.Key == Key.I && e.KeyModifiers == KeyModifiers.Control) {
            if (vm.CurrentView is SessionShellViewModel session) {
                session.StartIngestionCommand.Execute().Subscribe();
                e.Handled = true;
                return;
            }
        }

        // Ctrl+Shift+S: toggle staging queue
        if (e.Key == Key.S
            && e.KeyModifiers.HasFlag(KeyModifiers.Control)
            && e.KeyModifiers.HasFlag(KeyModifiers.Shift)
            && vm.CurrentView is SessionShellViewModel stagingSession) {
            stagingSession.ToggleStagingPaneCommand.Execute().Subscribe();
            e.Handled = true;
            return;
        }

        // F5: refresh current logs page
        if (e.Key == Key.F5 && vm.CurrentView is SessionShellViewModel refreshSession && refreshSession.LogsPage is { } refreshLogs) {
            refreshLogs.RefreshCommand.Execute().Subscribe();
            e.Handled = true;
            return;
        }

        // Escape: Close overlays/drawers
        if (e.Key == Key.Escape) {
            if (vm.CommandPalette.IsOpen) {
                vm.CommandPalette.IsOpen = false;
                e.Handled = true;
                return;
            }
            if (vm.ExportDialog.IsOpen && !vm.ExportDialog.IsExporting) {
                vm.ExportDialog.IsOpen = false;
                e.Handled = true;
                return;
            }
            if (vm.AboutDialog.IsOpen) {
                vm.AboutDialog.IsOpen = false;
                e.Handled = true;
                return;
            }
            // Close detail drawer if session is active
            if (vm.CurrentView is SessionShellViewModel session && session.LogsPage is { IsDetailOpen: true }) {
                session.LogsPage.IsDetailOpen = false;
                e.Handled = true;
                return;
            }
            if (vm.CurrentView is SessionShellViewModel timelineSession && timelineSession.Timeline is { HasSelection: true } timeline) {
                timeline.ClearSelection(notifyListeners: true);
                e.Handled = true;
                return;
            }
        }

        // PageDown: load more rows in the log view
        if (vm.CurrentView is SessionShellViewModel sessionVm && sessionVm.LogsPage is { } logsPage) {
            if (e.Key == Key.End && e.KeyModifiers.HasFlag(KeyModifiers.Control) && logsPage.HasNextPage) {
                logsPage.LoadToEndCommand.Execute().Subscribe();
                e.Handled = true;
                return;
            }

            if (e.Key == Key.PageDown && logsPage.HasNextPage) {
                logsPage.LoadMoreCommand.Execute().Subscribe();
                e.Handled = true;
                return;
            }
        }
    }
}
