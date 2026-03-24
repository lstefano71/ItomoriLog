using Avalonia.Controls;
using Avalonia.Input;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.App;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
        DataContext = new MainWindowViewModel();
    }

    protected override void OnKeyDown(KeyEventArgs e)
    {
        base.OnKeyDown(e);

        if (DataContext is not MainWindowViewModel vm) return;

        // Ctrl+Shift+P: Command palette
        if (e.Key == Key.P
            && e.KeyModifiers.HasFlag(KeyModifiers.Control)
            && e.KeyModifiers.HasFlag(KeyModifiers.Shift))
        {
            vm.CommandPalette.Toggle();
            e.Handled = true;
            return;
        }

        // Ctrl+E: Export dialog
        if (e.Key == Key.E && e.KeyModifiers == KeyModifiers.Control)
        {
            vm.ExportDialog.Open();
            e.Handled = true;
            return;
        }

        // Ctrl+F: Focus filter/search box
        if (e.Key == Key.F && e.KeyModifiers == KeyModifiers.Control)
        {
            // Try to find the search TextBox in the current session view
            var searchBox = this.FindControl<TextBox>("FilterTextBox");
            searchBox?.Focus();
            e.Handled = true;
            return;
        }

        // Escape: Close overlays/drawers
        if (e.Key == Key.Escape)
        {
            if (vm.CommandPalette.IsOpen)
            {
                vm.CommandPalette.IsOpen = false;
                e.Handled = true;
                return;
            }
            if (vm.ExportDialog.IsOpen && !vm.ExportDialog.IsExporting)
            {
                vm.ExportDialog.IsOpen = false;
                e.Handled = true;
                return;
            }
            if (vm.AboutDialog.IsOpen)
            {
                vm.AboutDialog.IsOpen = false;
                e.Handled = true;
                return;
            }
            // Close detail drawer if session is active
            if (vm.CurrentView is SessionShellViewModel session && session.LogsPage is { IsDetailOpen: true })
            {
                session.LogsPage.IsDetailOpen = false;
                e.Handled = true;
                return;
            }
        }

        // PageUp/PageDown: Navigate log pages
        if (vm.CurrentView is SessionShellViewModel sessionVm && sessionVm.LogsPage is { } logsPage)
        {
            if (e.Key == Key.PageDown && logsPage.HasNextPage)
            {
                logsPage.NextPageCommand.Execute().Subscribe();
                e.Handled = true;
                return;
            }
            if (e.Key == Key.PageUp && logsPage.HasPreviousPage)
            {
                logsPage.PreviousPageCommand.Execute().Subscribe();
                e.Handled = true;
                return;
            }
        }
    }
}
