using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Threading;
using Avalonia.VisualTree;

using ItomoriLog.UI.ViewModels;

using System.Reactive.Linq;

namespace ItomoriLog.UI.Views;

public partial class LogGridView : UserControl
{
    private DataGrid? _dataGrid;
    private ScrollViewer? _scrollViewer;

    public LogGridView()
    {
        InitializeComponent();
        AttachedToVisualTree += OnAttachedToVisualTree;
        DetachedFromVisualTree += OnDetachedFromVisualTree;
        DataContextChanged += (_, _) => Dispatcher.UIThread.Post(HookScrollViewer, DispatcherPriority.Loaded);
    }

    private void OnAttachedToVisualTree(object? sender, System.EventArgs e)
    {
        _dataGrid = this.FindControl<DataGrid>("LogDataGrid");
        if (_dataGrid is not null) {
            _dataGrid.LayoutUpdated += OnDataGridLayoutUpdated;
            _dataGrid.SelectionChanged += OnDataGridSelectionChanged;
            _dataGrid.PointerReleased += OnDataGridPointerReleased;
            _dataGrid.KeyDown += OnDataGridKeyDown;
        }

        Dispatcher.UIThread.Post(HookScrollViewer, DispatcherPriority.Loaded);
    }

    private void OnDetachedFromVisualTree(object? sender, System.EventArgs e)
    {
        if (_dataGrid is not null) {
            _dataGrid.LayoutUpdated -= OnDataGridLayoutUpdated;
            _dataGrid.SelectionChanged -= OnDataGridSelectionChanged;
            _dataGrid.PointerReleased -= OnDataGridPointerReleased;
            _dataGrid.KeyDown -= OnDataGridKeyDown;
        }

        if (_scrollViewer is not null)
            _scrollViewer.ScrollChanged -= OnScrollChanged;

        _dataGrid = null;
        _scrollViewer = null;
    }

    private void OnDataGridLayoutUpdated(object? sender, System.EventArgs e) => HookScrollViewer();

    private void HookScrollViewer()
    {
        var dataGrid = _dataGrid ?? this.FindControl<DataGrid>("LogDataGrid");
        if (dataGrid is null)
            return;

        var candidate = dataGrid.GetVisualDescendants()
            .OfType<ScrollViewer>()
            .OrderByDescending(scrollViewer => scrollViewer.Viewport.Height)
            .FirstOrDefault();
        if (candidate is null || ReferenceEquals(candidate, _scrollViewer))
            return;

        if (_scrollViewer is not null)
            _scrollViewer.ScrollChanged -= OnScrollChanged;

        _scrollViewer = candidate;
        _scrollViewer.ScrollChanged += OnScrollChanged;
    }

    private void OnScrollChanged(object? sender, ScrollChangedEventArgs e)
    {
        if (sender is not ScrollViewer scrollViewer || DataContext is not LogsPageViewModel vm)
            return;

        var nearBottom = scrollViewer.Offset.Y + scrollViewer.Viewport.Height >= scrollViewer.Extent.Height - 200;
        if (nearBottom)
            RequestMore(vm);
    }

    private void OnDataGridSelectionChanged(object? sender, SelectionChangedEventArgs e)
    {
        if (DataContext is LogsPageViewModel vm)
            RequestMoreFromSelection(vm);
    }

    private void OnDataGridPointerReleased(object? sender, PointerReleasedEventArgs e)
    {
        if (DataContext is LogsPageViewModel vm)
            Dispatcher.UIThread.Post(() => RequestMoreFromSelection(vm), DispatcherPriority.Background);
    }

    private void OnDataGridKeyDown(object? sender, KeyEventArgs e)
    {
        if (DataContext is not LogsPageViewModel vm)
            return;

        if (e.Key == Key.End && e.KeyModifiers.HasFlag(KeyModifiers.Control)) {
            if (vm.HasNextPage && !vm.IsLoading)
                vm.LoadToEndCommand.Execute().Subscribe();

            e.Handled = true;
            return;
        }

        if (e.Key is Key.Down or Key.PageDown or Key.End)
            Dispatcher.UIThread.Post(() => RequestMoreFromSelection(vm), DispatcherPriority.Background);
    }

    private void RequestMoreFromSelection(LogsPageViewModel vm)
    {
        if (_dataGrid?.SelectedItem is not LogRowDto selectedRow)
            return;

        var rowIndex = vm.CurrentPage.IndexOf(selectedRow);
        if (rowIndex < 0)
            return;

        if (rowIndex >= vm.CurrentPage.Count - 25)
            RequestMore(vm);
    }

    private static void RequestMore(LogsPageViewModel vm)
    {
        if (vm.HasNextPage && !vm.IsLoading)
            vm.LoadMoreCommand.Execute().Subscribe();
    }
}
