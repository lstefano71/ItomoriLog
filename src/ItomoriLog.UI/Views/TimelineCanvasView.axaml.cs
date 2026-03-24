using System.Reactive;
using System.Reactive.Linq;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using ItomoriLog.Core.Query;
using ReactiveUI;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.UI.Views;

public partial class TimelineCanvasView : UserControl
{
    private const double SelectionDragThreshold = 6;

    private bool _isDragging;
    private Point _dragStart;
    private double _dragStartX;
    private bool _isSelecting;
    private bool _isSlidingSelection;
    private DateTimeOffset? _slideAnchorTimestamp;
    private DateTimeOffset? _slideSelectionStart;
    private DateTimeOffset? _slideSelectionEnd;

    public TimelineCanvasView()
    {
        InitializeComponent();

        TimelineCanvas.PointerWheelChanged += OnPointerWheelChanged;
        TimelineCanvas.PointerPressed += OnPointerPressed;
        TimelineCanvas.PointerMoved += OnPointerMoved;
        TimelineCanvas.PointerReleased += OnPointerReleased;

        this.WhenAnyValue(x => x.DataContext)
            .Select(context => context as TimelineViewModel)
            .Select(vm => vm is null
                ? Observable.Empty<Unit>()
                : vm.WhenAnyValue(
                        x => x.Bins,
                        x => x.VisibleStart,
                        x => x.VisibleEnd,
                        x => x.SelectedStart,
                        x => x.SelectedEnd,
                        (_, _, _, _, _) => Unit.Default))
            .Switch()
            .ObserveOn(RxApp.MainThreadScheduler)
            .Subscribe(_ => TimelineCanvas.InvalidateVisual());
    }

    protected override void OnPropertyChanged(AvaloniaPropertyChangedEventArgs change)
    {
        base.OnPropertyChanged(change);
        if (change.Property == BoundsProperty)
            TimelineCanvas.InvalidateVisual();
    }

    private void OnPointerWheelChanged(object? sender, PointerWheelEventArgs e)
    {
        if (DataContext is not TimelineViewModel vm) return;

        var factor = e.Delta.Y > 0 ? 1.5 : 1.0 / 1.5;
        vm.Zoom(factor);

        // Trigger progressive refine
        _ = vm.RefineVisibleAsync();
    }

    private void OnPointerPressed(object? sender, PointerPressedEventArgs e)
    {
        if (DataContext is not TimelineViewModel vm) return;

        var point = e.GetPosition(TimelineCanvas);
        _dragStart = point;
        _dragStartX = point.X;
        e.Pointer.Capture(TimelineCanvas);

        if (e.GetCurrentPoint(TimelineCanvas).Properties.IsRightButtonPressed)
        {
            if (!IsPointInSelection(vm, point.X) && !TryGetBinAtPoint(vm, point, out _) && vm.HasSelection)
            {
                vm.ClearSelection(notifyListeners: true);
                TimelineCanvas.InvalidateVisual();
                e.Pointer.Capture(null);
                e.Handled = true;
                return;
            }

            // Right-click: start panning
            _isDragging = true;
            e.Handled = true;
        }
        else
        {
            var pointerTs = XToTimestamp(vm, point.X);
            if (pointerTs.HasValue && IsPointInSelection(vm, point.X))
            {
                _isSlidingSelection = true;
                _slideAnchorTimestamp = pointerTs.Value;
                _slideSelectionStart = vm.SelectedStart;
                _slideSelectionEnd = vm.SelectedEnd;
            }
            else
            {
                _isSelecting = true;
            }

            e.Handled = true;
        }
    }

    private void OnPointerMoved(object? sender, PointerEventArgs e)
    {
        if (DataContext is not TimelineViewModel vm) return;

        var point = e.GetPosition(TimelineCanvas);

        if (_isDragging)
        {
            var dx = point.X - _dragStart.X;
            var fraction = -dx / TimelineCanvas.Bounds.Width;
            vm.Pan(fraction);
            _dragStart = point;
            TimelineCanvas.InvalidateVisual();
        }
        else if (_isSlidingSelection)
        {
            var currentTs = XToTimestamp(vm, point.X);
            if (!currentTs.HasValue || !_slideAnchorTimestamp.HasValue || !_slideSelectionStart.HasValue || !_slideSelectionEnd.HasValue)
                return;

            var delta = currentTs.Value - _slideAnchorTimestamp.Value;
            var range = ClampSelectionToSession(vm, _slideSelectionStart.Value + delta, _slideSelectionEnd.Value + delta);
            vm.SelectedStart = range.Start;
            vm.SelectedEnd = range.End;
            TimelineCanvas.InvalidateVisual();
        }
        else if (_isSelecting)
        {
            if (Math.Abs(point.X - _dragStartX) < SelectionDragThreshold)
                return;

            var startTs = XToTimestamp(vm, _dragStartX);
            var endTs = XToTimestamp(vm, point.X);
            if (startTs.HasValue && endTs.HasValue)
            {
                var s = startTs.Value < endTs.Value ? startTs.Value : endTs.Value;
                var en = startTs.Value > endTs.Value ? startTs.Value : endTs.Value;
                vm.SelectedStart = s;
                vm.SelectedEnd = en;
                TimelineCanvas.InvalidateVisual();
            }
        }
    }

    private void OnPointerReleased(object? sender, PointerReleasedEventArgs e)
    {
        if (DataContext is not TimelineViewModel vm) return;

        if (_isDragging)
        {
            _isDragging = false;
            e.Pointer.Capture(null);
            _ = vm.RefineVisibleAsync();
        }
        else if (_isSlidingSelection)
        {
            _isSlidingSelection = false;
            e.Pointer.Capture(null);
            _slideAnchorTimestamp = null;
            _slideSelectionStart = null;
            _slideSelectionEnd = null;

            if (vm.SelectedStart.HasValue && vm.SelectedEnd.HasValue)
                vm.SelectTimeRange(vm.SelectedStart.Value, vm.SelectedEnd.Value);
        }
        else if (_isSelecting)
        {
            _isSelecting = false;
            e.Pointer.Capture(null);
            var point = e.GetPosition(TimelineCanvas);
            if (Math.Abs(point.X - _dragStartX) < SelectionDragThreshold)
            {
                if (TryGetBinAtPoint(vm, point, out var selectedBin))
                {
                    vm.SelectTimeRange(selectedBin.Start, selectedBin.End);
                    TimelineCanvas.InvalidateVisual();
                }
                else if (vm.HasSelection)
                {
                    vm.ClearSelection(notifyListeners: true);
                    TimelineCanvas.InvalidateVisual();
                }

                return;
            }

            var startTs = XToTimestamp(vm, _dragStartX);
            var endTs = XToTimestamp(vm, point.X);

            if (startTs.HasValue && endTs.HasValue)
            {
                vm.SelectTimeRange(startTs.Value, endTs.Value);
            }
        }
    }

    private DateTimeOffset? XToTimestamp(TimelineViewModel vm, double x)
    {
        if (vm.VisibleStart is null || vm.VisibleEnd is null) return null;

        var fraction = x / TimelineCanvas.Bounds.Width;
        fraction = Math.Clamp(fraction, 0, 1);
        var totalTicks = (vm.VisibleEnd.Value - vm.VisibleStart.Value).Ticks;
        return vm.VisibleStart.Value.AddTicks((long)(fraction * totalTicks));
    }

    private bool IsPointInSelection(TimelineViewModel vm, double x)
    {
        if (!vm.HasSelection)
            return false;

        var timestamp = XToTimestamp(vm, x);
        return timestamp.HasValue
            && timestamp.Value >= vm.SelectedStart!.Value
            && timestamp.Value <= vm.SelectedEnd!.Value;
    }

    private bool TryGetBinAtPoint(TimelineViewModel vm, Point point, out TimelineBin selectedBin)
    {
        selectedBin = default!;

        if (vm.Bins.Length == 0 || vm.VisibleStart is null || vm.VisibleEnd is null)
            return false;

        var maxCount = vm.Bins.Max(bin => bin.Count);
        if (maxCount <= 0 || TimelineCanvas.Bounds.Width <= 0 || TimelineCanvas.Bounds.Height <= 0)
            return false;

        var visibleStart = vm.VisibleStart.Value;
        var visibleEnd = vm.VisibleEnd.Value;
        var totalTicks = (visibleEnd - visibleStart).Ticks;
        if (totalTicks <= 0)
            return false;

        var canvasWidth = TimelineCanvas.Bounds.Width;
        var canvasHeight = Math.Max(0, TimelineCanvas.Bounds.Height - 4);
        foreach (var bin in vm.Bins)
        {
            if (bin.Count <= 0 || bin.End <= visibleStart || bin.Start >= visibleEnd)
                continue;

            var x = (bin.Start - visibleStart).Ticks / (double)totalTicks * canvasWidth;
            var width = (bin.End - bin.Start).Ticks / (double)totalTicks * canvasWidth;
            if (width < 1)
                width = 1;

            var height = bin.Count / (double)maxCount * canvasHeight;
            var y = canvasHeight - height;
            var rect = new Rect(x, y, width, height);
            if (rect.Contains(point))
            {
                selectedBin = bin;
                return true;
            }
        }

        return false;
    }

    private static (DateTimeOffset Start, DateTimeOffset End) ClampSelectionToSession(
        TimelineViewModel vm,
        DateTimeOffset start,
        DateTimeOffset end)
    {
        var span = end - start;

        if (vm.SessionStart.HasValue && start < vm.SessionStart.Value)
        {
            start = vm.SessionStart.Value;
            end = start + span;
        }

        if (vm.SessionEnd.HasValue && end > vm.SessionEnd.Value)
        {
            end = vm.SessionEnd.Value;
            start = end - span;
        }

        return (start, end);
    }

}
