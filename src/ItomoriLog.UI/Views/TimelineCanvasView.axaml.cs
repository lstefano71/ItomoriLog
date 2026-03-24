using System.Reactive.Linq;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Media;
using ReactiveUI;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.UI.Views;

public partial class TimelineCanvasView : UserControl
{
    // Severity brushes (resolved once)
    private static readonly SolidColorBrush InfoBrush = new(Color.Parse("#38BDF8"));
    private static readonly SolidColorBrush WarnBrush = new(Color.Parse("#F59E0B"));
    private static readonly SolidColorBrush ErrorBrush = new(Color.Parse("#EF4444"));
    private static readonly SolidColorBrush DebugBrush = new(Color.Parse("#9CA3AF"));
    private static readonly SolidColorBrush DefaultBrush = new(Color.Parse("#D6E2F0"));
    private static readonly SolidColorBrush SelectionBrush = new(Color.Parse("#40FF6DAE"));
    private static readonly Pen GridPen = new(new SolidColorBrush(Color.Parse("#2A2F3A")), 1);

    private bool _isDragging;
    private Point _dragStart;
    private double _dragStartX;
    private bool _isSelecting;

    public TimelineCanvasView()
    {
        InitializeComponent();

        TimelineCanvas.PointerWheelChanged += OnPointerWheelChanged;
        TimelineCanvas.PointerPressed += OnPointerPressed;
        TimelineCanvas.PointerMoved += OnPointerMoved;
        TimelineCanvas.PointerReleased += OnPointerReleased;

        // Re-render when bins change
        this.WhenAnyValue(x => x.DataContext)
            .OfType<TimelineViewModel>()
            .Subscribe(vm =>
            {
                vm.WhenAnyValue(x => x.Bins)
                    .ObserveOn(RxApp.MainThreadScheduler)
                    .Subscribe(_ => TimelineCanvas.InvalidateVisual());
            });
    }

    protected override void OnPropertyChanged(AvaloniaPropertyChangedEventArgs change)
    {
        base.OnPropertyChanged(change);
        if (change.Property == BoundsProperty)
            TimelineCanvas.InvalidateVisual();
    }

    public override void Render(DrawingContext context)
    {
        base.Render(context);
        RenderTimeline(context);
    }

    private void RenderTimeline(DrawingContext context)
    {
        if (DataContext is not TimelineViewModel vm || vm.Bins.Length == 0)
            return;

        var bounds = TimelineCanvas.Bounds;
        if (bounds.Width <= 0 || bounds.Height <= 0) return;

        var bins = vm.Bins;
        var maxCount = bins.Max(b => b.Count);
        if (maxCount == 0) return;

        var visibleStart = vm.VisibleStart ?? bins[0].Start;
        var visibleEnd = vm.VisibleEnd ?? bins[^1].End;
        var totalTicks = (visibleEnd - visibleStart).Ticks;
        if (totalTicks <= 0) return;

        var canvasW = bounds.Width;
        var canvasH = bounds.Height - 4; // small margin at bottom

        // Draw each bin as a colored rectangle
        foreach (var bin in bins)
        {
            if (bin.End <= visibleStart || bin.Start >= visibleEnd)
                continue;

            var x = (bin.Start - visibleStart).Ticks / (double)totalTicks * canvasW;
            var w = (bin.End - bin.Start).Ticks / (double)totalTicks * canvasW;
            var h = bin.Count / (double)maxCount * canvasH;
            var y = canvasH - h;

            if (w < 1) w = 1;

            var brush = GetLevelBrush(bin.DominantLevel);
            context.DrawRectangle(brush, null, new Rect(x, y, w, h));
        }

        // Draw selection overlay
        if (vm.SelectedStart.HasValue && vm.SelectedEnd.HasValue)
        {
            var selStart = vm.SelectedStart.Value;
            var selEnd = vm.SelectedEnd.Value;
            var sx = (selStart - visibleStart).Ticks / (double)totalTicks * canvasW;
            var sw = (selEnd - selStart).Ticks / (double)totalTicks * canvasW;
            context.DrawRectangle(SelectionBrush, null, new Rect(sx, 0, sw, canvasH));
        }
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

        if (e.GetCurrentPoint(TimelineCanvas).Properties.IsRightButtonPressed)
        {
            // Right-click: start panning
            _isDragging = true;
            e.Handled = true;
        }
        else
        {
            // Left-click: start selection
            _isSelecting = true;
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
        else if (_isSelecting)
        {
            // Show selection preview
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
            _ = vm.RefineVisibleAsync();
        }
        else if (_isSelecting)
        {
            _isSelecting = false;
            var point = e.GetPosition(TimelineCanvas);
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

    private static SolidColorBrush GetLevelBrush(string? level) => level?.ToUpperInvariant() switch
    {
        "INFO" => InfoBrush,
        "WARN" or "WARNING" => WarnBrush,
        "ERROR" or "ERR" or "FATAL" or "CRITICAL" => ErrorBrush,
        "DEBUG" or "DBG" or "TRACE" => DebugBrush,
        _ => DefaultBrush
    };
}
