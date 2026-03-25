using Avalonia;
using Avalonia.Controls;
using Avalonia.Media;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.UI.Views;

public sealed class TimelineRenderSurface : Control
{
    private static readonly SolidColorBrush InfoBrush = new(Color.Parse("#38BDF8"));
    private static readonly SolidColorBrush WarnBrush = new(Color.Parse("#F59E0B"));
    private static readonly SolidColorBrush ErrorBrush = new(Color.Parse("#EF4444"));
    private static readonly SolidColorBrush DebugBrush = new(Color.Parse("#9CA3AF"));
    private static readonly SolidColorBrush DefaultBrush = new(Color.Parse("#D6E2F0"));
    private static readonly SolidColorBrush MatchedBrush = new(Color.Parse("#FFFF8FC2"));
    private static readonly SolidColorBrush SelectionBrush = new(Color.Parse("#40FF6DAE"));
    private static readonly Pen SelectionPen = new(new SolidColorBrush(Color.Parse("#FFFF8FC2")), 1.5);
    private static readonly Pen GridPen = new(new SolidColorBrush(Color.Parse("#2A2F3A")), 1);

    public override void Render(DrawingContext context)
    {
        base.Render(context);

        var bounds = Bounds;
        if (bounds.Width > 0 && bounds.Height > 0)
            context.DrawRectangle(Brushes.Transparent, null, bounds);

        if (DataContext is not TimelineViewModel vm || vm.Bins.Length == 0)
            return;

        if (bounds.Width <= 0 || bounds.Height <= 0)
            return;

        var bins = vm.Bins;
        var maxCount = bins.Max(b => b.Count);
        if (maxCount == 0)
            return;

        var visibleStart = vm.VisibleStart ?? bins[0].Start;
        var visibleEnd = vm.VisibleEnd ?? bins[^1].End;
        var totalTicks = (visibleEnd - visibleStart).Ticks;
        if (totalTicks <= 0)
            return;

        var canvasW = bounds.Width;
        var canvasH = Math.Max(0, bounds.Height - 4);

        context.DrawLine(GridPen, new Point(0, canvasH / 2), new Point(canvasW, canvasH / 2));
        context.DrawLine(GridPen, new Point(0, canvasH), new Point(canvasW, canvasH));

        foreach (var bin in bins)
        {
            if (bin.End <= visibleStart || bin.Start >= visibleEnd)
                continue;

            var x = (bin.Start - visibleStart).Ticks / (double)totalTicks * canvasW;
            var w = (bin.End - bin.Start).Ticks / (double)totalTicks * canvasW;
            var h = bin.Count / (double)maxCount * canvasH;
            var y = canvasH - h;

            if (w < 1)
                w = 1;

            context.DrawRectangle(GetLevelBrush(bin.DominantLevel), null, new Rect(x, y, w, h));

            if (bin.MatchedCount > 0)
            {
                var matchedHeight = bin.MatchedCount / (double)maxCount * canvasH;
                var matchedY = canvasH - matchedHeight;
                context.DrawRectangle(MatchedBrush, null, new Rect(x, matchedY, w, matchedHeight));
            }
        }

        if (vm.SelectedStart.HasValue && vm.SelectedEnd.HasValue)
        {
            var selStart = vm.SelectedStart.Value;
            var selEnd = vm.SelectedEnd.Value;
            var sx = (selStart - visibleStart).Ticks / (double)totalTicks * canvasW;
            var sw = (selEnd - selStart).Ticks / (double)totalTicks * canvasW;
            if (sw < 2)
                sw = 2;
            context.DrawRectangle(SelectionBrush, SelectionPen, new Rect(sx, 0, sw, canvasH));
        }
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
