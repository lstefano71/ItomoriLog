namespace ItomoriLog.Core.Query;

public sealed record TickCompileResult(
    IReadOnlyList<UtcInterval> Intervals,
    string NormalizedTick,
    string? Warning);
