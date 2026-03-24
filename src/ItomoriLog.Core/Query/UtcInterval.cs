namespace ItomoriLog.Core.Query;

/// <summary>Half-open interval [Start, End).</summary>
public readonly record struct UtcInterval(DateTimeOffset Start, DateTimeOffset ExclusiveEnd)
{
    public bool Contains(DateTimeOffset ts) => ts >= Start && ts < ExclusiveEnd;
    public bool Overlaps(UtcInterval other) => Start < other.ExclusiveEnd && other.Start < ExclusiveEnd;
    public TimeSpan Duration => ExclusiveEnd - Start;
}
