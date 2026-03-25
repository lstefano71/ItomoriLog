namespace ItomoriLog.Core.Query;

/// <summary>
/// A single time-bucketed bin in a timeline query result.
/// </summary>
public sealed record TimelineBin(
    DateTimeOffset Start,
    DateTimeOffset End,
    long Count,
    string? DominantLevel,
    long MatchedCount = 0);
