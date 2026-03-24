namespace ItomoriLog.Core.Query;

public sealed record TickContext(
    DateTimeOffset Now,
    DateTimeOffset? FirstTimestamp = null,
    DateTimeOffset? LatestTimestamp = null);
