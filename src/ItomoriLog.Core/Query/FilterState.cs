namespace ItomoriLog.Core.Query;

/// <summary>
/// Immutable filter state for log queries.
/// </summary>
public sealed record FilterState
{
    public DateTimeOffset? StartUtc { get; init; }
    public DateTimeOffset? EndUtc { get; init; }
    public IReadOnlyList<string> SourceIds { get; init; } = [];
    public IReadOnlyList<string> Levels { get; init; } = [];
    public string? TextSearch { get; init; }
    public string? TickExpression { get; init; }

    public static FilterState Empty => new();
}
