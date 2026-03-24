namespace ItomoriLog.Core.Query;

/// <summary>
/// A single facet value with its count and selection state.
/// </summary>
public sealed record FacetItem(
    string Value,
    long Count,
    bool IsSelected = false);
