namespace ItomoriLog.UI.ViewModels;

/// <summary>
/// Tri-state for facet filtering: Include, Exclude, or Ignore.
/// </summary>
public enum FacetSelectionState
{
    /// <summary>Not used as a filter — records with this value pass through.</summary>
    Ignore,
    /// <summary>Actively include records with this value.</summary>
    Include,
    /// <summary>Actively exclude records with this value.</summary>
    Exclude
}
