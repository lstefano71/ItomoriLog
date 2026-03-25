using Avalonia.Data.Converters;

using System.Globalization;

namespace ItomoriLog.UI.ViewModels;

/// <summary>
/// Converts <see cref="FacetSelectionState"/> to a display symbol for the AXAML.
/// </summary>
public class FacetStateToSymbolConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        return value is FacetSelectionState state
            ? state switch {
                FacetSelectionState.Include => "✓",
                FacetSelectionState.Exclude => "✗",
                FacetSelectionState.Ignore => "○",
                _ => "○"
            }
            : "○";
    }

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        throw new NotSupportedException();
    }
}
