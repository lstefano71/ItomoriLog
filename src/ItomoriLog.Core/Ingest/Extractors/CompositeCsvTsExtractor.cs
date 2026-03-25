using ItomoriLog.Core.Model;

using System.Globalization;

namespace ItomoriLog.Core.Ingest.Extractors;

public sealed class CompositeCsvTsExtractor : ITimestampExtractor, ITimestampExtractorWithMetadata
{
    private readonly string[] _timestampFields;
    private readonly TimeBasisConfig _basis;

    private static readonly string[] Formats =
    [
        "o",
        "yyyy-MM-ddTHH:mm:ss.fffffffK",
        "yyyy-MM-ddTHH:mm:ssK",
        "yyyy-MM-dd HH:mm:ss.fff",
        "yyyy-MM-dd HH:mm:ss,fff",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy/MM/dd HH:mm:ss.fff",
        "yyyy/MM/dd HH:mm:ss",
        "MM/dd/yyyy HH:mm:ss",
        "dd/MM/yyyy HH:mm:ss",
        "yyyy-MM-dd",
        "HH:mm:ss.fff",
        "HH:mm:ss",
    ];

    public CompositeCsvTsExtractor(string[] timestampFields, TimeBasisConfig? basis = null)
    {
        _timestampFields = timestampFields;
        _basis = basis ?? new TimeBasisConfig(TimeBasis.Local);
    }

    public bool TryExtract(RawRecord raw, out DateTimeOffset timestamp)
    {
        if (TryExtractWithMetadata(raw, out var extraction)) {
            if (extraction.ExplicitTimestamp.HasValue) {
                timestamp = extraction.ExplicitTimestamp.Value;
                return true;
            }

            if (extraction.BareTimestamp.HasValue) {
                timestamp = TimezonePolicy.ApplyTimeBasisToBare(extraction.BareTimestamp.Value, _basis);
                return true;
            }
        }

        timestamp = default;
        return false;
    }

    public bool TryExtractWithMetadata(RawRecord raw, out TimestampExtraction extraction)
    {
        if (raw.Fields is null) {
            extraction = default!;
            return false;
        }

        // Composite: concatenate values from all timestamp fields
        if (_timestampFields.Length > 1) {
            var parts = new List<string>(_timestampFields.Length);
            foreach (var field in _timestampFields) {
                if (raw.Fields.TryGetValue(field, out var val) && !string.IsNullOrWhiteSpace(val))
                    parts.Add(val.Trim());
            }

            if (parts.Count > 0) {
                var combined = string.Join(' ', parts);
                if (TryParseTimestamp(combined, out extraction))
                    return true;
            }
        }

        // Single field or fallback: try each field individually
        foreach (var field in _timestampFields) {
            if (raw.Fields.TryGetValue(field, out var val) && !string.IsNullOrWhiteSpace(val)) {
                if (TryParseTimestamp(val.Trim(), out extraction))
                    return true;
            }
        }

        extraction = default!;
        return false;
    }

    private bool TryParseTimestamp(string value, out TimestampExtraction extraction)
    {
        // Try DateTimeOffset.TryParse (handles offsets and Z)
        if (DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces, out var explicitTs)) {
            extraction = TimestampExtraction.FromExplicit(explicitTs, value);
            return true;
        }

        // Try exact format parsing
        if (DateTime.TryParseExact(value, Formats, CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.NoCurrentDateDefault, out var dt)) {
            extraction = TimestampExtraction.FromBare(dt, value);
            return true;
        }

        if (TimestampParsing.TryParseWithTwoDigitYearWindow(value, out var yyDt, out var usedWindow)) {
            extraction = TimestampExtraction.FromBare(yyDt, value, usedTwoDigitYear: usedWindow);
            return true;
        }

        // Epoch fallback
        if (long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var epoch)) {
            var epochTs = value.Length >= 13
                ? DateTimeOffset.FromUnixTimeMilliseconds(epoch)
                : DateTimeOffset.FromUnixTimeSeconds(epoch);
            extraction = TimestampExtraction.FromExplicit(epochTs, value);
            return true;
        }

        extraction = default!;
        return false;
    }

    public string Description => _timestampFields.Length == 1
        ? $"CsvField({_timestampFields[0]})"
        : $"CsvComposite({string.Join('+', _timestampFields)})";
}
