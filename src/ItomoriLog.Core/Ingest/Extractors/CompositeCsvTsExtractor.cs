using System.Globalization;
using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Ingest.Extractors;

public sealed class CompositeCsvTsExtractor : ITimestampExtractor
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
        if (raw.Fields is null)
        {
            timestamp = default;
            return false;
        }

        // Composite: concatenate values from all timestamp fields
        if (_timestampFields.Length > 1)
        {
            var parts = new List<string>(_timestampFields.Length);
            foreach (var field in _timestampFields)
            {
                if (raw.Fields.TryGetValue(field, out var val) && !string.IsNullOrWhiteSpace(val))
                    parts.Add(val.Trim());
            }

            if (parts.Count > 0)
            {
                var combined = string.Join(' ', parts);
                if (TryParseTimestamp(combined, out timestamp))
                    return true;
            }
        }

        // Single field or fallback: try each field individually
        foreach (var field in _timestampFields)
        {
            if (raw.Fields.TryGetValue(field, out var val) && !string.IsNullOrWhiteSpace(val))
            {
                if (TryParseTimestamp(val.Trim(), out timestamp))
                    return true;
            }
        }

        timestamp = default;
        return false;
    }

    private bool TryParseTimestamp(string value, out DateTimeOffset ts)
    {
        // Try DateTimeOffset.TryParse (handles offsets and Z)
        if (DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal, out ts))
        {
            ts = TimezonePolicy.ApplyTimeBasis(ts, _basis);
            return true;
        }

        // Try exact format parsing
        if (DateTime.TryParseExact(value, Formats, CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.NoCurrentDateDefault, out var dt))
        {
            ts = TimezonePolicy.ApplyTimeBasisToBare(dt, _basis);
            return true;
        }

        // Epoch fallback
        if (long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var epoch))
        {
            ts = value.Length >= 13
                ? DateTimeOffset.FromUnixTimeMilliseconds(epoch)
                : DateTimeOffset.FromUnixTimeSeconds(epoch);
            return true;
        }

        ts = default;
        return false;
    }

    public string Description => _timestampFields.Length == 1
        ? $"CsvField({_timestampFields[0]})"
        : $"CsvComposite({string.Join('+', _timestampFields)})";
}
