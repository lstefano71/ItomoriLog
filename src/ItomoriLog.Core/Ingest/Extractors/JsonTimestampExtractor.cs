using ItomoriLog.Core.Model;

using System.Globalization;

namespace ItomoriLog.Core.Ingest.Extractors;

public sealed class JsonTimestampExtractor : ITimestampExtractor, ITimestampExtractorWithMetadata
{
    private readonly string _fieldPath;
    private readonly TimeBasisConfig _basis;

    private static readonly string[] Formats =
    [
        "o",
        "yyyy-MM-ddTHH:mm:ss.fffffffK",
        "yyyy-MM-ddTHH:mm:ssK",
        "yyyy-MM-ddTHH:mm:ss.fffK",
        "yyyy-MM-dd HH:mm:ss.fff",
        "yyyy-MM-dd HH:mm:ss,fff",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy/MM/dd HH:mm:ss.fff",
        "yyyy/MM/dd HH:mm:ss",
    ];

    public JsonTimestampExtractor(string fieldPath, TimeBasisConfig? basis = null)
    {
        _fieldPath = fieldPath;
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
        if (raw.Fields is null || !raw.Fields.TryGetValue(_fieldPath, out var value)
            || string.IsNullOrWhiteSpace(value)) {
            extraction = default!;
            return false;
        }

        var trimmed = value.Trim();

        // Try DateTimeOffset.TryParse (handles ISO-8601 with offsets and Z)
        if (DateTimeOffset.TryParse(trimmed, CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces, out var explicitTs)) {
            extraction = TimestampExtraction.FromExplicit(explicitTs, trimmed);
            return true;
        }

        // Try exact format parsing
        if (DateTime.TryParseExact(trimmed, Formats, CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.NoCurrentDateDefault, out var dt)) {
            extraction = TimestampExtraction.FromBare(dt, trimmed);
            return true;
        }

        if (TimestampParsing.TryParseWithTwoDigitYearWindow(trimmed, out var yyDt, out var usedWindow)) {
            extraction = TimestampExtraction.FromBare(yyDt, trimmed, usedTwoDigitYear: usedWindow);
            return true;
        }

        // Epoch fallback (seconds or milliseconds)
        if (long.TryParse(trimmed, NumberStyles.None, CultureInfo.InvariantCulture, out var epoch)) {
            var epochTs = trimmed.Length >= 13
                ? DateTimeOffset.FromUnixTimeMilliseconds(epoch)
                : DateTimeOffset.FromUnixTimeSeconds(epoch);
            extraction = TimestampExtraction.FromExplicit(epochTs, trimmed);
            return true;
        }

        // Epoch as double (fractional seconds)
        if (double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out var epochDouble)) {
            var epochTs = DateTimeOffset.FromUnixTimeMilliseconds((long)(epochDouble * 1000));
            extraction = TimestampExtraction.FromExplicit(epochTs, trimmed);
            return true;
        }

        extraction = default!;
        return false;
    }

    public string Description => $"JsonField({_fieldPath})";
}
