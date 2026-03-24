using System.Globalization;
using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Ingest.Extractors;

public sealed class JsonTimestampExtractor : ITimestampExtractor
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
        if (raw.Fields is null || !raw.Fields.TryGetValue(_fieldPath, out var value)
            || string.IsNullOrWhiteSpace(value))
        {
            timestamp = default;
            return false;
        }

        var trimmed = value.Trim();

        // Try DateTimeOffset.TryParse (handles ISO-8601 with offsets and Z)
        if (DateTimeOffset.TryParse(trimmed, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal, out timestamp))
        {
            timestamp = TimezonePolicy.ApplyTimeBasis(timestamp, _basis);
            return true;
        }

        // Try exact format parsing
        if (DateTime.TryParseExact(trimmed, Formats, CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.NoCurrentDateDefault, out var dt))
        {
            timestamp = TimezonePolicy.ApplyTimeBasisToBare(dt, _basis);
            return true;
        }

        // Epoch fallback (seconds or milliseconds)
        if (long.TryParse(trimmed, NumberStyles.None, CultureInfo.InvariantCulture, out var epoch))
        {
            timestamp = trimmed.Length >= 13
                ? DateTimeOffset.FromUnixTimeMilliseconds(epoch)
                : DateTimeOffset.FromUnixTimeSeconds(epoch);
            return true;
        }

        // Epoch as double (fractional seconds)
        if (double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out var epochDouble))
        {
            timestamp = DateTimeOffset.FromUnixTimeMilliseconds((long)(epochDouble * 1000));
            return true;
        }

        timestamp = default;
        return false;
    }

    public string Description => $"JsonField({_fieldPath})";
}
