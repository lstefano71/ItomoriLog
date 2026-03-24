using System.Globalization;
using System.Text.RegularExpressions;

namespace ItomoriLog.Core.Ingest.Extractors;

public sealed class RegexGroupTsExtractor : ITimestampExtractor, ITimestampExtractorWithMetadata
{
    private readonly Regex _regex;
    private readonly string _groupName;
    private readonly string[] _formats;

    private static readonly string[] DefaultFormats =
    [
        "o",
        "yyyy-MM-ddTHH:mm:ss.fffffffK",
        "yyyy-MM-ddTHH:mm:ssK",
        "yyyy-MM-dd HH:mm:ss.fff",
        "yyyy-MM-dd HH:mm:ss,fff",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy/MM/dd HH:mm:ss.fff",
        "yyyy/MM/dd HH:mm:ss",
    ];

    public RegexGroupTsExtractor(Regex regex, string groupName = "ts", string[]? formats = null)
    {
        _regex = regex;
        _groupName = groupName;
        _formats = formats ?? DefaultFormats;
    }

    public bool TryExtract(RawRecord raw, out DateTimeOffset ts)
    {
        if (TryExtractWithMetadata(raw, out var extraction))
        {
            if (extraction.ExplicitTimestamp.HasValue)
            {
                ts = extraction.ExplicitTimestamp.Value;
                return true;
            }

            if (extraction.BareTimestamp.HasValue)
            {
                ts = new DateTimeOffset(DateTime.SpecifyKind(extraction.BareTimestamp.Value, DateTimeKind.Local));
                return true;
            }
        }

        ts = default;
        return false;
    }

    public bool TryExtractWithMetadata(RawRecord raw, out TimestampExtraction extraction)
    {
        var match = _regex.Match(raw.FirstLine);
        if (!match.Success || !match.Groups[_groupName].Success)
        {
            extraction = default!;
            return false;
        }

        var value = match.Groups[_groupName].Value;
        string? alternate = null;

        // Try DateTimeOffset.TryParse first (handles offsets and Z)
        if (DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces, out var explicitTs))
        {
            if (TryFindAlternateTimestamp(raw.FirstLine, value, out var alt))
                alternate = alt;

            extraction = TimestampExtraction.FromExplicit(explicitTs, value, alternate);
            return true;
        }

        // Try exact format parsing
        if (DateTime.TryParseExact(value, _formats, CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.NoCurrentDateDefault, out var dt))
        {
            if (TryFindAlternateTimestamp(raw.FirstLine, value, out var alt))
                alternate = alt;

            extraction = TimestampExtraction.FromBare(dt, value, alternate);
            return true;
        }

        if (TimestampParsing.TryParseWithTwoDigitYearWindow(value, out var yyDt, out var usedWindow))
        {
            if (TryFindAlternateTimestamp(raw.FirstLine, value, out var alt))
                alternate = alt;

            extraction = TimestampExtraction.FromBare(yyDt, value, alternate, usedWindow);
            return true;
        }

        // Epoch fallback
        if (long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var epoch))
        {
            var epochTs = value.Length >= 13
                ? DateTimeOffset.FromUnixTimeMilliseconds(epoch)
                : DateTimeOffset.FromUnixTimeSeconds(epoch);
            if (TryFindAlternateTimestamp(raw.FirstLine, value, out var alt))
                alternate = alt;
            extraction = TimestampExtraction.FromExplicit(epochTs, value, alternate);
            return true;
        }

        extraction = default!;
        return false;
    }

    public string Description => $"RegexGroup({_groupName})";

    private static bool TryFindAlternateTimestamp(string line, string parsedValue, out string alternate)
    {
        alternate = string.Empty;
        if (string.IsNullOrWhiteSpace(line))
            return false;

        var normalizedParsed = parsedValue.Trim();
        var candidates = Regex.Matches(line, @"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[\.,]\d{1,7})?(?:Z|[+-]\d{2}:?\d{2})?");
        foreach (Match candidate in candidates)
        {
            var text = candidate.Value.Trim();
            if (string.Equals(text, normalizedParsed, StringComparison.Ordinal))
                continue;

            alternate = text;
            return true;
        }

        return false;
    }
}
