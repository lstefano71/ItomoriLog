using System.Globalization;
using System.Text.RegularExpressions;

namespace ItomoriLog.Core.Ingest.Extractors;

public sealed class RegexGroupTsExtractor : ITimestampExtractor
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
        var match = _regex.Match(raw.FirstLine);
        if (!match.Success || !match.Groups[_groupName].Success)
        {
            ts = default;
            return false;
        }

        var value = match.Groups[_groupName].Value;

        // Try DateTimeOffset.TryParse first (handles offsets and Z)
        if (DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal, out ts))
            return true;

        // Try exact format parsing
        if (DateTime.TryParseExact(value, _formats, CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.NoCurrentDateDefault, out var dt))
        {
            ts = new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Local));
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

    public string Description => $"RegexGroup({_groupName})";
}
