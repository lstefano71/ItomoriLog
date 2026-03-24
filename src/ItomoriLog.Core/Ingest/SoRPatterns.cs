using System.Text.RegularExpressions;

namespace ItomoriLog.Core.Ingest;

public static partial class SoRPatterns
{
    // ISO-8601: 2024-03-15T10:30:45.123Z or 2024-03-15 10:30:45,123+02:00
    [GeneratedRegex(@"^(?<ts>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[\.,]?\d{0,7}(?:Z|[+-]\d{2}:?\d{2})?)")]
    public static partial Regex Iso8601();

    // YMD with fractional seconds: 2024-03-15 10:30:45.123
    [GeneratedRegex(@"^(?<ts>\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}:\d{2}[\.,]\d{1,7})")]
    public static partial Regex YmdFractional();

    // Syslog: Mar 15 10:30:45 or Mar  5 10:30:45
    [GeneratedRegex(@"^(?<ts>[A-Z][a-z]{2}\s{1,2}\d{1,2}\s+\d{2}:\d{2}:\d{2})")]
    public static partial Regex Syslog();

    // Apache CLF: [15/Mar/2024:10:30:45 +0200]
    [GeneratedRegex(@"^\[(?<ts>\d{2}/[A-Z][a-z]{2}/\d{4}:\d{2}:\d{2}:\d{2}\s[+-]\d{4})\]")]
    public static partial Regex ApacheClf();

    // Epoch seconds: 1710500000
    [GeneratedRegex(@"^(?<ts>\d{10})(?:\s|,|;)")]
    public static partial Regex EpochSeconds();

    // Epoch milliseconds: 1710500000000
    [GeneratedRegex(@"^(?<ts>\d{13})(?:\s|,|;)")]
    public static partial Regex EpochMillis();

    public static IReadOnlyList<(string Name, Regex Pattern)> All { get; } =
    [
        ("ISO-8601", Iso8601()),
        ("YMD-Fractional", YmdFractional()),
        ("Syslog", Syslog()),
        ("Apache-CLF", ApacheClf()),
        ("Epoch-Seconds", EpochSeconds()),
        ("Epoch-Millis", EpochMillis()),
    ];
}
