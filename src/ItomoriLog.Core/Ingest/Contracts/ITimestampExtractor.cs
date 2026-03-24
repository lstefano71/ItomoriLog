namespace ItomoriLog.Core.Ingest;

public interface ITimestampExtractor
{
    bool TryExtract(RawRecord raw, out DateTimeOffset timestamp);
    string Description { get; }
}

public interface ITimestampExtractorWithMetadata
{
    bool TryExtractWithMetadata(RawRecord raw, out TimestampExtraction extraction);
}

public sealed record TimestampExtraction(
    DateTimeOffset? ExplicitTimestamp,
    DateTime? BareTimestamp,
    string ParsedText,
    string? AlternateText,
    bool UsedTwoDigitYear)
{
    public bool HasExplicitOffset => ExplicitTimestamp.HasValue;

    public static TimestampExtraction FromExplicit(
        DateTimeOffset timestamp,
        string parsedText,
        string? alternateText = null,
        bool usedTwoDigitYear = false) =>
        new(timestamp, null, parsedText, alternateText, usedTwoDigitYear);

    public static TimestampExtraction FromBare(
        DateTime timestamp,
        string parsedText,
        string? alternateText = null,
        bool usedTwoDigitYear = false) =>
        new(null, timestamp, parsedText, alternateText, usedTwoDigitYear);
}
