using System.Globalization;
using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Ingest;

public sealed record ResolvedTimestamp(
    DateTimeOffset UtcTimestamp,
    TimeBasis Basis,
    int EffectiveOffsetMinutes,
    string? TimestampOriginal,
    bool UsedTwoDigitYear);

public static class TimestampResolver
{
    private static readonly string[] BareFormats =
    [
        "yyyy-MM-ddTHH:mm:ss.fffffff",
        "yyyy-MM-ddTHH:mm:ss.fff",
        "yyyy-MM-ddTHH:mm:ss",
        "yyyy-MM-dd HH:mm:ss.fffffff",
        "yyyy-MM-dd HH:mm:ss.fff",
        "yyyy-MM-dd HH:mm:ss,fff",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy/MM/dd HH:mm:ss.fff",
        "yyyy/MM/dd HH:mm:ss",
        "MM/dd/yyyy HH:mm:ss.fff",
        "MM/dd/yyyy HH:mm:ss",
        "dd/MM/yyyy HH:mm:ss.fff",
        "dd/MM/yyyy HH:mm:ss",
        "yyyy-MM-dd",
        "yyyy/MM/dd",
        "MM/dd/yyyy",
        "dd/MM/yyyy"
    ];

    public static bool TryResolve(
        ITimestampExtractor extractor,
        RawRecord raw,
        TimeBasisConfig config,
        out ResolvedTimestamp resolved)
    {
        TimestampExtraction primaryExtraction;
        if (extractor is ITimestampExtractorWithMetadata withMetadata &&
            withMetadata.TryExtractWithMetadata(raw, out primaryExtraction))
        {
            if (TryResolveFromExtraction(primaryExtraction, config, out resolved))
                return true;
        }
        else if (extractor.TryExtract(raw, out var timestamp))
        {
            var basis = timestamp.Offset == TimeSpan.Zero ? TimeBasis.Utc : TimeBasis.FixedOffset;
            resolved = new ResolvedTimestamp(
                UtcTimestamp: timestamp.ToUniversalTime(),
                Basis: basis,
                EffectiveOffsetMinutes: (int)timestamp.Offset.TotalMinutes,
                TimestampOriginal: raw.FirstLine[..Math.Min(raw.FirstLine.Length, 64)],
                UsedTwoDigitYear: false);
            return true;
        }

        resolved = default!;
        return false;
    }

    private static bool TryResolveFromExtraction(
        TimestampExtraction extraction,
        TimeBasisConfig config,
        out ResolvedTimestamp resolved)
    {
        var primary = ToCandidate(extraction, extraction.ParsedText);
        if (primary is null)
        {
            resolved = default!;
            return false;
        }

        Candidate? alternate = null;
        if (!string.IsNullOrWhiteSpace(extraction.AlternateText) &&
            TryParseCandidate(extraction.AlternateText!, out var parsedAlternate))
        {
            alternate = parsedAlternate with { RawText = extraction.AlternateText! };
        }

        var chosen = ChoosePreferred(primary.Value, alternate, out var otherText);
        var usedTwoDigitYear = chosen.UsedTwoDigitYear || extraction.UsedTwoDigitYear;

        if (chosen.ExplicitTimestamp.HasValue)
        {
            var explicitTs = chosen.ExplicitTimestamp.Value;
            resolved = new ResolvedTimestamp(
                UtcTimestamp: explicitTs.ToUniversalTime(),
                Basis: explicitTs.Offset == TimeSpan.Zero ? TimeBasis.Utc : TimeBasis.FixedOffset,
                EffectiveOffsetMinutes: (int)explicitTs.Offset.TotalMinutes,
                TimestampOriginal: otherText ?? extraction.ParsedText,
                UsedTwoDigitYear: usedTwoDigitYear);
            return true;
        }

        if (!chosen.BareTimestamp.HasValue)
        {
            resolved = default!;
            return false;
        }

        var bare = chosen.BareTimestamp.Value;
        var (utc, basis, effectiveOffsetMinutes) = ResolveBareTimestamp(bare, config);
        resolved = new ResolvedTimestamp(
            UtcTimestamp: utc,
            Basis: basis,
            EffectiveOffsetMinutes: effectiveOffsetMinutes,
            TimestampOriginal: otherText ?? extraction.ParsedText,
            UsedTwoDigitYear: usedTwoDigitYear);
        return true;
    }

    private static (DateTimeOffset UtcTimestamp, TimeBasis Basis, int EffectiveOffsetMinutes)
        ResolveBareTimestamp(DateTime bare, TimeBasisConfig config)
    {
        return config.Basis switch
        {
            TimeBasis.Utc => (
                new DateTimeOffset(DateTime.SpecifyKind(bare, DateTimeKind.Utc)),
                TimeBasis.Utc,
                0),

            TimeBasis.FixedOffset when config.OffsetMinutes.HasValue => ResolveWithOffset(
                bare,
                TimeSpan.FromMinutes(config.OffsetMinutes.Value),
                TimeBasis.FixedOffset),

            TimeBasis.Zone when config.TimeZoneId is not null => ResolveWithZone(
                bare,
                config.TimeZoneId),

            TimeBasis.Local => ResolveWithOffset(
                bare,
                TimeZoneInfo.Local.GetUtcOffset(bare),
                TimeBasis.Local),

            _ => ResolveWithOffset(
                bare,
                TimeZoneInfo.Local.GetUtcOffset(bare),
                TimeBasis.Local)
        };
    }

    private static (DateTimeOffset UtcTimestamp, TimeBasis Basis, int EffectiveOffsetMinutes)
        ResolveWithOffset(DateTime bare, TimeSpan offset, TimeBasis basis)
    {
        var dto = new DateTimeOffset(bare, offset);
        return (dto.ToUniversalTime(), basis, (int)offset.TotalMinutes);
    }

    private static (DateTimeOffset UtcTimestamp, TimeBasis Basis, int EffectiveOffsetMinutes)
        ResolveWithZone(DateTime bare, string zoneId)
    {
        var tz = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
        var adjusted = bare;

        // Spring-forward gap: move to first valid local instant.
        if (tz.IsInvalidTime(adjusted))
            adjusted = adjusted.AddHours(1);

        TimeSpan offset;
        if (tz.IsAmbiguousTime(adjusted))
        {
            // Use post-transition offset (typically the smaller offset in absolute timeline ordering).
            var offsets = tz.GetAmbiguousTimeOffsets(adjusted);
            offset = offsets.Min();
        }
        else
        {
            offset = tz.GetUtcOffset(adjusted);
        }

        var dto = new DateTimeOffset(adjusted, offset);
        return (dto.ToUniversalTime(), TimeBasis.Zone, (int)offset.TotalMinutes);
    }

    private static Candidate? ToCandidate(TimestampExtraction extraction, string rawText)
    {
        if (extraction.ExplicitTimestamp.HasValue)
        {
            return new Candidate(
                ExplicitTimestamp: extraction.ExplicitTimestamp,
                BareTimestamp: null,
                RawText: rawText,
                UsedTwoDigitYear: extraction.UsedTwoDigitYear);
        }

        if (extraction.BareTimestamp.HasValue)
        {
            return new Candidate(
                ExplicitTimestamp: null,
                BareTimestamp: extraction.BareTimestamp,
                RawText: rawText,
                UsedTwoDigitYear: extraction.UsedTwoDigitYear);
        }

        return null;
    }

    private static Candidate ChoosePreferred(Candidate primary, Candidate? alternate, out string? otherText)
    {
        otherText = null;
        if (alternate is null)
            return primary;

        var alt = alternate.Value;

        // Prefer explicit timestamps over bare timestamps.
        if (primary.ExplicitTimestamp.HasValue && !alt.ExplicitTimestamp.HasValue)
        {
            otherText = alt.RawText;
            return primary;
        }

        if (!primary.ExplicitTimestamp.HasValue && alt.ExplicitTimestamp.HasValue)
        {
            otherText = primary.RawText;
            return alt;
        }

        // If both explicit, prefer the more precise text representation.
        if (primary.ExplicitTimestamp.HasValue && alt.ExplicitTimestamp.HasValue)
        {
            var primaryScore = PrecisionScore(primary.RawText);
            var altScore = PrecisionScore(alt.RawText);
            if (altScore > primaryScore)
            {
                otherText = primary.RawText;
                return alt;
            }

            otherText = alt.RawText;
            return primary;
        }

        // Both bare; keep primary.
        otherText = alt.RawText;
        return primary;
    }

    private static int PrecisionScore(string text)
    {
        var score = 0;
        var fractionalMatch = System.Text.RegularExpressions.Regex.Match(text, @"[\.,](\d+)");
        if (fractionalMatch.Success)
            score += fractionalMatch.Groups[1].Value.Length;

        if (text.Contains('Z', StringComparison.OrdinalIgnoreCase) ||
            System.Text.RegularExpressions.Regex.IsMatch(text, @"[+-]\d{2}:?\d{2}$"))
        {
            score += 100;
        }

        return score;
    }

    private static bool TryParseCandidate(string text, out Candidate candidate)
    {
        var trimmed = text.Trim();

        if (DateTimeOffset.TryParse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var explicitTs))
        {
            candidate = new Candidate(explicitTs, null, trimmed, false);
            return true;
        }

        if (DateTime.TryParseExact(
            trimmed,
            BareFormats,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.NoCurrentDateDefault,
            out var bare))
        {
            candidate = new Candidate(null, bare, trimmed, false);
            return true;
        }

        if (TimestampParsing.TryParseWithTwoDigitYearWindow(trimmed, out var yyBare, out var usedWindow))
        {
            candidate = new Candidate(null, yyBare, trimmed, usedWindow);
            return true;
        }

        if (long.TryParse(trimmed, NumberStyles.None, CultureInfo.InvariantCulture, out var epoch))
        {
            var epochTs = trimmed.Length >= 13
                ? DateTimeOffset.FromUnixTimeMilliseconds(epoch)
                : DateTimeOffset.FromUnixTimeSeconds(epoch);
            candidate = new Candidate(epochTs, null, trimmed, false);
            return true;
        }

        candidate = default;
        return false;
    }

    private readonly record struct Candidate(
        DateTimeOffset? ExplicitTimestamp,
        DateTime? BareTimestamp,
        string RawText,
        bool UsedTwoDigitYear);
}
