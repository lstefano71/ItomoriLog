using System.Text.RegularExpressions;
using ItomoriLog.Core.Ingest.Extractors;

namespace ItomoriLog.Core.Ingest.Detectors;

public sealed class TextFormatDetector : IFormatDetector
{
    private const int MinSniffLines = 20;
    private const int MaxSniffLines = 512;
    private const double MinConfidence = 0.50;

    public DetectionResult? Probe(Stream sample, string sourceName)
    {
        sample.Position = 0;
        using var reader = new StreamReader(sample, leaveOpen: true);

        var lines = new List<string>();
        string? line;
        while ((line = reader.ReadLine()) is not null && lines.Count < MaxSniffLines)
        {
            if (!string.IsNullOrWhiteSpace(line))
                lines.Add(line);
        }

        if (lines.Count < MinSniffLines)
            return null;

        DetectionResult? best = null;

        foreach (var (name, pattern) in SoRPatterns.All)
        {
            var (parseRate, monotonic) = ScorePattern(pattern, lines);

            if (parseRate < MinConfidence)
                continue;

            var confidence = parseRate * 0.7 + (monotonic ? 0.3 : 0.0);

            if (best is null || confidence > best.Confidence)
            {
                var extractor = new RegexGroupTsExtractor(pattern);
                best = new DetectionResult(
                    Confidence: confidence,
                    Boundary: new TextSoRBoundary(pattern, Anchored: true, PatternName: name),
                    Extractor: extractor,
                    Notes: $"Pattern: {name}, ParseRate: {parseRate:P0}");
            }
        }

        return best;
    }

    private static (double parseRate, bool monotonic) ScorePattern(Regex pattern, List<string> lines)
    {
        int matches = 0;
        var timestamps = new List<DateTimeOffset>();
        var extractor = new RegexGroupTsExtractor(pattern);

        foreach (var line in lines)
        {
            if (!pattern.IsMatch(line))
                continue;

            matches++;
            var raw = new RawRecord(line, line, 0, 0);
            if (extractor.TryExtract(raw, out var ts))
                timestamps.Add(ts);
        }

        if (matches == 0)
            return (0, false);

        var parseRate = (double)matches / lines.Count;

        // Check monotonicity (allow small out-of-order)
        bool monotonic = true;
        for (int i = 1; i < timestamps.Count; i++)
        {
            if (timestamps[i] < timestamps[i - 1].AddSeconds(-2))
            {
                monotonic = false;
                break;
            }
        }

        return (parseRate, monotonic);
    }
}
