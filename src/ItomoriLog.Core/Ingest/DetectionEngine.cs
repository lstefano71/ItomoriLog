namespace ItomoriLog.Core.Ingest;

public sealed class DetectionEngine
{
    private readonly IReadOnlyList<IFormatDetector> _detectors;

    public DetectionEngine() : this([
        new Detectors.NdjsonFormatDetector(),
        new Detectors.CsvFormatDetector(),
        new Detectors.TextFormatDetector(),
    ]) { }

    public DetectionEngine(IReadOnlyList<IFormatDetector> detectors) => _detectors = detectors;

    public EngineResult Detect(Stream sniffBuffer, string sourceName)
    {
        var results = DetectCandidatesInternal(sniffBuffer, sourceName);

        if (results.Count == 0)
            return new EngineResult(null, false);

        var best = results[0];
        bool needsDisambiguation = results.Count >= 2
            && Math.Abs(results[0].result.Confidence - results[1].result.Confidence) < 0.02;

        return new EngineResult(best.result, needsDisambiguation);
    }

    public IReadOnlyList<DetectionResult> DetectCandidates(Stream sniffBuffer, string sourceName)
    {
        var results = DetectCandidatesInternal(sniffBuffer, sourceName);
        return results.Select(r => r.result).ToArray();
    }

    private List<(DetectionResult result, int priority)> DetectCandidatesInternal(Stream sniffBuffer, string sourceName)
    {
        var results = new List<(DetectionResult result, int priority)>();

        for (int i = 0; i < _detectors.Count; i++)
        {
            sniffBuffer.Position = 0;
            var result = _detectors[i].Probe(sniffBuffer, sourceName);
            if (result is not null)
                results.Add((result, i));
        }

        // Sort by confidence desc, then priority asc (lower index = higher priority)
        results.Sort((a, b) =>
        {
            var cmp = b.result.Confidence.CompareTo(a.result.Confidence);
            return cmp != 0 ? cmp : a.priority.CompareTo(b.priority);
        });

        return results;
    }
}

public sealed record EngineResult(DetectionResult? Detection, bool NeedsDisambiguation);
