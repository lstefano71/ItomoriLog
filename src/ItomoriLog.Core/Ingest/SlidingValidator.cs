namespace ItomoriLog.Core.Ingest;

public sealed class SlidingValidator
{
    private readonly int _windowSize;
    private readonly double _threshold;
    private readonly Queue<bool> _matchResults;
    private readonly Queue<bool> _extractResults;

    public SlidingValidator(int windowSize = 256, double threshold = 0.80)
    {
        _windowSize = windowSize;
        _threshold = threshold;
        _matchResults = new Queue<bool>(windowSize);
        _extractResults = new Queue<bool>(windowSize);
    }

    public void RecordResult(bool sorMatched, bool timestampExtracted)
    {
        if (_matchResults.Count >= _windowSize) _matchResults.Dequeue();
        if (_extractResults.Count >= _windowSize) _extractResults.Dequeue();
        _matchResults.Enqueue(sorMatched);
        _extractResults.Enqueue(timestampExtracted);
    }

    public bool ShouldRedetect {
        get {
            if (_matchResults.Count < _windowSize) return false;
            var matchRate = (double)_matchResults.Count(x => x) / _matchResults.Count;
            var extractRate = (double)_extractResults.Count(x => x) / _extractResults.Count;
            return matchRate < _threshold || extractRate < _threshold;
        }
    }

    public double MatchRate => _matchResults.Count == 0 ? 1.0
        : (double)_matchResults.Count(x => x) / _matchResults.Count;

    public double ExtractRate => _extractResults.Count == 0 ? 1.0
        : (double)_extractResults.Count(x => x) / _extractResults.Count;

    public void Reset()
    {
        _matchResults.Clear();
        _extractResults.Clear();
    }
}
