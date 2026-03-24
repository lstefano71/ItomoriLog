using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Ingest;

public sealed class SkipLogger
{
    private readonly ISkipSink _sink;
    private readonly string _logicalSourceId;
    private readonly string _physicalFileId;
    private readonly string _segmentId;
    private readonly string? _detectorProfileId;
    private long _segmentSeq;

    public SkipLogger(ISkipSink sink, string logicalSourceId, string physicalFileId, string segmentId, string? detectorProfileId = null)
    {
        _sink = sink;
        _logicalSourceId = logicalSourceId;
        _physicalFileId = physicalFileId;
        _segmentId = segmentId;
        _detectorProfileId = detectorProfileId;
    }

    public SkipSegment BeginSkip(SkipReasonCode reason, string? detail, long? startLine = null, long? startOffset = null, byte[]? samplePrefix = null)
    {
        var seq = Interlocked.Increment(ref _segmentSeq);
        return new SkipSegment(this, seq, reason, detail, startLine, startOffset, samplePrefix);
    }

    private void Emit(SkipRow row) => _sink.Write(row);

    public struct SkipSegment : IDisposable
    {
        private readonly SkipLogger _logger;
        private readonly long _seq;
        private readonly SkipReasonCode _reason;
        private readonly string? _detail;
        private readonly long? _startLine;
        private readonly long? _startOffset;
        private readonly byte[]? _samplePrefix;
        private bool _closed;

        internal SkipSegment(SkipLogger logger, long seq, SkipReasonCode reason, string? detail, long? startLine, long? startOffset, byte[]? samplePrefix)
        {
            _logger = logger;
            _seq = seq;
            _reason = reason;
            _detail = detail;
            _startLine = startLine;
            _startOffset = startOffset;
            _samplePrefix = samplePrefix;
            _closed = false;
        }

        public void Close(long? endLine = null, long? endOffset = null)
        {
            if (_closed) return;
            _closed = true;
            _logger.Emit(new SkipRow(
                LogicalSourceId: _logger._logicalSourceId,
                PhysicalFileId: _logger._physicalFileId,
                SegmentId: _logger._segmentId,
                SegmentSeq: _seq,
                StartLine: _startLine,
                EndLine: endLine,
                StartOffset: _startOffset,
                EndOffset: endOffset,
                ReasonCode: _reason,
                ReasonDetail: _detail,
                SamplePrefix: _samplePrefix,
                DetectorProfileId: _logger._detectorProfileId,
                UtcLoggedAt: DateTimeOffset.UtcNow));
        }

        public void Dispose() => Close();
    }
}

/// <summary>
/// In-memory skip sink for testing and buffering before DB flush.
/// </summary>
public sealed class ListSkipSink : ISkipSink
{
    private readonly List<SkipRow> _skips = [];
    private readonly object _lock = new();

    public void Write(SkipRow skip)
    {
        lock (_lock) { _skips.Add(skip); }
    }

    public IReadOnlyList<SkipRow> GetSkips()
    {
        lock (_lock) { return [.. _skips]; }
    }
}
