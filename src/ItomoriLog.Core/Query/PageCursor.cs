namespace ItomoriLog.Core.Query;

/// <summary>
/// Keyset cursor for pagination on (timestamp_utc, segment_id, record_index).
/// </summary>
public sealed record PageCursor(
    DateTimeOffset TimestampUtc,
    string SegmentId,
    long RecordIndex);
