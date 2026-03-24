namespace ItomoriLog.Core.Model;

public sealed record SkipRow(
    string LogicalSourceId,
    string PhysicalFileId,
    string SegmentId,
    long SegmentSeq,
    long? StartLine,
    long? EndLine,
    long? StartOffset,
    long? EndOffset,
    SkipReasonCode ReasonCode,
    string? ReasonDetail,
    byte[]? SamplePrefix,
    string? DetectorProfileId,
    DateTimeOffset UtcLoggedAt);
