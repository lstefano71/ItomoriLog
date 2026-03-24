namespace ItomoriLog.Core.Model;

public sealed record LogRow(
    DateTimeOffset TimestampUtc,
    TimeBasis TimestampBasis,
    int TimestampEffectiveOffsetMinutes,
    string? TimestampOriginal,
    string LogicalSourceId,
    string SourcePath,
    string PhysicalFileId,
    string SegmentId,
    string IngestRunId,
    long RecordIndex,
    string? Level,
    string Message,
    string? FieldsJson);
