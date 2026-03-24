namespace ItomoriLog.Core.Model;

public sealed record SegmentRow(
    string SegmentId,
    string LogicalSourceId,
    string PhysicalFileId,
    DateTimeOffset? MinTsUtc,
    DateTimeOffset? MaxTsUtc,
    long RowCount,
    string LastIngestRunId,
    bool Active,
    string? SourcePath,
    long? FileSizeBytes,
    DateTimeOffset? LastModifiedUtc,
    string? FileHash,
    long? LastByteOffset);
