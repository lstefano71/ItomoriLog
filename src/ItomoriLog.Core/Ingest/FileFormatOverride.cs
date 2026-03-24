namespace ItomoriLog.Core.Ingest;

public sealed record FileFormatOverride(
    string SourcePath,
    DetectionResult Detection);
