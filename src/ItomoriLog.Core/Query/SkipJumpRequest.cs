namespace ItomoriLog.Core.Query;

public sealed record SkipJumpRequest(
    long? StartOffset,
    long? EndOffset,
    string SourcePath);
