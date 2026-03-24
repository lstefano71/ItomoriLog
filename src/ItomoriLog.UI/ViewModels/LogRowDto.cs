namespace ItomoriLog.UI.ViewModels;

/// <summary>
/// Lightweight DTO projected from LogRow for grid binding. Avoids heavy object bindings.
/// </summary>
public sealed record LogRowDto(
    string Timestamp,
    string? Level,
    string Source,
    string Message,
    string? FieldsJson,
    DateTimeOffset TimestampUtc,
    string SegmentId,
    long RecordIndex,
    string? TimestampOriginal,
    string SourcePath,
    string TimestampBasis,
    int OffsetMinutes);
