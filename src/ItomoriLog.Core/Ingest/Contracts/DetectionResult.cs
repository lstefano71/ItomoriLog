using System.Text.RegularExpressions;

namespace ItomoriLog.Core.Ingest;

public sealed record DetectionResult(
    double Confidence,
    RecordBoundarySpec Boundary,
    ITimestampExtractor Extractor,
    string? Notes = null);

public abstract record RecordBoundarySpec;
public sealed record CsvBoundary(
    char Delimiter,
    bool HasHeader,
    string[]? ColumnNames = null,
    char Quote = '"') : RecordBoundarySpec;
public sealed record JsonNdBoundary(string? TimestampFieldPath = null) : RecordBoundarySpec;
public sealed record TextSoRBoundary(Regex StartRegex, bool Anchored = true, string? PatternName = null) : RecordBoundarySpec;
