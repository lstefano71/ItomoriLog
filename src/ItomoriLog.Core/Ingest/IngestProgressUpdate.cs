namespace ItomoriLog.Core.Ingest;

public enum IngestFilePhase
{
    Queued,
    Sniffing,
    Ingesting,
    Completed,
    Skipped,
    Failed
}

public sealed record IngestProgressUpdate(
    string SourcePath,
    IngestFilePhase Phase,
    long BytesProcessed,
    long BytesTotal,
    long RecordsProcessed,
    string? Message = null);

public sealed record IngestVisibilityUpdate(
    string SourcePath,
    int RowsCommitted,
    long TotalRowsCommitted);
