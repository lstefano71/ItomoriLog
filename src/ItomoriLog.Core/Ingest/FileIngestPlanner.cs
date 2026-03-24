using DuckDB.NET.Data;

namespace ItomoriLog.Core.Ingest;

public enum ExistingFileAction
{
    Skip,
    Reingest,
    ForceAdd
}

public sealed record PlannedFileSkip(
    string SourcePath,
    string Reason);

public sealed record FileIngestPlan(
    IReadOnlyList<string> FilesToIngest,
    IReadOnlyList<string> SegmentsToReingest,
    IReadOnlyList<PlannedFileSkip> SkippedFiles);

public sealed class FileIngestPlanner
{
    private readonly DuckDBConnection _connection;

    public FileIngestPlanner(DuckDBConnection connection)
    {
        _connection = connection;
    }

    public async Task<FileIngestPlan> PlanAsync(
        IReadOnlyList<string> filePaths,
        ExistingFileAction existingFileAction = ExistingFileAction.Skip,
        CancellationToken ct = default)
    {
        var filesToIngest = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var segmentsToReingest = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var skippedFiles = new List<PlannedFileSkip>();

        foreach (var rawPath in filePaths)
        {
            ct.ThrowIfCancellationRequested();

            if (rawPath.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
            {
                filesToIngest.Add(Path.GetFullPath(rawPath));
                continue;
            }

            var path = Path.GetFullPath(rawPath);
            if (!File.Exists(path))
            {
                skippedFiles.Add(new PlannedFileSkip(path, "File does not exist"));
                continue;
            }

            var fileInfo = new FileInfo(path);
            var lastModifiedUtc = new DateTimeOffset(fileInfo.LastWriteTimeUtc, TimeSpan.Zero);
            var physicalFileId = IdentityGenerator.PhysicalFileId(path, fileInfo.Length, lastModifiedUtc);

            var exactMatch = await FindExactPhysicalMatchAsync(physicalFileId, ct);
            if (exactMatch is not null)
            {
                ApplyExistingFileAction(
                    existingFileAction,
                    path,
                    exactMatch.Value.SegmentId,
                    "File already ingested with same fingerprint",
                    filesToIngest,
                    segmentsToReingest,
                    skippedFiles);
                continue;
            }

            var sourcePathMatch = await FindSourcePathMatchAsync(path, ct);
            if (sourcePathMatch is not null)
            {
                var existingLastModifiedUtc = sourcePathMatch.Value.LastModifiedUtc;
                var modified = existingLastModifiedUtc is null || existingLastModifiedUtc.Value != lastModifiedUtc;
                if (modified)
                {
                    ApplyExistingFileAction(
                        existingFileAction,
                        path,
                        sourcePathMatch.Value.SegmentId,
                        "File path exists but contents changed since previous ingest",
                        filesToIngest,
                        segmentsToReingest,
                        skippedFiles);
                    continue;
                }
            }

            filesToIngest.Add(path);
        }

        return new FileIngestPlan(
            FilesToIngest: filesToIngest.ToArray(),
            SegmentsToReingest: segmentsToReingest.ToArray(),
            SkippedFiles: skippedFiles);
    }

    private static void ApplyExistingFileAction(
        ExistingFileAction action,
        string path,
        string segmentId,
        string reason,
        HashSet<string> filesToIngest,
        HashSet<string> segmentsToReingest,
        List<PlannedFileSkip> skippedFiles)
    {
        switch (action)
        {
            case ExistingFileAction.Skip:
                skippedFiles.Add(new PlannedFileSkip(path, $"{reason}; action=Skip"));
                break;
            case ExistingFileAction.Reingest:
                segmentsToReingest.Add(segmentId);
                break;
            case ExistingFileAction.ForceAdd:
                filesToIngest.Add(path);
                break;
            default:
                skippedFiles.Add(new PlannedFileSkip(path, $"{reason}; action=Skip"));
                break;
        }
    }

    private async Task<(string SegmentId, DateTimeOffset? LastModifiedUtc)?> FindExactPhysicalMatchAsync(
        string physicalFileId,
        CancellationToken ct)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            SELECT segment_id, last_modified_utc
            FROM segments
            WHERE physical_file_id = $1 AND active = TRUE
            ORDER BY last_modified_utc DESC NULLS LAST
            LIMIT 1
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = physicalFileId });

        using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        return (
            SegmentId: reader.GetString(0),
            LastModifiedUtc: reader.IsDBNull(1) ? null : new DateTimeOffset(reader.GetDateTime(1), TimeSpan.Zero));
    }

    private async Task<(string SegmentId, DateTimeOffset? LastModifiedUtc)?> FindSourcePathMatchAsync(
        string sourcePath,
        CancellationToken ct)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            SELECT segment_id, last_modified_utc
            FROM segments
            WHERE source_path = $1 AND active = TRUE
            ORDER BY last_modified_utc DESC NULLS LAST
            LIMIT 1
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = sourcePath });

        using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        return (
            SegmentId: reader.GetString(0),
            LastModifiedUtc: reader.IsDBNull(1) ? null : new DateTimeOffset(reader.GetDateTime(1), TimeSpan.Zero));
    }
}
