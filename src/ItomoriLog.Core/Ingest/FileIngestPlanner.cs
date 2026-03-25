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

        foreach (var expandedPath in ExpandInputPaths(filePaths, skippedFiles)) {
            ct.ThrowIfCancellationRequested();

            if (SourcePathHelper.IsArchiveFilePath(expandedPath)) {
                filesToIngest.Add(SourcePathHelper.Normalize(expandedPath));
                continue;
            }

            var path = SourcePathHelper.Normalize(expandedPath);

            long sourceSizeBytes;
            DateTimeOffset lastModifiedUtc;
            if (SourcePathHelper.TrySplitArchiveEntry(path, out var archivePath, out var entryFullName)) {
                if (!File.Exists(archivePath)) {
                    skippedFiles.Add(new PlannedFileSkip(path, "Archive file does not exist"));
                    continue;
                }

                if (!ZipHandler.TryGetEntry(archivePath, entryFullName, out var zipEntry)) {
                    skippedFiles.Add(new PlannedFileSkip(path, "Archive entry does not exist"));
                    continue;
                }

                sourceSizeBytes = zipEntry.SizeBytes;
                lastModifiedUtc = new DateTimeOffset(File.GetLastWriteTimeUtc(archivePath), TimeSpan.Zero);
            } else {
                if (!File.Exists(path)) {
                    skippedFiles.Add(new PlannedFileSkip(path, "File does not exist"));
                    continue;
                }

                var fileInfo = new FileInfo(path);
                sourceSizeBytes = fileInfo.Length;
                lastModifiedUtc = new DateTimeOffset(fileInfo.LastWriteTimeUtc, TimeSpan.Zero);
            }

            var physicalFileId = IdentityGenerator.PhysicalFileId(path, sourceSizeBytes, lastModifiedUtc);

            var exactMatch = await FindExactPhysicalMatchAsync(physicalFileId, ct);
            if (exactMatch is not null) {
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
            if (sourcePathMatch is not null) {
                var existingLastModifiedUtc = sourcePathMatch.Value.LastModifiedUtc;
                var modified = existingLastModifiedUtc is null || existingLastModifiedUtc.Value != lastModifiedUtc;
                if (modified) {
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

    private static IEnumerable<string> ExpandInputPaths(IReadOnlyList<string> inputPaths, List<PlannedFileSkip> skippedFiles)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var rawPath in inputPaths) {
            if (string.IsNullOrWhiteSpace(rawPath))
                continue;

            if (SourcePathHelper.TrySplitArchiveEntry(rawPath, out var archivePath, out var entryFullName)) {
                var entryPath = SourcePathHelper.CombineArchiveEntryPath(archivePath, entryFullName);
                if (seen.Add(entryPath))
                    yield return entryPath;
                continue;
            }

            var fullPath = Path.GetFullPath(rawPath);
            if (File.Exists(fullPath)) {
                if (seen.Add(fullPath))
                    yield return fullPath;
                continue;
            }

            if (Directory.Exists(fullPath)) {
                IEnumerable<string> files;
                try {
                    files = Directory.EnumerateFiles(fullPath, "*", SearchOption.AllDirectories);
                } catch (Exception ex) {
                    skippedFiles.Add(new PlannedFileSkip(fullPath, $"Directory expansion failed: {ex.Message}"));
                    continue;
                }

                foreach (var file in files) {
                    var expanded = Path.GetFullPath(file);
                    if (seen.Add(expanded))
                        yield return expanded;
                }
                continue;
            }

            if (seen.Add(fullPath))
                yield return fullPath;
        }
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
        switch (action) {
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
