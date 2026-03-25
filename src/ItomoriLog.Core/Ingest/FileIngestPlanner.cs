using DuckDB.NET.Data;

using System.Text;

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

        // Phase 1: file-system I/O only — no DB calls.
        var archivePaths = new List<string>();
        var fileMetas = new List<FileMetaEntry>();

        foreach (var expandedPath in ExpandInputPaths(filePaths, skippedFiles)) {
            ct.ThrowIfCancellationRequested();

            if (SourcePathHelper.IsArchiveFilePath(expandedPath)) {
                archivePaths.Add(SourcePathHelper.Normalize(expandedPath));
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
            fileMetas.Add(new FileMetaEntry(path, physicalFileId, lastModifiedUtc));
        }

        // Archive paths always go to ingest directly.
        foreach (var ap in archivePaths)
            filesToIngest.Add(ap);

        if (fileMetas.Count == 0)
            return new FileIngestPlan(filesToIngest.ToArray(), segmentsToReingest.ToArray(), skippedFiles);

        // Phase 2: single batch DB query for all files.
        var physicalIds = fileMetas.ConvertAll(static f => f.PhysicalFileId);
        var sourcePaths = fileMetas.ConvertAll(static f => f.Path);
        var (byPhysId, bySrcPath) = await FetchMatchingSegmentsBatchAsync(physicalIds, sourcePaths, ct);

        // Phase 3: apply skip/reingest decisions in memory.
        foreach (var meta in fileMetas) {
            ct.ThrowIfCancellationRequested();

            if (byPhysId.TryGetValue(meta.PhysicalFileId, out var exactMatch)) {
                ApplyExistingFileAction(
                    existingFileAction,
                    meta.Path,
                    exactMatch.SegmentId,
                    "File already ingested with same fingerprint",
                    filesToIngest,
                    segmentsToReingest,
                    skippedFiles);
                continue;
            }

            if (bySrcPath.TryGetValue(meta.Path, out var sourceMatch)) {
                var modified = sourceMatch.LastModifiedUtc is null
                    || sourceMatch.LastModifiedUtc.Value != meta.LastModifiedUtc;
                if (modified) {
                    ApplyExistingFileAction(
                        existingFileAction,
                        meta.Path,
                        sourceMatch.SegmentId,
                        "File path exists but contents changed since previous ingest",
                        filesToIngest,
                        segmentsToReingest,
                        skippedFiles);
                    continue;
                }
            }

            filesToIngest.Add(meta.Path);
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

    private async Task<(
        Dictionary<string, (string SegmentId, DateTimeOffset? LastModifiedUtc)> ByPhysicalId,
        Dictionary<string, (string SegmentId, DateTimeOffset? LastModifiedUtc)> BySourcePath)>
        FetchMatchingSegmentsBatchAsync(
            IReadOnlyList<string> physicalFileIds,
            IReadOnlyList<string> sourcePaths,
            CancellationToken ct)
    {
        var byPhysicalId = new Dictionary<string, (string, DateTimeOffset?)>(StringComparer.OrdinalIgnoreCase);
        var bySourcePath = new Dictionary<string, (string, DateTimeOffset?)>(StringComparer.OrdinalIgnoreCase);

        var sb = new StringBuilder(256);
        sb.Append("SELECT segment_id, physical_file_id, source_path, last_modified_utc FROM segments WHERE active = TRUE AND (");

        using var cmd = _connection.CreateCommand();
        var paramIdx = 1;

        if (physicalFileIds.Count > 0) {
            sb.Append("physical_file_id IN (");
            for (var i = 0; i < physicalFileIds.Count; i++) {
                if (i > 0) sb.Append(", ");
                sb.Append('$').Append(paramIdx++);
                cmd.Parameters.Add(new DuckDBParameter { Value = physicalFileIds[i] });
            }
            sb.Append(')');
        }

        if (sourcePaths.Count > 0) {
            if (physicalFileIds.Count > 0) sb.Append(" OR ");
            sb.Append("source_path IN (");
            for (var i = 0; i < sourcePaths.Count; i++) {
                if (i > 0) sb.Append(", ");
                sb.Append('$').Append(paramIdx++);
                cmd.Parameters.Add(new DuckDBParameter { Value = sourcePaths[i] });
            }
            sb.Append(')');
        }

        // ORDER BY DESC so that TryAdd retains the most recent row per key (matches original LIMIT 1 behaviour).
        sb.Append(") ORDER BY last_modified_utc DESC NULLS LAST");
        cmd.CommandText = sb.ToString();

        using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) {
            var segmentId = reader.GetString(0);
            var physId = reader.GetString(1);
            var srcPath = reader.GetString(2);
            var lastMod = reader.IsDBNull(3)
                ? null
                : (DateTimeOffset?)new DateTimeOffset(reader.GetDateTime(3), TimeSpan.Zero);

            byPhysicalId.TryAdd(physId, (segmentId, lastMod));
            bySourcePath.TryAdd(srcPath, (segmentId, lastMod));
        }

        return (byPhysicalId, bySourcePath);
    }

    private readonly record struct FileMetaEntry(
        string Path,
        string PhysicalFileId,
        DateTimeOffset LastModifiedUtc);
}

