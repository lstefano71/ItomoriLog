using DuckDB.NET.Data;

using System.Security.Cryptography;

namespace ItomoriLog.Core.Ingest;

/// <summary>
/// Compares on-disk file state against recorded segment metadata to detect changes.
/// </summary>
public sealed class FileChangeDetector
{
    private readonly DuckDBConnection _connection;

    /// <summary>Files smaller than this threshold will be hashed for comparison.</summary>
    public const long HashThresholdBytes = 50 * 1024 * 1024; // 50 MB

    public FileChangeDetector(DuckDBConnection connection)
    {
        _connection = connection;
    }

    /// <summary>
    /// Checks whether the file backing a segment has changed on disk.
    /// </summary>
    public async Task<FileChangeResult> DetectAsync(string segmentId, CancellationToken ct = default)
    {
        var meta = await LoadSegmentFileMetaAsync(segmentId, ct);
        if (meta is null)
            return new FileChangeResult(segmentId, FileChangeStatus.New, "Segment has no file metadata recorded");

        if (meta.SourcePath is null)
            return new FileChangeResult(segmentId, FileChangeStatus.New, "No source path stored for segment");

        var canonicalSourcePath = SourcePathHelper.Normalize(meta.SourcePath);
        if (SourcePathHelper.TrySplitArchiveEntry(canonicalSourcePath, out var archivePath, out var entryFullName)) {
            if (!File.Exists(archivePath))
                return new FileChangeResult(segmentId, FileChangeStatus.Deleted, $"Archive not found: {archivePath}");

            if (!ZipHandler.TryGetEntry(archivePath, entryFullName, out var zipEntry))
                return new FileChangeResult(segmentId, FileChangeStatus.Deleted, $"Archive entry not found: {entryFullName}");

            if (meta.FileSizeBytes.HasValue && zipEntry.SizeBytes != meta.FileSizeBytes.Value)
                return new FileChangeResult(segmentId, FileChangeStatus.Modified,
                    $"Size changed: {meta.FileSizeBytes.Value} → {zipEntry.SizeBytes}");

            var archiveInfo = new FileInfo(archivePath);
            if (meta.LastModifiedUtc.HasValue && archiveInfo.LastWriteTimeUtc != meta.LastModifiedUtc.Value) {
                if (meta.FileHash is not null && zipEntry.SizeBytes <= HashThresholdBytes) {
                    await using var currentStream = ZipHandler.OpenRead(archivePath, zipEntry.EntryName);
                    var currentHash = await ComputeStreamHashAsync(currentStream, ct);
                    if (currentHash != meta.FileHash)
                        return new FileChangeResult(segmentId, FileChangeStatus.Modified,
                            "Archive entry hash differs (timestamp and content changed)");

                    return new FileChangeResult(segmentId, FileChangeStatus.Unchanged,
                        "Archive timestamp differs but entry hash matches");
                }

                return new FileChangeResult(segmentId, FileChangeStatus.Modified,
                    "Archive last modified time changed");
            }

            return new FileChangeResult(segmentId, FileChangeStatus.Unchanged);
        }

        if (!File.Exists(canonicalSourcePath))
            return new FileChangeResult(segmentId, FileChangeStatus.Deleted, $"File not found: {canonicalSourcePath}");

        var fileInfo = new FileInfo(canonicalSourcePath);

        // Quick checks: size and modification date
        if (meta.FileSizeBytes.HasValue && fileInfo.Length != meta.FileSizeBytes.Value)
            return new FileChangeResult(segmentId, FileChangeStatus.Modified,
                $"Size changed: {meta.FileSizeBytes.Value} → {fileInfo.Length}");

        if (meta.LastModifiedUtc.HasValue && fileInfo.LastWriteTimeUtc != meta.LastModifiedUtc.Value) {
            // Last-modified differs — confirm with hash if available and file is small enough
            if (meta.FileHash is not null && fileInfo.Length <= HashThresholdBytes) {
                var currentHash = await ComputeFileHashAsync(meta.SourcePath, ct);
                if (currentHash != meta.FileHash)
                    return new FileChangeResult(segmentId, FileChangeStatus.Modified,
                        "File hash differs (timestamp and content changed)");

                // Hash matches despite different timestamp — treat as unchanged
                return new FileChangeResult(segmentId, FileChangeStatus.Unchanged,
                    "Timestamp differs but content hash matches");
            }

            return new FileChangeResult(segmentId, FileChangeStatus.Modified,
                "Last modified time changed");
        }

        return new FileChangeResult(segmentId, FileChangeStatus.Unchanged);
    }

    /// <summary>
    /// Stores current file metadata into the segments table for future comparison.
    /// </summary>
    public async Task RecordFileMetadataAsync(string segmentId, string sourcePath, CancellationToken ct = default)
    {
        var canonicalSourcePath = SourcePathHelper.Normalize(sourcePath);
        long fileSizeBytes;
        DateTime lastModifiedUtc;
        string? hash = null;

        if (SourcePathHelper.TrySplitArchiveEntry(canonicalSourcePath, out var archivePath, out var entryFullName)) {
            if (!File.Exists(archivePath) || !ZipHandler.TryGetEntry(archivePath, entryFullName, out var zipEntry))
                return;

            fileSizeBytes = zipEntry.SizeBytes;
            lastModifiedUtc = File.GetLastWriteTimeUtc(archivePath);
            if (fileSizeBytes <= HashThresholdBytes) {
                await using var entryStream = ZipHandler.OpenRead(archivePath, zipEntry.EntryName);
                hash = await ComputeStreamHashAsync(entryStream, ct);
            }
        } else {
            var fileInfo = new FileInfo(canonicalSourcePath);
            if (!fileInfo.Exists)
                return;

            fileSizeBytes = fileInfo.Length;
            lastModifiedUtc = fileInfo.LastWriteTimeUtc;
            if (fileSizeBytes <= HashThresholdBytes)
                hash = await ComputeFileHashAsync(canonicalSourcePath, ct);
        }

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            UPDATE segments SET
                source_path = $1,
                file_size_bytes = $2,
                last_modified_utc = $3,
                file_hash = $4
            WHERE segment_id = $5
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = canonicalSourcePath });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileSizeBytes });
        cmd.Parameters.Add(new DuckDBParameter { Value = lastModifiedUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)hash ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task<SegmentFileMeta?> LoadSegmentFileMetaAsync(string segmentId, CancellationToken ct)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            SELECT source_path, file_size_bytes, last_modified_utc, file_hash
            FROM segments WHERE segment_id = $1
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = segmentId });
        using var reader = await cmd.ExecuteReaderAsync(ct);

        if (!await reader.ReadAsync(ct))
            return null;

        return new SegmentFileMeta(
            SourcePath: reader.IsDBNull(0) ? null : reader.GetString(0),
            FileSizeBytes: reader.IsDBNull(1) ? null : reader.GetInt64(1),
            LastModifiedUtc: reader.IsDBNull(2) ? null : reader.GetDateTime(2),
            FileHash: reader.IsDBNull(3) ? null : reader.GetString(3));
    }

    internal static async Task<string> ComputeFileHashAsync(string filePath, CancellationToken ct = default)
    {
        using var stream = File.OpenRead(filePath);
        return await ComputeStreamHashAsync(stream, ct);
    }

    internal static async Task<string> ComputeStreamHashAsync(Stream stream, CancellationToken ct = default)
    {
        var originalPosition = stream.CanSeek ? stream.Position : 0;
        if (stream.CanSeek)
            stream.Position = 0;

        var hash = await SHA256.HashDataAsync(stream, ct);

        if (stream.CanSeek)
            stream.Position = originalPosition;

        return Convert.ToHexStringLower(hash);
    }

    private sealed record SegmentFileMeta(
        string? SourcePath,
        long? FileSizeBytes,
        DateTime? LastModifiedUtc,
        string? FileHash);
}

public enum FileChangeStatus
{
    Unchanged,
    Modified,
    Deleted,
    New
}

public sealed record FileChangeResult(
    string SegmentId,
    FileChangeStatus Status,
    string? Detail = null);
