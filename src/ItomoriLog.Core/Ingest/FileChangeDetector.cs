using System.Security.Cryptography;
using DuckDB.NET.Data;

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

        if (!File.Exists(meta.SourcePath))
            return new FileChangeResult(segmentId, FileChangeStatus.Deleted, $"File not found: {meta.SourcePath}");

        var fileInfo = new FileInfo(meta.SourcePath);

        // Quick checks: size and modification date
        if (meta.FileSizeBytes.HasValue && fileInfo.Length != meta.FileSizeBytes.Value)
            return new FileChangeResult(segmentId, FileChangeStatus.Modified,
                $"Size changed: {meta.FileSizeBytes.Value} → {fileInfo.Length}");

        if (meta.LastModifiedUtc.HasValue && fileInfo.LastWriteTimeUtc != meta.LastModifiedUtc.Value)
        {
            // Last-modified differs — confirm with hash if available and file is small enough
            if (meta.FileHash is not null && fileInfo.Length <= HashThresholdBytes)
            {
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
        var fileInfo = new FileInfo(sourcePath);
        if (!fileInfo.Exists) return;

        string? hash = null;
        if (fileInfo.Length <= HashThresholdBytes)
            hash = await ComputeFileHashAsync(sourcePath, ct);

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            UPDATE segments SET
                source_path = $1,
                file_size_bytes = $2,
                last_modified_utc = $3,
                file_hash = $4
            WHERE segment_id = $5
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = sourcePath });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileInfo.Length });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileInfo.LastWriteTimeUtc });
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
        var hash = await SHA256.HashDataAsync(stream, ct);
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
