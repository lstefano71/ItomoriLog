using System.IO.Compression;
using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Ingest;

public static class ZipHandler
{
    public static IEnumerable<ZipFileEntry> EnumerateEntries(string zipPath, ISkipSink? skipSink = null)
    {
        using var archive = ZipFile.OpenRead(zipPath);
        foreach (var entry in archive.Entries)
        {
            // Skip directories
            if (string.IsNullOrEmpty(entry.Name)) continue;

            // Skip nested ZIPs
            if (entry.FullName.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
            {
                skipSink?.Write(new SkipRow(
                    LogicalSourceId: IdentityGenerator.LogicalSourceId(entry.Name),
                    PhysicalFileId: "zip-entry",
                    SegmentId: "zip-entry",
                    SegmentSeq: 0,
                    StartLine: null, EndLine: null,
                    StartOffset: null, EndOffset: null,
                    ReasonCode: SkipReasonCode.NestedArchive,
                    ReasonDetail: $"Nested ZIP: {entry.FullName}",
                    SamplePrefix: null,
                    DetectorProfileId: null,
                    UtcLoggedAt: DateTimeOffset.UtcNow));
                continue;
            }

            var sourcePath = SourcePathHelper.CombineArchiveEntryPath(zipPath, entry.FullName);
            yield return new ZipFileEntry(entry.FullName, sourcePath, entry.Length, entry.LastWriteTime);
        }
    }

    public static bool TryGetEntry(string zipPath, string entryFullName, out ZipFileEntry entry)
    {
        var normalizedZipPath = Path.GetFullPath(zipPath);
        var normalizedEntryName = entryFullName.TrimStart('/', '\\').Replace('\\', '/');

        using var archive = ZipFile.OpenRead(normalizedZipPath);
        var zipEntry = archive.GetEntry(normalizedEntryName);
        if (zipEntry is null || string.IsNullOrEmpty(zipEntry.Name))
        {
            entry = null!;
            return false;
        }

        entry = new ZipFileEntry(
            zipEntry.FullName,
            SourcePathHelper.CombineArchiveEntryPath(normalizedZipPath, zipEntry.FullName),
            zipEntry.Length,
            zipEntry.LastWriteTime);
        return true;
    }

    public static bool TryGetEntry(string sourcePath, out ZipFileEntry entry)
    {
        if (!SourcePathHelper.TrySplitArchiveEntry(sourcePath, out var archivePath, out var entryFullName))
        {
            entry = null!;
            return false;
        }

        return TryGetEntry(archivePath, entryFullName, out entry);
    }

    public static Stream OpenRead(string zipPath, string entryFullName)
    {
        var archive = ZipFile.OpenRead(Path.GetFullPath(zipPath));
        var normalizedEntryName = entryFullName.TrimStart('/', '\\').Replace('\\', '/');
        var entry = archive.GetEntry(normalizedEntryName)
            ?? throw new FileNotFoundException($"Entry not found: {entryFullName}");

        return new ZipEntryStream(archive, entry.Open());
    }
}

public sealed record ZipFileEntry(
    string EntryName,
    string SourcePath,
    long SizeBytes,
    DateTimeOffset LastWriteTime);

internal sealed class ZipEntryStream : Stream
{
    private readonly ZipArchive _archive;
    private readonly Stream _inner;
    private bool _disposed;

    public ZipEntryStream(ZipArchive archive, Stream inner)
    {
        _archive = archive;
        _inner = inner;
    }

    public override bool CanRead => !_disposed && _inner.CanRead;
    public override bool CanSeek => !_disposed && _inner.CanSeek;
    public override bool CanWrite => false;
    public override long Length => _inner.Length;
    public override long Position
    {
        get => _inner.Position;
        set => _inner.Position = value;
    }

    public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
    public override int Read(Span<byte> buffer) => _inner.Read(buffer);
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        => _inner.ReadAsync(buffer, cancellationToken);
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => _inner.ReadAsync(buffer, offset, count, cancellationToken);
    public override void Flush() => _inner.Flush();
    public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);
    public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        if (disposing)
        {
            _inner.Dispose();
            _archive.Dispose();
        }

        _disposed = true;
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        await _inner.DisposeAsync();
        _archive.Dispose();
        _disposed = true;
        await base.DisposeAsync();
    }
}
