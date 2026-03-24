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

            var sourcePath = $"{zipPath}!/{entry.FullName}";
            yield return new ZipFileEntry(entry.FullName, sourcePath, entry.Length, entry.LastWriteTime);
        }
    }

    public static MemoryStream ExtractToMemory(string zipPath, string entryFullName)
    {
        using var archive = ZipFile.OpenRead(zipPath);
        var entry = archive.GetEntry(entryFullName)
            ?? throw new FileNotFoundException($"Entry not found: {entryFullName}");

        var ms = new MemoryStream();
        using (var entryStream = entry.Open())
            entryStream.CopyTo(ms);
        ms.Position = 0;
        return ms;
    }
}

public sealed record ZipFileEntry(
    string EntryName,
    string SourcePath,
    long CompressedLength,
    DateTimeOffset LastWriteTime);
