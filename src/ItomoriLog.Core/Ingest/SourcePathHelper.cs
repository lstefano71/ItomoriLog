namespace ItomoriLog.Core.Ingest;

public static class SourcePathHelper
{
    public static string Normalize(string sourcePath)
    {
        if (TrySplitArchiveEntry(sourcePath, out var archivePath, out var entryName))
            return CombineArchiveEntryPath(archivePath, entryName);

        return Path.GetFullPath(sourcePath);
    }

    public static bool IsArchiveEntryPath(string sourcePath) =>
        TrySplitArchiveEntry(sourcePath, out _, out _);

    public static bool IsArchiveFilePath(string sourcePath) =>
        !IsArchiveEntryPath(sourcePath)
        && sourcePath.EndsWith(".zip", StringComparison.OrdinalIgnoreCase);

    public static string CombineArchiveEntryPath(string archivePath, string entryName)
    {
        var normalizedArchivePath = Path.GetFullPath(archivePath);
        var normalizedEntryName = entryName
            .TrimStart('/', '\\')
            .Replace('\\', '/');
        return $"{normalizedArchivePath}!/{normalizedEntryName}";
    }

    public static bool TrySplitArchiveEntry(string sourcePath, out string archivePath, out string entryName)
    {
        archivePath = string.Empty;
        entryName = string.Empty;

        if (string.IsNullOrWhiteSpace(sourcePath))
            return false;

        var bangIndex = sourcePath.IndexOf('!');
        if (bangIndex <= 0 || bangIndex >= sourcePath.Length - 1)
            return false;

        var archivePart = sourcePath[..bangIndex];
        var entryPart = sourcePath[(bangIndex + 1)..].TrimStart('/', '\\');
        if (string.IsNullOrWhiteSpace(entryPart))
            return false;

        archivePath = Path.GetFullPath(archivePart);
        entryName = entryPart.Replace('\\', '/');
        return true;
    }
}
