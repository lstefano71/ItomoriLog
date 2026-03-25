namespace ItomoriLog.Core.Ingest;

public static class FeedbackTemplateKeyBuilder
{
    public static string BuildKey(string sourcePath)
    {
        var sourceName = GetSourceName(sourcePath);
        var logicalSourceId = IdentityGenerator.LogicalSourceId(sourceName).Trim().ToLowerInvariant();
        var extension = Path.GetExtension(sourceName).Trim().ToLowerInvariant();
        return $"{logicalSourceId}|{extension}";
    }

    public static string GetSourceName(string sourcePath)
    {
        var normalized = SourcePathHelper.Normalize(sourcePath);
        if (SourcePathHelper.TrySplitArchiveEntry(normalized, out _, out var entryName))
            return Path.GetFileName(entryName);

        return Path.GetFileName(normalized);
    }
}
