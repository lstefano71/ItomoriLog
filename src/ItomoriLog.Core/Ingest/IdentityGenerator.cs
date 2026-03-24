using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace ItomoriLog.Core.Ingest;

public static class IdentityGenerator
{
    public static string PhysicalFileId(string canonicalPath, long fileSize, DateTimeOffset lastModifiedUtc)
    {
        var input = $"{canonicalPath}:{fileSize}:{lastModifiedUtc:O}";
        return ComputeHash(input);
    }

    public static string SegmentId(string physicalFileId, int formatRegionIndex = 0)
    {
        var input = $"{physicalFileId}:{formatRegionIndex}";
        return ComputeHash(input);
    }

    public static string LogicalSourceId(string fileName)
    {
        var name = Path.GetFileNameWithoutExtension(fileName);
        // Remove common timestamp patterns from filename
        name = Regex.Replace(name, @"\d{4}[-_]?\d{2}[-_]?\d{2}[-_T]?\d{2}[-_:]?\d{2}[-_:]?\d{2}", "");
        name = Regex.Replace(name, @"\d{8,14}", "");
        name = name.Trim('-', '_', '.');
        return string.IsNullOrWhiteSpace(name) ? fileName : name;
    }

    private static string ComputeHash(string input)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexStringLower(bytes)[..16];
    }
}
