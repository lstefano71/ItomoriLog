namespace ItomoriLog.Core.Storage;

public static class SessionPaths
{
    public const string DefaultDbFileName = "ItomoriLog.duckdb";

    public static string DefaultSessionsRoot =>
        Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            "ItomoriLog",
            "Sessions");

    public static string CreateNew(string basePath, string title)
    {
        var sanitized = SanitizeFolderName(title);
        var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd_HH-mm-ss");
        var folderName = $"{timestamp}_{sanitized}";
        var fullPath = Path.Combine(basePath, folderName);

        Directory.CreateDirectory(fullPath);
        Directory.CreateDirectory(Path.Combine(fullPath, "exports"));

        return fullPath;
    }

    public static string GetDbPath(string sessionFolder) =>
        Path.Combine(sessionFolder, DefaultDbFileName);

    private static string SanitizeFolderName(string name)
    {
        var invalid = Path.GetInvalidFileNameChars();
        var sanitized = new string(name
            .Select(c => invalid.Contains(c) || c == ' ' ? '_' : c)
            .ToArray());
        return sanitized.Length > 60 ? sanitized[..60] : sanitized;
    }
}
