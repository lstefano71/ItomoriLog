namespace ItomoriLog.App;

public static class Brand
{
    public const string AppName = "ItomoriLog";
    public const string Tagline = "Braiding time from your logs";
    public const string DefaultDbFileName = "ItomoriLog.duckdb";
    public const string AppVersion = "0.1.0";
    public const string SessionsFolderName = "Sessions";

    public static string DefaultSessionsRoot =>
        Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            AppName,
            SessionsFolderName);

    public static string AppDataRoot =>
        Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            ".itomorilog");
}
