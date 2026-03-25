namespace ItomoriLog.Core.Storage;

public static class SessionDefaults
{
    public static string BuildDefaultSessionTitle(DateTime? localNow = null) =>
        $"ItomoriLog Session — {(localNow ?? DateTime.Now):yyyy-MM-dd HH:mm}";

    public static string ResolveDefaultTimezone(string? timezoneId) =>
        string.IsNullOrWhiteSpace(timezoneId)
            ? TimeZoneInfo.Local.Id
            : timezoneId.Trim();

    public static bool IsValidTimezoneId(string timezoneId)
    {
        if (string.IsNullOrWhiteSpace(timezoneId))
            return false;

        try {
            _ = TimeZoneInfo.FindSystemTimeZoneById(timezoneId);
            return true;
        } catch {
            return false;
        }
    }
}
