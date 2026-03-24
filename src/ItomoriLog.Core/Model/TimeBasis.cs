namespace ItomoriLog.Core.Model;

public enum TimeBasis
{
    Local,
    Utc,
    FixedOffset,
    Zone
}

public sealed record TimeBasisConfig(
    TimeBasis Basis,
    int? OffsetMinutes = null,
    string? TimeZoneId = null);
