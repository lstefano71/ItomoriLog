using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Ingest;

public static class TimezonePolicy
{
    public static DateTimeOffset ApplyTimeBasis(DateTimeOffset raw, TimeBasisConfig config)
    {
        return config.Basis switch
        {
            TimeBasis.Utc => raw.ToUniversalTime(),
            TimeBasis.Local => raw.ToLocalTime().ToUniversalTime(),
            TimeBasis.FixedOffset when config.OffsetMinutes.HasValue =>
                raw.ToOffset(TimeSpan.FromMinutes(config.OffsetMinutes.Value)).ToUniversalTime(),
            TimeBasis.Zone when config.TimeZoneId is not null =>
                TimeZoneInfo.ConvertTime(raw, TimeZoneInfo.FindSystemTimeZoneById(config.TimeZoneId)).ToUniversalTime(),
            _ => raw.ToUniversalTime(),
        };
    }

    /// <summary>
    /// Applies timezone basis to a bare DateTime (no offset info). This is the common path for log timestamps.
    /// </summary>
    public static DateTimeOffset ApplyTimeBasisToBare(DateTime bareDateTime, TimeBasisConfig config)
    {
        return config.Basis switch
        {
            TimeBasis.Utc => new DateTimeOffset(DateTime.SpecifyKind(bareDateTime, DateTimeKind.Utc)),
            TimeBasis.Local => new DateTimeOffset(DateTime.SpecifyKind(bareDateTime, DateTimeKind.Local)).ToUniversalTime(),
            TimeBasis.FixedOffset when config.OffsetMinutes.HasValue =>
                new DateTimeOffset(bareDateTime, TimeSpan.FromMinutes(config.OffsetMinutes.Value)).ToUniversalTime(),
            TimeBasis.Zone when config.TimeZoneId is not null =>
                ConvertFromZone(bareDateTime, config.TimeZoneId),
            _ => new DateTimeOffset(DateTime.SpecifyKind(bareDateTime, DateTimeKind.Local)).ToUniversalTime(),
        };
    }

    private static DateTimeOffset ConvertFromZone(DateTime dt, string zoneId)
    {
        var tz = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
        var offset = tz.GetUtcOffset(dt);
        if (tz.IsInvalidTime(dt))
        {
            // Spring forward gap — shift forward
            dt = dt.Add(tz.GetAdjustmentRules()
                .Where(r => r.DateStart <= dt && r.DateEnd >= dt)
                .Select(r => r.DaylightDelta)
                .FirstOrDefault(TimeSpan.FromHours(1)));
            offset = tz.GetUtcOffset(dt);
        }
        return new DateTimeOffset(dt, offset).ToUniversalTime();
    }
}
