using FluentAssertions;

using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;

namespace ItomoriLog.Tests.Ingest;

public class TimezonePolicyTests
{
    [Fact]
    public void ApplyTimeBasisToBare_Utc_ReturnsUtc()
    {
        var bare = new DateTime(2024, 3, 15, 10, 0, 0, DateTimeKind.Unspecified);
        var config = new TimeBasisConfig(TimeBasis.Utc);

        var result = TimezonePolicy.ApplyTimeBasisToBare(bare, config);

        result.Offset.Should().Be(TimeSpan.Zero);
        result.UtcDateTime.Should().Be(bare);
    }

    [Fact]
    public void ApplyTimeBasisToBare_FixedOffset_AppliesCorrectly()
    {
        var bare = new DateTime(2024, 3, 15, 12, 0, 0, DateTimeKind.Unspecified);
        var config = new TimeBasisConfig(TimeBasis.FixedOffset, OffsetMinutes: 120); // +02:00

        var result = TimezonePolicy.ApplyTimeBasisToBare(bare, config);

        result.UtcDateTime.Hour.Should().Be(10); // 12:00 +02:00 = 10:00 UTC
    }

    [Fact]
    public void ApplyTimeBasis_PreservesOriginalMeaning()
    {
        var original = new DateTimeOffset(2024, 3, 15, 10, 0, 0, TimeSpan.FromHours(2));
        var config = new TimeBasisConfig(TimeBasis.Utc);

        var result = TimezonePolicy.ApplyTimeBasis(original, config);

        result.UtcDateTime.Should().Be(original.UtcDateTime);
    }
}
