using FluentAssertions;

using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;

namespace ItomoriLog.Tests.Ingest;

public class TimestampResolverTests
{
    private sealed class StubExtractor : ITimestampExtractor, ITimestampExtractorWithMetadata
    {
        private readonly TimestampExtraction _extraction;

        public StubExtractor(TimestampExtraction extraction)
        {
            _extraction = extraction;
        }

        public bool TryExtract(RawRecord raw, out DateTimeOffset timestamp)
        {
            if (_extraction.ExplicitTimestamp.HasValue) {
                timestamp = _extraction.ExplicitTimestamp.Value;
                return true;
            }

            if (_extraction.BareTimestamp.HasValue) {
                timestamp = new DateTimeOffset(_extraction.BareTimestamp.Value, TimeSpan.Zero);
                return true;
            }

            timestamp = default;
            return false;
        }

        public bool TryExtractWithMetadata(RawRecord raw, out TimestampExtraction extraction)
        {
            extraction = _extraction;
            return true;
        }

        public string Description => "stub";
    }

    [Fact]
    public void Resolve_ExplicitOffset_UsesPerRecordBasis()
    {
        var extractor = new StubExtractor(
            TimestampExtraction.FromExplicit(
                new DateTimeOffset(2024, 3, 15, 12, 0, 0, TimeSpan.FromHours(2)),
                "2024-03-15T12:00:00+02:00"));

        var ok = TimestampResolver.TryResolve(
            extractor,
            new RawRecord("", "", 0, 0),
            new TimeBasisConfig(TimeBasis.Local),
            out var resolved);

        ok.Should().BeTrue();
        resolved.Basis.Should().Be(TimeBasis.FixedOffset);
        resolved.EffectiveOffsetMinutes.Should().Be(120);
        resolved.UtcTimestamp.Should().Be(new DateTimeOffset(2024, 3, 15, 10, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public void Resolve_BareTimestamp_UsesConfiguredBasis()
    {
        var extractor = new StubExtractor(
            TimestampExtraction.FromBare(
                new DateTime(2024, 3, 15, 12, 0, 0, DateTimeKind.Unspecified),
                "2024-03-15 12:00:00"));

        var ok = TimestampResolver.TryResolve(
            extractor,
            new RawRecord("", "", 0, 0),
            new TimeBasisConfig(TimeBasis.FixedOffset, OffsetMinutes: 180),
            out var resolved);

        ok.Should().BeTrue();
        resolved.Basis.Should().Be(TimeBasis.FixedOffset);
        resolved.EffectiveOffsetMinutes.Should().Be(180);
        resolved.UtcTimestamp.Should().Be(new DateTimeOffset(2024, 3, 15, 9, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public void Resolve_DualTimestamps_PrefersExplicitAndStoresAlternate()
    {
        var extractor = new StubExtractor(
            new TimestampExtraction(
                ExplicitTimestamp: null,
                BareTimestamp: new DateTime(2024, 3, 15, 12, 0, 0, DateTimeKind.Unspecified),
                ParsedText: "2024-03-15 12:00:00",
                AlternateText: "2024-03-15T10:00:00.123456Z",
                UsedTwoDigitYear: false));

        var ok = TimestampResolver.TryResolve(
            extractor,
            new RawRecord("", "", 0, 0),
            new TimeBasisConfig(TimeBasis.Local),
            out var resolved);

        ok.Should().BeTrue();
        resolved.Basis.Should().Be(TimeBasis.Utc);
        resolved.TimestampOriginal.Should().Be("2024-03-15 12:00:00");
        resolved.UtcTimestamp.Should().Be(new DateTimeOffset(2024, 3, 15, 10, 0, 0, 123, TimeSpan.Zero).AddTicks(4560));
    }

    [Fact]
    public void Resolve_TwoDigitYearWindow_Uses1900For50And2000For49()
    {
        TimestampParsing.TryParseWithTwoDigitYearWindow("49-03-15 12:00:00", out var yy49, out var used49).Should().BeTrue();
        TimestampParsing.TryParseWithTwoDigitYearWindow("50-03-15 12:00:00", out var yy50, out var used50).Should().BeTrue();

        used49.Should().BeTrue();
        used50.Should().BeTrue();
        yy49.Year.Should().Be(2049);
        yy50.Year.Should().Be(1950);
    }

    [Fact]
    public void Resolve_DstAmbiguous_UsesPostTransitionOffset()
    {
        var extractor = new StubExtractor(
            TimestampExtraction.FromBare(
                new DateTime(2024, 11, 3, 1, 30, 0, DateTimeKind.Unspecified),
                "2024-11-03 01:30:00"));

        var ok = TimestampResolver.TryResolve(
            extractor,
            new RawRecord("", "", 0, 0),
            new TimeBasisConfig(TimeBasis.Zone, TimeZoneId: "Eastern Standard Time"),
            out var resolved);

        ok.Should().BeTrue();
        resolved.Basis.Should().Be(TimeBasis.Zone);
        resolved.EffectiveOffsetMinutes.Should().Be(-300);
    }
}
