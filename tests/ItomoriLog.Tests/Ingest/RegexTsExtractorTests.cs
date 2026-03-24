using FluentAssertions;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Ingest.Extractors;

namespace ItomoriLog.Tests.Ingest;

public class RegexTsExtractorTests
{
    private readonly RegexGroupTsExtractor _extractor = new(SoRPatterns.Iso8601());

    [Theory]
    [InlineData("2024-03-15T10:30:45.123Z INFO message")]
    [InlineData("2024-03-15 10:30:45.123 INFO message")]
    [InlineData("2024-03-15T10:30:45+02:00 INFO message")]
    public void TryExtract_ValidIso_Succeeds(string line)
    {
        var raw = new RawRecord(line, line, 1, 0);
        var result = _extractor.TryExtract(raw, out var ts);

        result.Should().BeTrue();
        ts.Year.Should().Be(2024);
        ts.Month.Should().Be(3);
        ts.Day.Should().Be(15);
    }

    [Fact]
    public void TryExtract_NoMatch_ReturnsFalse()
    {
        var raw = new RawRecord("no timestamp here", "no timestamp here", 1, 0);
        _extractor.TryExtract(raw, out _).Should().BeFalse();
    }

    [Fact]
    public void TryExtract_EpochSeconds_ParsesCorrectly()
    {
        var epochExtractor = new RegexGroupTsExtractor(SoRPatterns.EpochSeconds());
        var raw = new RawRecord("1710500000 message", "1710500000 message", 1, 0);
        var result = epochExtractor.TryExtract(raw, out var ts);

        result.Should().BeTrue();
        ts.Year.Should().Be(2024);
    }
}
