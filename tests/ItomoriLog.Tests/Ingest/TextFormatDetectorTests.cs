using FluentAssertions;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Ingest.Detectors;

namespace ItomoriLog.Tests.Ingest;

public class TextFormatDetectorTests
{
    [Fact]
    public void Probe_IsoTimestampLogs_DetectsWithHighConfidence()
    {
        var log = GenerateIsoLog(100);
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(log));

        var detector = new TextFormatDetector();
        var result = detector.Probe(stream, "test.log");

        result.Should().NotBeNull();
        result!.Confidence.Should().BeGreaterThan(0.8);
        result.Boundary.Should().BeOfType<TextSoRBoundary>();
        ((TextSoRBoundary)result.Boundary).PatternName.Should().NotBeNullOrWhiteSpace();
    }

    [Fact]
    public void Probe_RandomGarbage_ReturnsNull()
    {
        var lines = Enumerable.Range(0, 100)
            .Select(i => $"random garbage line {i} with no timestamp pattern");
        var content = string.Join('\n', lines);
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));

        var detector = new TextFormatDetector();
        var result = detector.Probe(stream, "garbage.txt");

        result.Should().BeNull();
    }

    [Fact]
    public void Probe_SyslogFormat_Detects()
    {
        var lines = Enumerable.Range(0, 100)
            .Select(i => $"Mar 15 10:{i:D2}:00 myhost sshd[1234]: Connection from 10.0.0.{i}");
        var content = string.Join('\n', lines);
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));

        var detector = new TextFormatDetector();
        var result = detector.Probe(stream, "syslog");

        result.Should().NotBeNull();
        result!.Confidence.Should().BeGreaterThan(0.5);
    }

    private static string GenerateIsoLog(int count)
    {
        var baseTime = new DateTime(2024, 3, 15, 10, 0, 0);
        var lines = Enumerable.Range(0, count)
            .Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO Message {i}");
        return string.Join('\n', lines);
    }
}
