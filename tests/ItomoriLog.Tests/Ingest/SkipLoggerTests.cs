using FluentAssertions;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;

namespace ItomoriLog.Tests.Ingest;

public class SkipLoggerTests
{
    [Fact]
    public void BeginAndClose_EmitsSkipRow()
    {
        var sink = new ListSkipSink();
        var logger = new SkipLogger(sink, "source1", "file1", "seg1");

        var segment = logger.BeginSkip(SkipReasonCode.TimeParse, "Bad timestamp", startLine: 42);
        segment.Close(endLine: 45);

        var skips = sink.GetSkips();
        skips.Should().ContainSingle();
        skips[0].ReasonCode.Should().Be(SkipReasonCode.TimeParse);
        skips[0].StartLine.Should().Be(42);
        skips[0].EndLine.Should().Be(45);
    }

    [Fact]
    public void Dispose_ClosesAutomatically()
    {
        var sink = new ListSkipSink();
        var logger = new SkipLogger(sink, "source1", "file1", "seg1");

        using (var segment = logger.BeginSkip(SkipReasonCode.DecodeError, "Encoding issue"))
        {
            // dispose will auto-close
        }

        sink.GetSkips().Should().ContainSingle();
    }

    [Fact]
    public void MultipleSkips_GetIncrementingSequence()
    {
        var sink = new ListSkipSink();
        var logger = new SkipLogger(sink, "source1", "file1", "seg1");

        logger.BeginSkip(SkipReasonCode.TimeParse, "a").Close();
        logger.BeginSkip(SkipReasonCode.Oversize, "b").Close();
        logger.BeginSkip(SkipReasonCode.RegexDrift, "c").Close();

        var seqs = sink.GetSkips().Select(s => s.SegmentSeq).ToList();
        seqs.Should().BeInAscendingOrder();
        seqs.Should().OnlyHaveUniqueItems();
    }
}
