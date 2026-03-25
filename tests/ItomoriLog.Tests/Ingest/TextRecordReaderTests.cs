using FluentAssertions;

using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Ingest.Readers;

namespace ItomoriLog.Tests.Ingest;

public class TextRecordReaderTests
{
    [Fact]
    public void SingleLineRecords_ReadsAll()
    {
        var input = """
            2024-03-15 10:00:00.123 INFO Starting app
            2024-03-15 10:00:01.456 DEBUG Loading config
            2024-03-15 10:00:02.789 WARN Config missing
            """;

        var records = ReadAllRecords(input);

        records.Should().HaveCount(3);
        records[0].FirstLine.Should().Contain("Starting app");
        records[2].FirstLine.Should().Contain("Config missing");
    }

    [Fact]
    public void MultilineRecords_ContinuationLinesAppended()
    {
        var input = """
            2024-03-15 10:00:00.123 ERROR NullRef exception
              at MyApp.Main() in Program.cs:line 42
              at System.AppDomain.Run()
            2024-03-15 10:00:01.456 INFO Recovery complete
            """;

        var records = ReadAllRecords(input);

        records.Should().HaveCount(2);
        records[0].FullText.Should().Contain("at MyApp.Main()");
        records[0].FullText.Should().Contain("at System.AppDomain.Run()");
        records[1].FirstLine.Should().Contain("Recovery complete");
    }

    [Fact]
    public void EmptyInput_ReturnsNoRecords()
    {
        var records = ReadAllRecords("");
        records.Should().BeEmpty();
    }

    [Fact]
    public void LeadingGarbage_SkippedUntilFirstMatch()
    {
        var input = """
            some garbage line
            another garbage
            2024-03-15 10:00:00.123 INFO First real record
            """;

        var records = ReadAllRecords(input);

        records.Should().ContainSingle();
        records[0].FirstLine.Should().Contain("First real record");
    }

    private static List<RawRecord> ReadAllRecords(string input)
    {
        using var reader = new TextRecordReader(new StringReader(input), SoRPatterns.Iso8601());
        var records = new List<RawRecord>();
        while (reader.TryReadNext(out var rec))
            records.Add(rec);
        return records;
    }
}
