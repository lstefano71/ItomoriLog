using FluentAssertions;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Ingest.Detectors;
using ItomoriLog.Core.Ingest.Extractors;
using ItomoriLog.Core.Ingest.Readers;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Tests.Ingest;

public class CsvIngestionTests
{
    [Fact]
    public void Probe_CommaSeparatedWithHeader_DetectsCorrectly()
    {
        var csv = """
            Timestamp,Level,Message
            2024-03-15 10:00:00,INFO,Starting application
            2024-03-15 10:00:01,DEBUG,Loading config file
            2024-03-15 10:00:02,WARN,Config not found
            2024-03-15 10:00:03,INFO,Using defaults
            2024-03-15 10:00:04,INFO,App started
            2024-03-15 10:00:05,ERROR,Connection failed
            2024-03-15 10:00:06,INFO,Retrying
            2024-03-15 10:00:07,INFO,Connected
            2024-03-15 10:00:08,DEBUG,Handshake complete
            2024-03-15 10:00:09,INFO,Ready
            """;

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));
        var detector = new CsvFormatDetector();
        var result = detector.Probe(stream, "test.csv");

        result.Should().NotBeNull();
        result!.Confidence.Should().BeGreaterThan(0.8);
        result.Boundary.Should().BeOfType<CsvBoundary>();

        var boundary = (CsvBoundary)result.Boundary;
        boundary.Delimiter.Should().Be(',');
        boundary.HasHeader.Should().BeTrue();
        boundary.ColumnNames.Should().Contain("Timestamp");
        boundary.ColumnNames.Should().Contain("Level");
        boundary.ColumnNames.Should().Contain("Message");
    }

    [Fact]
    public void Probe_SemicolonDelimited_DetectsDelimiter()
    {
        var csv = """
            Timestamp;Severity;Text
            2024-03-15 10:00:00;INFO;Starting application
            2024-03-15 10:00:01;DEBUG;Loading config
            2024-03-15 10:00:02;WARN;Config issue
            2024-03-15 10:00:03;INFO;Using defaults
            2024-03-15 10:00:04;INFO;App started
            2024-03-15 10:00:05;ERROR;Failure
            2024-03-15 10:00:06;INFO;Retry
            2024-03-15 10:00:07;INFO;Connected
            2024-03-15 10:00:08;DEBUG;Done
            2024-03-15 10:00:09;INFO;Ready
            """;

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));
        var detector = new CsvFormatDetector();
        var result = detector.Probe(stream, "test.csv");

        result.Should().NotBeNull();
        var boundary = (CsvBoundary)result!.Boundary;
        boundary.Delimiter.Should().Be(';');
    }

    [Fact]
    public void Probe_SingleQuotedSemicolonDelimited_DetectsQuoteAndReadsRows()
    {
        var csv = """
            'Timestamp';'Level';'Message'
            '2024-03-15 10:00:00';'INFO';'It''s, alive'
            '2024-03-15 10:00:01';'DEBUG';'Loading, config'
            '2024-03-15 10:00:02';'WARN';'Config, issue'
            '2024-03-15 10:00:03';'INFO';'Using, defaults'
            '2024-03-15 10:00:04';'INFO';'App, started'
            '2024-03-15 10:00:05';'ERROR';'Connection, failed'
            """;

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));
        var detector = new CsvFormatDetector();
        var result = detector.Probe(stream, "single-quoted.csv");

        result.Should().NotBeNull();
        var boundary = (CsvBoundary)result!.Boundary;
        boundary.Delimiter.Should().Be(';');
        boundary.Quote.Should().Be('\'');
        boundary.HasHeader.Should().BeTrue();
        boundary.ColumnNames.Should().Contain("Timestamp");
        boundary.ColumnNames.Should().Contain("Level");
        boundary.ColumnNames.Should().Contain("Message");

        using var reader = new CsvRecordReader(new StringReader(csv), boundary);
        reader.TryReadNext(out var first).Should().BeTrue();
        first.Fields.Should().NotBeNull();
        first.Fields!["Message"].Should().Be("It's, alive");
    }

    [Fact]
    public void Probe_TabDelimited_DetectsDelimiter()
    {
        var lines = new List<string> { "Timestamp\tLevel\tMessage" };
        for (int i = 0; i < 10; i++)
            lines.Add($"2024-03-15 10:00:{i:D2}\tINFO\tMessage {i}");
        var tsv = string.Join('\n', lines);

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(tsv));
        var detector = new CsvFormatDetector();
        var result = detector.Probe(stream, "test.tsv");

        result.Should().NotBeNull();
        var boundary = (CsvBoundary)result!.Boundary;
        boundary.Delimiter.Should().Be('\t');
    }

    [Fact]
    public void Probe_HeaderWithSymbolsAndDigits_DetectsHeader()
    {
        var csv = """
            @timestamp,level-name,message_2
            2024-03-15 10:00:00,INFO,Starting application
            2024-03-15 10:00:01,DEBUG,Loading config
            2024-03-15 10:00:02,WARN,Config missing
            2024-03-15 10:00:03,INFO,Using defaults
            2024-03-15 10:00:04,INFO,App started
            2024-03-15 10:00:05,ERROR,Connection failed
            """;

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));
        var detector = new CsvFormatDetector();
        var result = detector.Probe(stream, "header-symbols.csv");

        result.Should().NotBeNull();
        var boundary = (CsvBoundary)result!.Boundary;
        boundary.HasHeader.Should().BeTrue();
        boundary.ColumnNames.Should().Contain("@timestamp");
        boundary.ColumnNames.Should().Contain("level-name");
        boundary.ColumnNames.Should().Contain("message_2");
    }

    [Fact]
    public void CsvReader_BadRows_SkipsAndResyncs()
    {
        var lines = new List<string> { "Timestamp,Level,Message" };
        // 3 good rows
        lines.Add("2024-03-15 10:00:00,INFO,Good 1");
        lines.Add("2024-03-15 10:00:01,INFO,Good 2");
        lines.Add("2024-03-15 10:00:02,INFO,Good 3");
        // 2 bad rows (wrong column count)
        lines.Add("2024-03-15 10:00:03,BAD");
        lines.Add("2024-03-15 10:00:04");
        // 5 good rows to trigger resync (K=5)
        lines.Add("2024-03-15 10:00:05,INFO,Resync 1");
        lines.Add("2024-03-15 10:00:06,INFO,Resync 2");
        lines.Add("2024-03-15 10:00:07,INFO,Resync 3");
        lines.Add("2024-03-15 10:00:08,INFO,Resync 4");
        lines.Add("2024-03-15 10:00:09,INFO,Resync 5");
        var csv = string.Join('\n', lines);

        var skipSink = new ListSkipSink();
        var skipLogger = new SkipLogger(skipSink, "test", "file1", "seg1");
        var boundary = new CsvBoundary(',', true, ["Timestamp", "Level", "Message"]);
        using var reader = new CsvRecordReader(new StringReader(csv), boundary, skipLogger);

        var records = new List<RawRecord>();
        while (reader.TryReadNext(out var rec))
            records.Add(rec);

        // 3 good + 5 resync = 8 records (bad rows skipped)
        records.Should().HaveCount(8);

        // Should have logged a skip
        var skips = skipSink.GetSkips();
        skips.Should().HaveCountGreaterThanOrEqualTo(1);
        skips[0].ReasonCode.Should().Be(SkipReasonCode.CsvColumnMismatch);
    }

    [Fact]
    public void CsvReader_ValidCsv_ReadsAllRows()
    {
        var csv = """
            Timestamp,Level,Message
            2024-03-15 10:00:00,INFO,Line 1
            2024-03-15 10:00:01,DEBUG,Line 2
            2024-03-15 10:00:02,WARN,Line 3
            """;

        var boundary = new CsvBoundary(',', true, ["Timestamp", "Level", "Message"]);
        using var reader = new CsvRecordReader(new StringReader(csv), boundary);

        var records = new List<RawRecord>();
        while (reader.TryReadNext(out var rec))
            records.Add(rec);

        records.Should().HaveCount(3);
        records[0].Fields.Should().ContainKey("Timestamp");
        records[0].Fields!["Timestamp"].Should().Be("2024-03-15 10:00:00");
        records[0].Fields!["Level"].Should().Be("INFO");
        records[0].Fields!["Message"].Should().Be("Line 1");
    }

    [Fact]
    public void CompositeTsExtractor_DateAndTime_Merges()
    {
        var fields = new Dictionary<string, string>
        {
            ["Date"] = "2024-03-15",
            ["Time"] = "10:30:45",
            ["Message"] = "Hello"
        };
        var raw = new RawRecord("", "", 0, 0, fields);

        var extractor = new CompositeCsvTsExtractor(["Date", "Time"]);
        extractor.TryExtract(raw, out var ts).Should().BeTrue();
        ts.Year.Should().Be(2024);
        ts.Month.Should().Be(3);
        ts.Day.Should().Be(15);
    }

    [Fact]
    public void CompositeTsExtractor_SingleColumn_Works()
    {
        var fields = new Dictionary<string, string>
        {
            ["Timestamp"] = "2024-03-15T10:30:45Z",
            ["Message"] = "Hello"
        };
        var raw = new RawRecord("", "", 0, 0, fields);

        var extractor = new CompositeCsvTsExtractor(["Timestamp"]);
        extractor.TryExtract(raw, out var ts).Should().BeTrue();
        ts.Year.Should().Be(2024);
        ts.Month.Should().Be(3);
        ts.Day.Should().Be(15);
        ts.Hour.Should().Be(10);
        ts.Minute.Should().Be(30);
        ts.Second.Should().Be(45);
    }

    [Fact]
    public void Probe_HeaderlessDateAndTimeColumns_PrefersCompositeTimestamp()
    {
        var csv = """
            2023-02-28,13:43:56.961Z,FILE9,INIT
            2023-02-28,13:46:13.116Z,FILE9,PING
            2023-02-28,13:49:10.133Z,FILE9,PING
            2023-02-28,13:51:31.073Z,FILE9,INIT
            2023-02-28,13:53:52.251Z,FILE9,PING
            2023-02-28,13:56:49.303Z,FILE9,PING
            2023-02-28,13:59:46.325Z,FILE9,PING
            2023-02-28,14:02:43.398Z,FILE9,PING
            2023-02-28,14:05:40.434Z,FILE9,PING
            2023-02-28,14:08:37.483Z,FILE9,PING
            """;

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));
        var detector = new CsvFormatDetector();
        var result = detector.Probe(stream, "split-ts.csv");

        result.Should().NotBeNull();
        result!.Boundary.Should().BeOfType<CsvBoundary>();
        var boundary = (CsvBoundary)result.Boundary;
        boundary.HasHeader.Should().BeFalse();
        result!.Extractor.Should().BeOfType<CompositeCsvTsExtractor>();
        result.Extractor.Description.Should().Contain("Column0");
        result.Extractor.Description.Should().Contain("Column1");

        var fields = new Dictionary<string, string>
        {
            ["Column0"] = "2023-02-28",
            ["Column1"] = "13:43:56.961Z",
            ["Column2"] = "FILE9",
            ["Column3"] = "INIT"
        };

        var raw = new RawRecord("", "", 0, 0, fields);
        result.Extractor.TryExtract(raw, out var timestamp).Should().BeTrue();
        timestamp.UtcDateTime.Should().Be(new DateTime(2023, 2, 28, 13, 43, 56, 961, DateTimeKind.Utc));
    }

    [Fact]
    public async Task Integration_CsvEndToEnd_IngestsIntoDb()
    {
        var csv = """
            Timestamp,Level,Message
            2024-03-15 10:00:00,INFO,Starting application
            2024-03-15 10:00:01,DEBUG,Loading config
            2024-03-15 10:00:02,WARN,Config missing
            2024-03-15 10:00:03,INFO,Using defaults
            2024-03-15 10:00:04,INFO,App started
            2024-03-15 10:00:05,ERROR,Connection failed
            2024-03-15 10:00:06,INFO,Retrying
            2024-03-15 10:00:07,INFO,Connected
            2024-03-15 10:00:08,DEBUG,Handshake
            2024-03-15 10:00:09,INFO,Ready
            """;

        // Detect
        using var detectStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));
        var detector = new CsvFormatDetector();
        var detection = detector.Probe(detectStream, "test.csv");
        detection.Should().NotBeNull();

        var boundary = (CsvBoundary)detection!.Boundary;

        // Read records
        using var readReader = new CsvRecordReader(new StringReader(csv), boundary);
        var rows = new List<LogRow>();
        long idx = 0;
        while (readReader.TryReadNext(out var rec))
        {
            if (detection.Extractor.TryExtract(rec, out var ts))
            {
                rows.Add(new LogRow(
                    TimestampUtc: ts.ToUniversalTime(),
                    TimestampBasis: TimeBasis.Local,
                    TimestampEffectiveOffsetMinutes: (int)ts.Offset.TotalMinutes,
                    TimestampOriginal: rec.Fields?["Timestamp"],
                    LogicalSourceId: "test",
                    SourcePath: "test.csv",
                    PhysicalFileId: "csv001",
                    SegmentId: "seg001",
                    IngestRunId: "run001",
                    RecordIndex: idx++,
                    Level: rec.Fields?["Level"],
                    Message: rec.Fields?["Message"] ?? rec.FullText,
                    FieldsJson: null));
            }
        }

        rows.Should().HaveCount(10);

        // Insert into DuckDB
        var tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_csv_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);
        try
        {
            var dbPath = Path.Combine(tempDir, "test.duckdb");
            using var factory = new DuckLakeConnectionFactory(dbPath);
            var conn = await factory.GetConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);

            var inserter = new LogBatchInserter(conn);
            await inserter.InsertBatchAsync(rows);

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM logs";
            var count = await cmd.ExecuteScalarAsync();
            Convert.ToInt64(count).Should().Be(10);
        }
        finally
        {
            try { Directory.Delete(tempDir, recursive: true); } catch { }
        }
    }
}
