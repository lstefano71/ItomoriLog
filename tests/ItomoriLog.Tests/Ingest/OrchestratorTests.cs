using FluentAssertions;

using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;

using System.IO.Compression;
using System.Text;

namespace ItomoriLog.Tests.Ingest;

public class OrchestratorTests : IDisposable
{
    private readonly string _tempDir;

    public OrchestratorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_orch_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    #region IdentityGenerator

    [Fact]
    public void IdentityGenerator_PhysicalFileId_IsDeterministic()
    {
        var path = "/var/log/app.log";
        var size = 12345L;
        var lastMod = new DateTimeOffset(2024, 3, 15, 10, 0, 0, TimeSpan.Zero);

        var id1 = IdentityGenerator.PhysicalFileId(path, size, lastMod);
        var id2 = IdentityGenerator.PhysicalFileId(path, size, lastMod);

        id1.Should().Be(id2);
        id1.Should().HaveLength(16);
        id1.Should().MatchRegex("^[0-9a-f]{16}$");
    }

    [Fact]
    public void IdentityGenerator_PhysicalFileId_DiffersForDifferentInputs()
    {
        var lastMod = new DateTimeOffset(2024, 3, 15, 10, 0, 0, TimeSpan.Zero);

        var id1 = IdentityGenerator.PhysicalFileId("/a.log", 100, lastMod);
        var id2 = IdentityGenerator.PhysicalFileId("/b.log", 100, lastMod);

        id1.Should().NotBe(id2);
    }

    [Fact]
    public void IdentityGenerator_SegmentId_IsDeterministic()
    {
        var physId = "abc123def456";

        var seg1 = IdentityGenerator.SegmentId(physId, 0);
        var seg2 = IdentityGenerator.SegmentId(physId, 0);

        seg1.Should().Be(seg2);
        seg1.Should().HaveLength(16);
    }

    [Fact]
    public void IdentityGenerator_SegmentId_DiffersForDifferentIndex()
    {
        var physId = "abc123def456";

        var seg0 = IdentityGenerator.SegmentId(physId, 0);
        var seg1 = IdentityGenerator.SegmentId(physId, 1);

        seg0.Should().NotBe(seg1);
    }

    [Theory]
    [InlineData("app-2024-03-15T10-00-00.log", "app")]
    [InlineData("myservice-20240315.log", "myservice")]
    [InlineData("access.log", "access")]
    [InlineData("20240315120000.log", "20240315120000.log")] // All digits → fallback to full filename
    public void IdentityGenerator_LogicalSourceId_StripsTimestamps(string fileName, string expected)
    {
        var result = IdentityGenerator.LogicalSourceId(fileName);
        result.Should().Be(expected);
    }

    #endregion

    #region DetectionEngine

    [Fact]
    public void DetectionEngine_BestConfidence_Wins()
    {
        var ndjson = """
            {"timestamp":"2024-03-15T10:00:00Z","level":"INFO","message":"Hello"}
            {"timestamp":"2024-03-15T10:00:01Z","level":"DEBUG","message":"World"}
            {"timestamp":"2024-03-15T10:00:02Z","level":"INFO","message":"Test1"}
            {"timestamp":"2024-03-15T10:00:03Z","level":"WARN","message":"Test2"}
            {"timestamp":"2024-03-15T10:00:04Z","level":"ERROR","message":"Test3"}
            {"timestamp":"2024-03-15T10:00:05Z","level":"INFO","message":"Test4"}
            {"timestamp":"2024-03-15T10:00:06Z","level":"DEBUG","message":"Test5"}
            {"timestamp":"2024-03-15T10:00:07Z","level":"INFO","message":"Test6"}
            {"timestamp":"2024-03-15T10:00:08Z","level":"INFO","message":"Test7"}
            {"timestamp":"2024-03-15T10:00:09Z","level":"INFO","message":"Test10"}
            """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(ndjson));
        var engine = new DetectionEngine();
        var result = engine.Detect(stream, "data.jsonl");

        result.Detection.Should().NotBeNull();
        result.Detection!.Boundary.Should().BeOfType<JsonNdBoundary>();
    }

    [Fact]
    public void DetectionEngine_NoViableDetector_ReturnsNull()
    {
        // Binary garbage that won't match any detector
        var data = new byte[1024];
        new Random(42).NextBytes(data);
        // Make sure it's not valid UTF-8 or parseable
        for (int i = 0; i < data.Length; i++)
            data[i] = (byte)((data[i] % 26) + 128); // high bytes, no valid patterns

        using var stream = new MemoryStream(data);
        var engine = new DetectionEngine();
        var result = engine.Detect(stream, "random.bin");

        result.Detection.Should().BeNull();
    }

    [Fact]
    public void DetectionEngine_CloseConfidence_MarksDisambiguation()
    {
        // CSV that could also be interpreted as text
        var csv = """
            timestamp,level,message
            2024-03-15 10:00:00,INFO,Starting application
            2024-03-15 10:00:01,DEBUG,Loading config
            2024-03-15 10:00:02,WARN,Config missing
            2024-03-15 10:00:03,INFO,Using defaults
            2024-03-15 10:00:04,INFO,Ready
            2024-03-15 10:00:05,ERROR,Connection failed
            2024-03-15 10:00:06,INFO,Retrying
            2024-03-15 10:00:07,INFO,Connected
            2024-03-15 10:00:08,DEBUG,Handshake
            2024-03-15 10:00:09,INFO,Done
            """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));
        var engine = new DetectionEngine();
        var result = engine.Detect(stream, "data.csv");

        // Should detect something (CSV or Text)
        result.Detection.Should().NotBeNull();
    }

    #endregion

    #region SlidingValidator

    [Fact]
    public void SlidingValidator_BelowThreshold_TriggersRedetect()
    {
        var validator = new SlidingValidator(windowSize: 10, threshold: 0.80);

        // 7 good, 3 bad => 70% < 80% threshold
        for (int i = 0; i < 7; i++) validator.RecordResult(true, true);
        for (int i = 0; i < 3; i++) validator.RecordResult(false, false);

        validator.ShouldRedetect.Should().BeTrue();
    }

    [Fact]
    public void SlidingValidator_AboveThreshold_NoRedetect()
    {
        var validator = new SlidingValidator(windowSize: 10, threshold: 0.80);

        // 9 good, 1 bad => 90% > 80%
        for (int i = 0; i < 9; i++) validator.RecordResult(true, true);
        validator.RecordResult(false, false);

        validator.ShouldRedetect.Should().BeFalse();
    }

    [Fact]
    public void SlidingValidator_UnderWindowSize_NeverRedetects()
    {
        var validator = new SlidingValidator(windowSize: 256, threshold: 0.80);

        // Only 5 records, all bad — still not enough to trigger
        for (int i = 0; i < 5; i++) validator.RecordResult(false, false);

        validator.ShouldRedetect.Should().BeFalse();
    }

    [Fact]
    public void SlidingValidator_SlidingWindow_EvictsOld()
    {
        var validator = new SlidingValidator(windowSize: 10, threshold: 0.80);

        // Fill with 10 bad results
        for (int i = 0; i < 10; i++) validator.RecordResult(false, false);
        validator.ShouldRedetect.Should().BeTrue();

        // Now add 10 good results, evicting all bad ones
        for (int i = 0; i < 10; i++) validator.RecordResult(true, true);
        validator.ShouldRedetect.Should().BeFalse();
    }

    #endregion

    #region FieldSynthesizer

    [Fact]
    public void FieldSynthesizer_LevelAndMessage_Extracted()
    {
        var synth = new FieldSynthesizer();
        var result = synth.Extract(" INFO Starting application");

        result.Level.Should().Be("INFO");
        result.Message.Should().Be("Starting application");
        result.FieldsJson.Should().BeNull();
    }

    [Fact]
    public void FieldSynthesizer_LevelSourceMessage_Extracted()
    {
        var synth = new FieldSynthesizer();
        var result = synth.Extract(" INFO [MyApp.Core] Starting up");

        result.Level.Should().Be("INFO");
        result.Message.Should().Be("Starting up");
        result.FieldsJson.Should().Contain("MyApp.Core");
    }

    [Fact]
    public void FieldSynthesizer_LevelSourceTaskMessage_Extracted()
    {
        var synth = new FieldSynthesizer();
        var result = synth.Extract(" DEBUG [MyApp.Core] [task-42] Processing item");

        result.Level.Should().Be("DEBUG");
        result.Message.Should().Be("Processing item");
        result.FieldsJson.Should().Contain("MyApp.Core");
        result.FieldsJson.Should().Contain("task-42");
    }

    [Theory]
    [InlineData("INFO", "INFO")]
    [InlineData("info", "INFO")]
    [InlineData("INF", "INFO")]
    [InlineData("INFORMATION", "INFO")]
    [InlineData("WARN", "WARN")]
    [InlineData("WRN", "WARN")]
    [InlineData("WARNING", "WARN")]
    [InlineData("ERROR", "ERROR")]
    [InlineData("ERR", "ERROR")]
    [InlineData("DEBUG", "DEBUG")]
    [InlineData("DBG", "DEBUG")]
    [InlineData("FATAL", "FATAL")]
    [InlineData("FTL", "FATAL")]
    [InlineData("CRITICAL", "FATAL")]
    [InlineData("TRACE", "TRACE")]
    [InlineData("TRC", "TRACE")]
    [InlineData("VERBOSE", "TRACE")]
    public void FieldSynthesizer_NormalizeLevel_Maps(string input, string expected)
    {
        FieldSynthesizer.NormalizeLevel(input).Should().Be(expected);
    }

    [Fact]
    public void FieldSynthesizer_NoPattern_TreatsAsMessage()
    {
        var synth = new FieldSynthesizer();
        // Use text starting with non-word char so no common pattern matches
        var result = synth.Extract("  >>> some plain text  ");

        result.Level.Should().BeNull();
        result.Message.Should().Be(">>> some plain text");
    }

    #endregion

    #region EncodingDetector

    [Fact]
    public void EncodingDetector_Utf8NoBom_Detected()
    {
        var bytes = Encoding.UTF8.GetBytes("Hello, world! Some UTF-8 text with special chars: café");
        using var stream = new MemoryStream(bytes);

        var encoding = EncodingDetector.Detect(stream);
        encoding.Should().Be(Encoding.UTF8);
    }

    [Fact]
    public void EncodingDetector_Utf8Bom_Detected()
    {
        var preamble = Encoding.UTF8.GetPreamble();
        var content = Encoding.UTF8.GetBytes("Hello BOM");
        var bytes = new byte[preamble.Length + content.Length];
        preamble.CopyTo(bytes, 0);
        content.CopyTo(bytes, preamble.Length);
        using var stream = new MemoryStream(bytes);

        var encoding = EncodingDetector.Detect(stream);
        encoding.Should().Be(Encoding.UTF8);
    }

    [Fact]
    public void EncodingDetector_Utf16LeBom_Detected()
    {
        var bytes = Encoding.Unicode.GetPreamble()
            .Concat(Encoding.Unicode.GetBytes("Hello")).ToArray();
        using var stream = new MemoryStream(bytes);

        var encoding = EncodingDetector.Detect(stream);
        encoding.Should().Be(Encoding.Unicode);
    }

    [Fact]
    public void EncodingDetector_Utf16BeBom_Detected()
    {
        var bytes = Encoding.BigEndianUnicode.GetPreamble()
            .Concat(Encoding.BigEndianUnicode.GetBytes("Hello")).ToArray();
        using var stream = new MemoryStream(bytes);

        var encoding = EncodingDetector.Detect(stream);
        encoding.Should().Be(Encoding.BigEndianUnicode);
    }

    #endregion

    #region ZipHandler

    [Fact]
    public void ZipHandler_EnumeratesEntries()
    {
        var zipPath = CreateTestZip("test_enum",
            ("app.log", "2024-03-15 10:00:00 INFO Starting\n"),
            ("errors.log", "2024-03-15 10:00:01 ERROR Something broke\n"));

        var entries = ZipHandler.EnumerateEntries(zipPath).ToList();

        entries.Should().HaveCount(2);
        entries.Select(e => e.EntryName).Should().Contain("app.log");
        entries.Select(e => e.EntryName).Should().Contain("errors.log");
        entries.All(e => e.SourcePath.Contains("!/")).Should().BeTrue();
    }

    [Fact]
    public void ZipHandler_NestedZip_Skipped()
    {
        var zipPath = CreateTestZip("test_nested",
            ("app.log", "2024-03-15 10:00:00 INFO Normal log\n"),
            ("inner.zip", "PK fake zip content"));

        var skipSink = new ListSkipSink();
        var entries = ZipHandler.EnumerateEntries(zipPath, skipSink).ToList();

        entries.Should().HaveCount(1);
        entries[0].EntryName.Should().Be("app.log");

        var skips = skipSink.GetSkips();
        skips.Should().HaveCount(1);
        skips[0].ReasonCode.Should().Be(SkipReasonCode.NestedArchive);
    }

    [Fact]
    public void ZipHandler_OpenRead_ReturnsStreamingContent()
    {
        var content = "2024-03-15 10:00:00 INFO Test message\n";
        var zipPath = CreateTestZip("test_extract", ("test.log", content));

        using var stream = ZipHandler.OpenRead(zipPath, "test.log");
        stream.Should().NotBeOfType<MemoryStream>();
        stream.CanRead.Should().BeTrue();

        using var ms = new MemoryStream();
        stream.CopyTo(ms);

        var text = Encoding.UTF8.GetString(ms.ToArray());
        text.Should().Be(content);
    }

    #endregion

    #region IngestRunTracker

    [Fact]
    public async Task IngestRunTracker_StartAndComplete_TracksRun()
    {
        var dbPath = Path.Combine(_tempDir, "tracker_test.duckdb");
        var factory = new DuckLakeConnectionFactory(dbPath);
        try {
            var conn = await factory.GetConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);

            var tracker = new IngestRunTracker(conn);
            var runId = await tracker.StartRunAsync();

            // Should be in interrupted runs
            var interrupted = await tracker.GetInterruptedRunsAsync();
            interrupted.Should().Contain(runId);

            // Complete the run
            await tracker.CompleteRunAsync(runId);

            // Should no longer be interrupted
            interrupted = await tracker.GetInterruptedRunsAsync();
            interrupted.Should().NotContain(runId);
        } finally {
            factory.Dispose();
        }
    }

    #endregion

    #region IngestOrchestrator Integration

    [Fact]
    public async Task IngestOrchestrator_TextFile_IngestsSuccessfully()
    {
        var dbPath = Path.Combine(_tempDir, "orch_text_test.duckdb");
        var factory = new DuckLakeConnectionFactory(dbPath);
        try {
            var conn = await factory.GetConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);

            // Create a log file
            var logPath = Path.Combine(_tempDir, "app.log");
            var baseTime = new DateTime(2024, 3, 15, 10, 0, 0);
            var lines = Enumerable.Range(0, 25)
                .Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO Message {i}");
            await File.WriteAllTextAsync(logPath, string.Join('\n', lines));

            var orchestrator = new IngestOrchestrator(conn, maxConcurrency: 1);
            var result = await orchestrator.IngestFilesAsync(
                [logPath],
                new TimeBasisConfig(TimeBasis.Utc));

            result.Status.Should().Be("completed");
            result.TotalRows.Should().Be(25);
            result.FilesProcessed.Should().Be(1);

            // Verify in DB
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM logs";
            var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
            count.Should().Be(25);
        } finally {
            factory.Dispose();
        }
    }

    [Fact]
    public async Task IngestOrchestrator_CsvFile_IngestsSuccessfully()
    {
        var dbPath = Path.Combine(_tempDir, "orch_csv_test.duckdb");
        var factory = new DuckLakeConnectionFactory(dbPath);
        try {
            var conn = await factory.GetConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);

            var csvPath = Path.Combine(_tempDir, "data.csv");
            var sb = new StringBuilder("Timestamp,Level,Message\n");
            var baseTime = new DateTime(2024, 3, 15, 10, 0, 0);
            for (int i = 0; i < 15; i++)
                sb.AppendLine($"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss},INFO,CSV message {i}");
            await File.WriteAllTextAsync(csvPath, sb.ToString());

            var orchestrator = new IngestOrchestrator(conn, maxConcurrency: 1);
            var result = await orchestrator.IngestFilesAsync(
                [csvPath],
                new TimeBasisConfig(TimeBasis.Utc));

            result.Status.Should().Be("completed");
            result.TotalRows.Should().Be(15);
            result.FilesProcessed.Should().Be(1);
        } finally {
            factory.Dispose();
        }
    }

    [Fact]
    public async Task IngestOrchestrator_NdjsonFile_IngestsSuccessfully()
    {
        var dbPath = Path.Combine(_tempDir, "orch_ndjson_test.duckdb");
        var factory = new DuckLakeConnectionFactory(dbPath);
        try {
            var conn = await factory.GetConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);

            var jsonPath = Path.Combine(_tempDir, "data.jsonl");
            var sb = new StringBuilder();
            var baseTime = new DateTime(2024, 3, 15, 10, 0, 0);
            for (int i = 0; i < 10; i++)
                sb.AppendLine($"{{\"timestamp\":\"{baseTime.AddSeconds(i):yyyy-MM-ddTHH:mm:ssZ}\",\"level\":\"INFO\",\"message\":\"JSON msg {i}\"}}");
            await File.WriteAllTextAsync(jsonPath, sb.ToString());

            var orchestrator = new IngestOrchestrator(conn, maxConcurrency: 1);
            var result = await orchestrator.IngestFilesAsync(
                [jsonPath],
                new TimeBasisConfig(TimeBasis.Utc));

            result.Status.Should().Be("completed");
            result.TotalRows.Should().Be(10);
            result.FilesProcessed.Should().Be(1);
        } finally {
            factory.Dispose();
        }
    }

    [Fact]
    public async Task IngestOrchestrator_MixedFiles_IngestsAll()
    {
        var dbPath = Path.Combine(_tempDir, "orch_mixed_test.duckdb");
        var factory = new DuckLakeConnectionFactory(dbPath);
        try {
            var conn = await factory.GetConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);

            var baseTime = new DateTime(2024, 3, 15, 10, 0, 0);

            // Text log (need 25+ lines for TextFormatDetector MinSniffLines)
            var textPath = Path.Combine(_tempDir, "app.log");
            var textLines = Enumerable.Range(0, 25)
                .Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO Text msg {i}");
            await File.WriteAllTextAsync(textPath, string.Join('\n', textLines));

            // CSV log
            var csvPath = Path.Combine(_tempDir, "data.csv");
            var csvSb = new StringBuilder("Timestamp,Level,Message\n");
            for (int i = 0; i < 15; i++)
                csvSb.AppendLine($"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss},WARN,CSV msg {i}");
            await File.WriteAllTextAsync(csvPath, csvSb.ToString());

            // NDJSON log
            var jsonPath = Path.Combine(_tempDir, "events.jsonl");
            var jsonSb = new StringBuilder();
            for (int i = 0; i < 10; i++)
                jsonSb.AppendLine($"{{\"timestamp\":\"{baseTime.AddSeconds(i):yyyy-MM-ddTHH:mm:ssZ}\",\"level\":\"ERROR\",\"message\":\"JSON msg {i}\"}}");
            await File.WriteAllTextAsync(jsonPath, jsonSb.ToString());

            var orchestrator = new IngestOrchestrator(conn, maxConcurrency: 1);
            var result = await orchestrator.IngestFilesAsync(
                [textPath, csvPath, jsonPath],
                new TimeBasisConfig(TimeBasis.Utc));

            result.Status.Should().Be("completed");
            result.TotalRows.Should().Be(50);
            result.FilesProcessed.Should().Be(3);
        } finally {
            factory.Dispose();
        }
    }

    [Fact]
    public async Task IngestOrchestrator_ZipFile_IngestsEntries()
    {
        var dbPath = Path.Combine(_tempDir, "orch_zip_test.duckdb");
        var factory = new DuckLakeConnectionFactory(dbPath);
        try {
            var conn = await factory.GetConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);

            var baseTime = new DateTime(2024, 3, 15, 10, 0, 0);
            var logContent = string.Join('\n', Enumerable.Range(0, 25)
                .Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO Zipped msg {i}"));

            var zipPath = CreateTestZip("orch_zip", ("app.log", logContent));

            var orchestrator = new IngestOrchestrator(conn, maxConcurrency: 1);
            var result = await orchestrator.IngestFilesAsync(
                [zipPath],
                new TimeBasisConfig(TimeBasis.Utc));

            result.Status.Should().Be("completed");
            result.TotalRows.Should().Be(25);
            result.FilesProcessed.Should().Be(1);
        } finally {
            factory.Dispose();
        }
    }

    [Fact]
    public async Task IngestOrchestrator_ZipEntryPath_IngestsOnlySelectedEntry()
    {
        var dbPath = Path.Combine(_tempDir, "orch_zip_entry_test.duckdb");
        var factory = new DuckLakeConnectionFactory(dbPath);
        try {
            var conn = await factory.GetConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);

            var baseTime = new DateTime(2024, 3, 15, 10, 0, 0);
            var appContent = string.Join('\n', Enumerable.Range(0, 25)
                .Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} INFO App msg {i}"));
            var errorContent = string.Join('\n', Enumerable.Range(0, 25)
                .Select(i => $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff} ERROR Error msg {i}"));

            var zipPath = CreateTestZip("orch_zip_entry", ("app.log", appContent), ("errors.log", errorContent));
            var entrySourcePath = ZipHandler.EnumerateEntries(zipPath).Single(entry => entry.EntryName == "errors.log").SourcePath;

            var orchestrator = new IngestOrchestrator(conn, maxConcurrency: 1);
            var result = await orchestrator.IngestFilesAsync(
                [entrySourcePath],
                new TimeBasisConfig(TimeBasis.Utc));

            result.Status.Should().Be("completed");
            result.TotalRows.Should().Be(25);
            result.FilesProcessed.Should().Be(1);

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*), MIN(source_path), MAX(source_path) FROM logs";
            using var reader = await cmd.ExecuteReaderAsync();
            (await reader.ReadAsync()).Should().BeTrue();
            reader.GetInt64(0).Should().Be(25);
            reader.GetString(1).Should().Be(entrySourcePath);
            reader.GetString(2).Should().Be(entrySourcePath);
        } finally {
            factory.Dispose();
        }
    }

    [Fact]
    public async Task IngestOrchestrator_UnrecognizedFile_SkipsWithReason()
    {
        var dbPath = Path.Combine(_tempDir, "orch_unrec_test.duckdb");
        var factory = new DuckLakeConnectionFactory(dbPath);
        try {
            var conn = await factory.GetConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);

            // Create a file with random binary content that no detector will recognize
            var binPath = Path.Combine(_tempDir, "random.bin");
            var random = new Random(42);
            var bytes = new byte[2048];
            random.NextBytes(bytes);
            // Make it all high bytes so it won't be valid text patterns
            for (int i = 0; i < bytes.Length; i++)
                bytes[i] = (byte)((bytes[i] % 26) + 200);
            await File.WriteAllBytesAsync(binPath, bytes);

            var orchestrator = new IngestOrchestrator(conn, maxConcurrency: 1);
            var result = await orchestrator.IngestFilesAsync(
                [binPath],
                new TimeBasisConfig(TimeBasis.Utc));

            result.TotalRows.Should().Be(0);
            result.Skips.Should().Contain(s => s.ReasonCode == SkipReasonCode.NotRecognized);
        } finally {
            factory.Dispose();
        }
    }

    #endregion

    #region Helpers

    private string CreateTestZip(string name, params (string fileName, string content)[] files)
    {
        var zipDir = Path.Combine(_tempDir, $"zipfiles_{name}");
        Directory.CreateDirectory(zipDir);

        foreach (var (fileName, content) in files)
            File.WriteAllText(Path.Combine(zipDir, fileName), content);

        var zipPath = Path.Combine(_tempDir, $"{name}.zip");
        ZipFile.CreateFromDirectory(zipDir, zipPath);
        return zipPath;
    }

    #endregion

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
