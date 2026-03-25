using DuckDB.NET.Data;

using FluentAssertions;

using ItomoriLog.Core.Export;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Tests.Export;

public class ExportServiceTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public ExportServiceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_export_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    private async Task<DuckDBConnection> SetupDbWithRowsAsync(int rowCount = 10)
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var inserter = new LogBatchInserter(conn);
        var baseTime = new DateTime(2024, 3, 15, 10, 0, 0, DateTimeKind.Utc);
        var rows = Enumerable.Range(0, rowCount).Select(i => new LogRow(
            TimestampUtc: new DateTimeOffset(baseTime.AddSeconds(i), TimeSpan.Zero),
            TimestampBasis: TimeBasis.Utc,
            TimestampEffectiveOffsetMinutes: 0,
            TimestampOriginal: $"{baseTime.AddSeconds(i):yyyy-MM-dd HH:mm:ss.fff}",
            LogicalSourceId: i % 2 == 0 ? "source-a" : "source-b",
            SourcePath: "/var/log/test.log",
            PhysicalFileId: "phys001",
            SegmentId: "seg001",
            IngestRunId: "run001",
            RecordIndex: i,
            Level: i % 3 == 0 ? "ERROR" : "INFO",
            Message: $"Test message {i}",
            FieldsJson: i % 4 == 0 ? """{"key":"value"}""" : null)).ToList();
        await inserter.InsertBatchAsync(rows);

        return conn;
    }

    [Fact]
    public async Task ExportCsv_WritesHeaderAndAllRows()
    {
        var conn = await SetupDbWithRowsAsync(10);
        var service = new ExportService(conn);
        var outputPath = Path.Combine(_tempDir, "export.csv");

        var count = await service.ExportAsync(new ExportOptions(ExportFormat.Csv, outputPath));

        count.Should().Be(10);
        File.Exists(outputPath).Should().BeTrue();

        var lines = await File.ReadAllLinesAsync(outputPath);
        lines.Length.Should().Be(11); // 1 header + 10 data rows
        lines[0].Should().StartWith("timestamp_utc,timestamp_basis,");
        lines[0].Should().Contain("message,fields");

        var metadataPath = ExportService.BuildCsvMetadataPath(outputPath);
        File.Exists(metadataPath).Should().BeTrue();
    }

    [Fact]
    public async Task ExportCsv_EscapesCommasAndQuotes()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var inserter = new LogBatchInserter(conn);
        var rows = new List<LogRow>
        {
            new(
                TimestampUtc: new DateTimeOffset(2024, 3, 15, 10, 0, 0, TimeSpan.Zero),
                TimestampBasis: TimeBasis.Utc,
                TimestampEffectiveOffsetMinutes: 0,
                TimestampOriginal: "2024-03-15T10:00:00",
                LogicalSourceId: "test",
                SourcePath: "/test.log",
                PhysicalFileId: "phys",
                SegmentId: "seg",
                IngestRunId: "run",
                RecordIndex: 0,
                Level: "INFO",
                Message: "Has a comma, and \"quotes\"",
                FieldsJson: null)
        };
        await inserter.InsertBatchAsync(rows);

        var service = new ExportService(conn);
        var outputPath = Path.Combine(_tempDir, "export_escape.csv");

        await service.ExportAsync(new ExportOptions(ExportFormat.Csv, outputPath));

        var content = await File.ReadAllTextAsync(outputPath);
        // Message with comma and quotes should be properly escaped
        content.Should().Contain("\"Has a comma, and \"\"quotes\"\"\"");
    }

    [Fact]
    public async Task ExportJsonLines_WritesOneObjectPerLine()
    {
        var conn = await SetupDbWithRowsAsync(5);
        var service = new ExportService(conn);
        var outputPath = Path.Combine(_tempDir, "export.jsonl");

        var count = await service.ExportAsync(new ExportOptions(ExportFormat.JsonLines, outputPath));

        count.Should().Be(5);
        File.Exists(outputPath).Should().BeTrue();

        var lines = (await File.ReadAllLinesAsync(outputPath))
            .Where(l => !string.IsNullOrWhiteSpace(l)).ToArray();
        lines.Length.Should().Be(5);

        // Each line should be valid JSON
        foreach (var line in lines) {
            var doc = System.Text.Json.JsonDocument.Parse(line);
            doc.RootElement.GetProperty("message").GetString().Should().StartWith("Test message");
            doc.RootElement.GetProperty("logical_source_id").GetString().Should().NotBeNullOrEmpty();
        }
    }

    [Fact]
    public async Task ExportJsonLines_IncludesAllFields()
    {
        var conn = await SetupDbWithRowsAsync(1);
        var service = new ExportService(conn);
        var outputPath = Path.Combine(_tempDir, "export_fields.jsonl");

        await service.ExportAsync(new ExportOptions(ExportFormat.JsonLines, outputPath));

        var line = (await File.ReadAllLinesAsync(outputPath)).First(l => !string.IsNullOrWhiteSpace(l));
        var doc = System.Text.Json.JsonDocument.Parse(line);
        var root = doc.RootElement;

        root.TryGetProperty("timestamp_utc", out _).Should().BeTrue();
        root.TryGetProperty("timestamp_basis", out _).Should().BeTrue();
        root.TryGetProperty("timestamp_effective_offset_minutes", out _).Should().BeTrue();
        root.TryGetProperty("logical_source_id", out _).Should().BeTrue();
        root.TryGetProperty("segment_id", out _).Should().BeTrue();
        root.TryGetProperty("record_index", out _).Should().BeTrue();
        root.TryGetProperty("level", out _).Should().BeTrue();
        root.TryGetProperty("message", out _).Should().BeTrue();
    }

    [Fact]
    public async Task ExportParquet_CreatesFile()
    {
        var conn = await SetupDbWithRowsAsync(10);
        var service = new ExportService(conn);
        var outputPath = Path.Combine(_tempDir, "export.parquet");

        var count = await service.ExportAsync(new ExportOptions(ExportFormat.Parquet, outputPath));

        count.Should().Be(10);
        File.Exists(outputPath).Should().BeTrue();
        new FileInfo(outputPath).Length.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task ExportParquet_CanBeReadBack()
    {
        var conn = await SetupDbWithRowsAsync(10);
        var service = new ExportService(conn);
        var outputPath = Path.Combine(_tempDir, "export_readback.parquet");
        var escapedPath = outputPath.Replace("\\", "/");

        await service.ExportAsync(new ExportOptions(ExportFormat.Parquet, outputPath));

        // Read back and count rows
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM read_parquet('{escapedPath}')";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        count.Should().Be(10);
    }

    [Fact]
    public async Task ExportCsv_WithFilter_ExportsSubset()
    {
        var conn = await SetupDbWithRowsAsync(10);
        var service = new ExportService(conn);
        var outputPath = Path.Combine(_tempDir, "export_filtered.csv");

        var filter = new FilterState { Levels = ["ERROR"] };
        var count = await service.ExportAsync(
            new ExportOptions(ExportFormat.Csv, outputPath, filter));

        // Records 0, 3, 6, 9 have level ERROR (i % 3 == 0)
        count.Should().Be(4);

        var lines = await File.ReadAllLinesAsync(outputPath);
        lines.Length.Should().Be(5); // 1 header + 4 data rows
    }

    [Fact]
    public async Task ExportJsonLines_WithFilter_ExportsSubset()
    {
        var conn = await SetupDbWithRowsAsync(10);
        var service = new ExportService(conn);
        var outputPath = Path.Combine(_tempDir, "export_filtered.jsonl");

        var filter = new FilterState { SourceIds = ["source-a"] };
        var count = await service.ExportAsync(
            new ExportOptions(ExportFormat.JsonLines, outputPath, filter));

        // Records 0, 2, 4, 6, 8 have source-a (i % 2 == 0)
        count.Should().Be(5);
    }

    [Fact]
    public async Task ExportCsv_WithTextSearch_FiltersMessages()
    {
        var conn = await SetupDbWithRowsAsync(20);
        var service = new ExportService(conn);
        var outputPath = Path.Combine(_tempDir, "export_text.csv");

        var filter = new FilterState { TextSearch = "message 1" };
        var count = await service.ExportAsync(
            new ExportOptions(ExportFormat.Csv, outputPath, filter));

        // "message 1" matches: 1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
        count.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task ExportCsv_ReportsProgress()
    {
        var conn = await SetupDbWithRowsAsync(5);
        var service = new ExportService(conn);
        var outputPath = Path.Combine(_tempDir, "export_progress.csv");

        var progressReports = new List<ExportProgress>();
        var progress = new Progress<ExportProgress>(p => progressReports.Add(p));

        await service.ExportAsync(new ExportOptions(ExportFormat.Csv, outputPath), progress);

        // Wait briefly for Progress<T> callbacks to be delivered
        await Task.Delay(100);

        progressReports.Should().NotBeEmpty();
        progressReports.Last().Status.Should().Contain("complete");
    }

    [Fact]
    public async Task ExportCsv_FullSession_IgnoresFilter()
    {
        var conn = await SetupDbWithRowsAsync(12);
        var service = new ExportService(conn);
        var outputPath = Path.Combine(_tempDir, "export_full_scope.csv");

        var filter = new FilterState { Levels = ["ERROR"] };
        var count = await service.ExportAsync(
            new ExportOptions(ExportFormat.Csv, outputPath, filter, Scope: ExportScope.FullSession));

        count.Should().Be(12);
    }

    [Fact]
    public async Task ExportParquet_WritesSessionMetadata()
    {
        var conn = await SetupDbWithRowsAsync(3);
        var service = new ExportService(conn);
        var outputPath = Path.Combine(_tempDir, "export_kv.parquet");
        var escapedPath = outputPath.Replace("\\", "/");

        await service.ExportAsync(new ExportOptions(
            ExportFormat.Parquet,
            outputPath,
            Scope: ExportScope.CurrentView,
            SessionTitle: "Session X",
            SessionDescription: "Desc",
            SessionFolder: "C:\\sessions\\x"));

        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT CAST("key" AS VARCHAR), CAST("value" AS VARCHAR)
            FROM parquet_kv_metadata('{escapedPath}')
            WHERE "key" IS NOT NULL
            """;
        using var reader = await cmd.ExecuteReaderAsync();
        var kv = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        while (await reader.ReadAsync()) {
            var key = reader.GetString(0);
            var value = reader.GetString(1);
            kv[key] = value;
        }

        kv.Should().ContainKey("session_title");
        kv["session_title"].Should().Be("Session X");
        kv.Should().ContainKey("session_folder");
        kv.Should().ContainKey("export_scope");
    }

    [Fact]
    public async Task ExportCsv_EmptyTable_WritesHeaderOnly()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var service = new ExportService(conn);
        var outputPath = Path.Combine(_tempDir, "export_empty.csv");

        var count = await service.ExportAsync(new ExportOptions(ExportFormat.Csv, outputPath));

        count.Should().Be(0);
        var lines = await File.ReadAllLinesAsync(outputPath);
        lines.Length.Should().Be(1); // header only
    }

    [Fact]
    public void CsvEscape_HandlesSpecialCharacters()
    {
        ExportService.CsvEscape("hello").Should().Be("hello");
        ExportService.CsvEscape("hello,world").Should().Be("\"hello,world\"");
        ExportService.CsvEscape("say \"hi\"").Should().Be("\"say \"\"hi\"\"\"");
        ExportService.CsvEscape("line1\nline2").Should().Be("\"line1\nline2\"");
        ExportService.CsvEscape("").Should().Be("");
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
