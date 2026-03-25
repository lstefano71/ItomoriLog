using DuckDB.NET.Data;

using FluentAssertions;

using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Tests.Storage;

public class StorageTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public StorageTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, SessionPaths.DefaultDbFileName);
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    [Fact]
    public async Task SchemaInitializer_CreatesAllTables()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_catalog = current_database() AND table_schema = 'main'
            ORDER BY table_name
            """;
        using var reader = await cmd.ExecuteReaderAsync();

        var tables = new List<string>();
        while (await reader.ReadAsync())
            tables.Add(reader.GetString(0));

        tables.Should().Contain(["session", "logs", "segments", "skips", "rules", "ingest_runs"]);
    }

    [Fact]
    public async Task SchemaInitializer_IsIdempotent()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);
        await SchemaInitializer.EnsureSchemaAsync(conn); // second call should not throw

        File.Exists(_dbPath).Should().BeTrue();
        Directory.Exists(SessionPaths.GetDuckLakeDataPath(_tempDir)).Should().BeTrue();
    }

    [Fact]
    public async Task DuckLakeConnectionFactory_AppliesPreferredStorageOptions()
    {
        var conn = await _factory.GetConnectionAsync();

        var options = await ReadDuckLakeOptionsAsync(conn);

        options.Should().ContainKey("parquet_version");
        options["parquet_version"].Should().BeOneOf("2", "V2");
        options.Should().ContainKey("parquet_compression");
        options["parquet_compression"].Should().Be(DuckLakeSessionMaintenance.PreferredParquetCompression);
    }

    [Fact]
    public async Task SessionStore_InitializeAndReadHeader_RoundTrips()
    {
        var store = new SessionStore(_factory);
        await store.InitializeAsync("Test Session", "A test description", "Europe/Rome");

        var header = await store.ReadHeaderAsync();

        header.Should().NotBeNull();
        header!.Title.Should().Be("Test Session");
        header.Description.Should().Be("A test description");
        header.DefaultTimezone.Should().Be("Europe/Rome");
        header.AppVersion.Should().Contain("ItomoriLog");
    }

    [Fact]
    public async Task SessionStore_UpdateHeader_ChangesTitle()
    {
        var store = new SessionStore(_factory);
        await store.InitializeAsync("Original", null, null);
        await store.UpdateHeaderAsync(title: "Updated Title");

        var header = await store.ReadHeaderAsync();
        header!.Title.Should().Be("Updated Title");
    }

    [Fact]
    public void SessionPaths_CreateNew_CreatesFolder()
    {
        var sessionDir = SessionPaths.CreateNew(_tempDir, "My Test Session");

        Directory.Exists(sessionDir).Should().BeTrue();
        Directory.Exists(Path.Combine(sessionDir, "exports")).Should().BeTrue();
        Directory.Exists(SessionPaths.GetDuckLakeDataPath(sessionDir)).Should().BeTrue();
        Path.GetFileName(sessionDir).Should().Contain("My_Test_Session");
    }

    [Fact]
    public async Task QueryHistoryService_RecordQuery_WorksWithDuckLakeSchema()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);
        var service = new QueryHistoryService(conn);
        await service.EnsureSchemaAsync();

        await service.RecordQueryAsync("level = 'ERROR'", 42);

        var recent = await service.GetRecentAsync();
        recent.Should().ContainSingle();
        recent[0].Id.Should().Be(1);
        recent[0].QueryText.Should().Be("level = 'ERROR'");
        recent[0].ResultCount.Should().Be(42);
    }

    [Fact]
    public async Task DuckLakeConnectionFactory_SecondAttachmentToSameCatalog_FailsWhileFirstIsOpen()
    {
        var writerConnection = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(writerConnection);

        using var secondFactory = new DuckLakeConnectionFactory(_dbPath);
        var act = async () => await secondFactory.GetConnectionAsync();

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task DuckLakeConnectionFactory_NewCatalog_WritesParquetUnderSessionDataFolder()
    {
        var connection = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(connection);

        using (var cmd = connection.CreateCommand()) {
            cmd.CommandText = """
                INSERT INTO logs (
                    timestamp_utc, timestamp_basis, timestamp_effective_offset_minutes,
                    timestamp_original, logical_source_id, source_path,
                    physical_file_id, segment_id, ingest_run_id,
                    record_index, level, message, fields
                )
                SELECT
                    TIMESTAMP '2026-01-01 00:00:00' + (i * INTERVAL 1 SECOND),
                    'Utc',
                    0,
                    NULL,
                    'source-1',
                    'C:\logs\sample.log',
                    'file-1',
                    'segment-1',
                    'run-1',
                    i,
                    'INFO',
                    'message ' || CAST(i AS VARCHAR),
                    NULL
                FROM range(1000) AS t(i);
                CHECKPOINT;
                """;

            await cmd.ExecuteNonQueryAsync();
        }

        var dataPath = SessionPaths.GetDuckLakeDataPath(_tempDir);
        var parquetFiles = Directory.EnumerateFiles(dataPath, "*.parquet", SearchOption.AllDirectories).ToArray();

        parquetFiles.Should().NotBeEmpty();
        parquetFiles.Should().OnlyContain(path => Path.GetFullPath(path).StartsWith(dataPath, StringComparison.OrdinalIgnoreCase));
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    private static async Task<Dictionary<string, string?>> ReadDuckLakeOptionsAsync(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM itomori_session.options()";
        using var reader = await cmd.ExecuteReaderAsync();

        var options = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        while (await reader.ReadAsync()) {
            var optionName = TryGetReaderValue(reader, "option_name") ?? TryGetReaderValue(reader, "name");
            if (string.IsNullOrWhiteSpace(optionName))
                continue;

            var optionValue = TryGetReaderValue(reader, "option_value") ?? TryGetReaderValue(reader, "value");
            options[optionName] = optionValue;
        }

        return options;
    }

    private static string? TryGetReaderValue(System.Data.Common.DbDataReader reader, string columnName)
    {
        for (var i = 0; i < reader.FieldCount; i++) {
            if (!string.Equals(reader.GetName(i), columnName, StringComparison.OrdinalIgnoreCase))
                continue;

            return reader.IsDBNull(i) ? null : reader.GetValue(i)?.ToString();
        }

        return null;
    }
}

public class GlobalStoreTests : IDisposable
{
    private readonly string _tempDir;
    private readonly GlobalStore _store;

    public GlobalStoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_global_test_{Guid.NewGuid():N}");
        _store = new GlobalStore(_tempDir);
    }

    [Fact]
    public async Task AddAndGetRecentSessions_RoundTrips()
    {
        await _store.AddRecentSessionAsync("C:\\test\\session1", "Test Session", "A description");

        var sessions = await _store.GetRecentSessionsAsync();

        sessions.Should().ContainSingle();
        sessions[0].SessionFolder.Should().Be("C:\\test\\session1");
        sessions[0].Title.Should().Be("Test Session");
        sessions[0].Description.Should().Be("A description");
    }

    [Fact]
    public async Task Add_DuplicatePath_UpdatesExisting()
    {
        await _store.AddRecentSessionAsync("C:\\a", "First");
        await _store.AddRecentSessionAsync("C:\\b", "Second");
        await _store.AddRecentSessionAsync("C:\\a", "First Updated");

        var sessions = await _store.GetRecentSessionsAsync();

        sessions.Should().HaveCount(2);
        sessions.First(s => s.SessionFolder == "C:\\a").Title.Should().Be("First Updated");
    }

    [Fact]
    public async Task PruneDeadSessions_RemovesMissing()
    {
        // Create a real session folder with a DB file
        var realSessionDir = Path.Combine(_tempDir, "real_session");
        Directory.CreateDirectory(realSessionDir);
        await File.WriteAllBytesAsync(Path.Combine(realSessionDir, SessionPaths.DefaultDbFileName), []);

        await _store.AddRecentSessionAsync(realSessionDir, "Real");
        await _store.AddRecentSessionAsync("C:\\nonexistent", "Fake");

        var pruned = await _store.PruneDeadSessionsAsync();

        pruned.Should().Be(1);
        var remaining = await _store.GetRecentSessionsAsync();
        remaining.Should().ContainSingle();
        remaining[0].Title.Should().Be("Real");
    }

    [Fact]
    public async Task Preferences_SetAndGet_RoundTrips()
    {
        await _store.SetPreferenceAsync("theme", "dark");

        var value = await _store.GetPreferenceAsync("theme");

        value.Should().Be("dark");
    }

    [Fact]
    public async Task FeedbackRules_AddAndLookupByTemplate_RoundTrips()
    {
        var sourcePath = @"C:\logs\server-prod-20240315.log";
        var candidate = new FeedbackRuleCandidate(
            RuleType: "detection",
            TemplateKey: FeedbackTemplateKeyBuilder.BuildKey(sourcePath),
            Config: new FeedbackRuleConfig(
                SourceName: "server-prod-20240315.log",
                FormatKind: "CSV",
                Summary: "CSV · Timestamp · UTF-8 · UTC",
                EncodingCodePage: 65001,
                TimeBasis: "Utc",
                OffsetMinutes: null,
                TimeZoneId: null,
                TimestampExpression: "CsvComposite(date+time)",
                CsvDelimiter: ",",
                CsvHasHeader: true));

        await _store.AddOrUpdateFeedbackRuleAsync(candidate);

        var rules = await _store.GetFeedbackRulesAsync(@"C:\logs\server-prod-20240316.log", "detection");

        rules.Should().ContainSingle();
        rules[0].TemplateKey.Should().Be(candidate.TemplateKey);
        rules[0].Config.TimestampExpression.Should().Be("CsvComposite(date+time)");
        rules[0].Config.CsvHasHeader.Should().BeTrue();
    }

    [Fact]
    public async Task FeedbackRules_ReinforcingSameTemplate_UpdatesConfigAndUseCount()
    {
        var sourcePath = @"C:\logs\server-prod-20240315.log";
        var templateKey = FeedbackTemplateKeyBuilder.BuildKey(sourcePath);

        await _store.AddOrUpdateFeedbackRuleAsync(new FeedbackRuleCandidate(
            RuleType: "detection",
            TemplateKey: templateKey,
            Config: new FeedbackRuleConfig(
                SourceName: "server-prod-20240315.log",
                FormatKind: "Text",
                Summary: "Original",
                EncodingCodePage: 65001,
                TimeBasis: "Local",
                OffsetMinutes: null,
                TimeZoneId: null,
                TimestampExpression: "RegexGroup(ts)",
                CsvDelimiter: null,
                CsvHasHeader: null)));

        await _store.AddOrUpdateFeedbackRuleAsync(new FeedbackRuleCandidate(
            RuleType: "detection",
            TemplateKey: templateKey,
            Config: new FeedbackRuleConfig(
                SourceName: "server-prod-20240316.log",
                FormatKind: "Text",
                Summary: "Updated",
                EncodingCodePage: 1252,
                TimeBasis: "Zone",
                OffsetMinutes: null,
                TimeZoneId: "Europe/Rome",
                TimestampExpression: "RegexGroup(ts)",
                CsvDelimiter: null,
                CsvHasHeader: null)));

        var rules = await _store.GetFeedbackRulesAsync(sourcePath, "detection");

        rules.Should().ContainSingle();
        rules[0].UseCount.Should().Be(2);
        rules[0].Config.Summary.Should().Be("Updated");
        rules[0].Config.TimeZoneId.Should().Be("Europe/Rome");
    }

    public void Dispose()
    {
        _store.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
