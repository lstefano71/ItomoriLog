using FluentAssertions;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Storage;
using DuckDB.NET.Data;

namespace ItomoriLog.Tests.Query;

public class QueryHistoryServiceTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckLakeConnectionFactory _factory;

    public QueryHistoryServiceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_qhist_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _factory = new DuckLakeConnectionFactory(_dbPath);
    }

    [Fact]
    public async Task RecordAndRetrieve_ReturnsEntry()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var service = new QueryHistoryService(conn);
        await service.EnsureSchemaAsync();

        await service.RecordQueryAsync("SELECT * FROM logs", 42);

        var recent = await service.GetRecentAsync();
        recent.Should().HaveCount(1);
        recent[0].QueryText.Should().Be("SELECT * FROM logs");
        recent[0].ResultCount.Should().Be(42);
    }

    [Fact]
    public async Task GetRecent_ReturnsInReverseChronologicalOrder()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var service = new QueryHistoryService(conn);
        await service.EnsureSchemaAsync();

        await service.RecordQueryAsync("query 1", 10);
        await Task.Delay(10);
        await service.RecordQueryAsync("query 2", 20);
        await Task.Delay(10);
        await service.RecordQueryAsync("query 3", 30);

        var recent = await service.GetRecentAsync();
        recent.Should().HaveCount(3);
        recent[0].QueryText.Should().Be("query 3");
        recent[1].QueryText.Should().Be("query 2");
        recent[2].QueryText.Should().Be("query 1");
    }

    [Fact]
    public async Task GetRecent_RespectsLimit()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var service = new QueryHistoryService(conn);
        await service.EnsureSchemaAsync();

        for (int i = 0; i < 10; i++)
            await service.RecordQueryAsync($"query {i}");

        var recent = await service.GetRecentAsync(limit: 3);
        recent.Should().HaveCount(3);
    }

    [Fact]
    public async Task Search_FindsMatchingQueries()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var service = new QueryHistoryService(conn);
        await service.EnsureSchemaAsync();

        await service.RecordQueryAsync("SELECT * FROM logs WHERE level = 'ERROR'");
        await service.RecordQueryAsync("SELECT COUNT(*) FROM segments");
        await service.RecordQueryAsync("SELECT * FROM logs WHERE level = 'WARN'");

        var results = await service.SearchAsync("ERROR");
        results.Should().HaveCount(1);
        results[0].QueryText.Should().Contain("ERROR");
    }

    [Fact]
    public async Task Search_CaseInsensitive()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var service = new QueryHistoryService(conn);
        await service.EnsureSchemaAsync();

        await service.RecordQueryAsync("SELECT * FROM logs WHERE level = 'ERROR'");

        var results = await service.SearchAsync("error");
        results.Should().HaveCount(1);
    }

    [Fact]
    public async Task RecordQuery_WithNullResultCount_StoresNull()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var service = new QueryHistoryService(conn);
        await service.EnsureSchemaAsync();

        await service.RecordQueryAsync("some query");

        var recent = await service.GetRecentAsync();
        recent.Should().HaveCount(1);
        recent[0].ResultCount.Should().BeNull();
    }

    [Fact]
    public async Task RecordQuery_IgnoresWhitespaceOnlyQueries()
    {
        var conn = await _factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var service = new QueryHistoryService(conn);
        await service.EnsureSchemaAsync();

        await service.RecordQueryAsync("   ");
        await service.RecordQueryAsync("");

        var recent = await service.GetRecentAsync();
        recent.Should().BeEmpty();
    }

    [Fact]
    public async Task GlobalStore_QueryHistory_RoundTrip()
    {
        var globalRoot = Path.Combine(_tempDir, "global");
        using var globalStore = new GlobalStore(globalRoot);

        await globalStore.AddQueryHistoryAsync("global query 1", "session-abc", 100);
        await globalStore.AddQueryHistoryAsync("global query 2", null, 200);

        var recent = await globalStore.GetRecentQueriesAsync();
        recent.Should().HaveCount(2);
        recent[0].QueryText.Should().Be("global query 2"); // most recent first
        recent[1].QueryText.Should().Be("global query 1");
        recent[1].SessionId.Should().Be("session-abc");
        recent[1].ResultCount.Should().Be(100);
    }

    [Fact]
    public async Task GlobalStore_SearchQueries_FindsMatches()
    {
        var globalRoot = Path.Combine(_tempDir, "global_search");
        using var globalStore = new GlobalStore(globalRoot);

        await globalStore.AddQueryHistoryAsync("SELECT * FROM logs WHERE level = 'ERROR'", "s1");
        await globalStore.AddQueryHistoryAsync("SELECT COUNT(*) FROM segments", "s2");

        var results = await globalStore.SearchQueriesAsync("ERROR");
        results.Should().HaveCount(1);
        results[0].QueryText.Should().Contain("ERROR");
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
