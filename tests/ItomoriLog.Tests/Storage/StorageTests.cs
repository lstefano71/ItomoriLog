using FluentAssertions;
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
        cmd.CommandText = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name";
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
        Path.GetFileName(sessionDir).Should().Contain("My_Test_Session");
    }

    public void Dispose()
    {
        _factory.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
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

    public void Dispose()
    {
        _store.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
