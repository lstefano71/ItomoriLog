using FluentAssertions;

using ItomoriLog.Core.Storage;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.Tests.Storage;

public class SessionWorkflowTests : IDisposable
{
    private readonly string _tempDir;

    public SessionWorkflowTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"itomorilog_workflow_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    [Fact]
    public void WelcomeViewModel_DefaultTitle_IsPrepopulated()
    {
        var vm = new WelcomeViewModel(new MainWindowViewModel());
        vm.SessionTitle.Should().NotBeNullOrWhiteSpace();
        vm.SessionTitle.Should().Contain("ItomoriLog Session");
    }

    [Fact]
    public async Task SessionStore_UpdateHeader_UpdatesDescriptionAndTimezone()
    {
        var dbPath = Path.Combine(_tempDir, "workflow.duckdb");
        using var factory = new DuckLakeConnectionFactory(dbPath);
        var store = new SessionStore(factory);

        await store.InitializeAsync("Initial", "desc", "UTC");
        await store.UpdateHeaderAsync(title: "Renamed", description: "updated description", defaultTimezone: "Europe/Rome");
        var header = await store.ReadHeaderAsync();

        header.Should().NotBeNull();
        header!.Title.Should().Be("Renamed");
        header.Description.Should().Be("updated description");
        header.DefaultTimezone.Should().Be("Europe/Rome");
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
}
