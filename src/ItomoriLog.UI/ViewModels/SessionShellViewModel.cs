using System.Reactive;
using System.Reactive.Linq;
using ReactiveUI;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;
using ItomoriLog.Core.Ingest;

namespace ItomoriLog.UI.ViewModels;

public class SessionShellViewModel : ViewModelBase
{
    private readonly MainWindowViewModel _main;
    private readonly string _sessionFolder;
    private string _title = "";
    private string _description = "";
    private string _defaultTimezone = "";
    private string _statusText = "Ready";
    private long _recordCount;
    private bool _isIngesting;
    private LogsPageViewModel? _logsPage;

    public SessionShellViewModel(MainWindowViewModel main, string sessionFolder)
    {
        _main = main;
        _sessionFolder = sessionFolder;

        CloseSessionCommand = ReactiveCommand.Create(() => _main.NavigateToWelcome());
        IngestFilesCommand = ReactiveCommand.CreateFromTask<IReadOnlyList<string>>(IngestFilesAsync);

        // Load session header
        Observable.StartAsync(LoadHeaderAsync).Subscribe();
    }

    public string Title
    {
        get => _title;
        set => this.RaiseAndSetIfChanged(ref _title, value);
    }

    public string Description
    {
        get => _description;
        set => this.RaiseAndSetIfChanged(ref _description, value);
    }

    public string DefaultTimezone
    {
        get => _defaultTimezone;
        set => this.RaiseAndSetIfChanged(ref _defaultTimezone, value);
    }

    public string SessionFolder => _sessionFolder;

    public string StatusText
    {
        get => _statusText;
        set => this.RaiseAndSetIfChanged(ref _statusText, value);
    }

    public long RecordCount
    {
        get => _recordCount;
        set => this.RaiseAndSetIfChanged(ref _recordCount, value);
    }

    public bool IsIngesting
    {
        get => _isIngesting;
        set => this.RaiseAndSetIfChanged(ref _isIngesting, value);
    }

    public LogsPageViewModel? LogsPage
    {
        get => _logsPage;
        set => this.RaiseAndSetIfChanged(ref _logsPage, value);
    }

    public ReactiveCommand<Unit, Unit> CloseSessionCommand { get; }
    public ReactiveCommand<IReadOnlyList<string>, Unit> IngestFilesCommand { get; }

    private async Task LoadHeaderAsync()
    {
        var dbPath = SessionPaths.GetDbPath(_sessionFolder);
        using var factory = new DuckLakeConnectionFactory(dbPath);
        var store = new SessionStore(factory);
        var header = await store.ReadHeaderAsync();
        if (header is not null)
        {
            Title = header.Title;
            Description = header.Description ?? "";
            DefaultTimezone = header.DefaultTimezone ?? TimeZoneInfo.Local.Id;
        }

        // Count records
        var conn = await factory.GetConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM logs";
        try
        {
            var count = await cmd.ExecuteScalarAsync();
            RecordCount = Convert.ToInt64(count);
        }
        catch { RecordCount = 0; }

        StatusText = $"{RecordCount:N0} records | {_sessionFolder}";

        // Initialize logs page VM with a persistent connection factory
        var logsFactory = new DuckLakeConnectionFactory(dbPath);
        var logsConn = await logsFactory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(logsConn);
        LogsPage = new LogsPageViewModel(logsFactory, DefaultTimezone);
    }

    private async Task IngestFilesAsync(IReadOnlyList<string> filePaths)
    {
        if (filePaths.Count == 0) return;

        IsIngesting = true;
        StatusText = $"Ingesting {filePaths.Count} file(s)...";

        try
        {
            var dbPath = SessionPaths.GetDbPath(_sessionFolder);
            using var factory = new DuckLakeConnectionFactory(dbPath);
            var conn = await factory.GetConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);

            var defaultTz = new TimeBasisConfig(TimeBasis.Local);
            var orchestrator = new IngestOrchestrator(conn);
            var result = await orchestrator.IngestFilesAsync(filePaths, defaultTz);

            RecordCount += result.TotalRows;
            StatusText = $"{RecordCount:N0} records | {result.FilesProcessed} files ingested, {result.Skips.Count} skips | {_sessionFolder}";

            // Update global store
            using var globalStore = new GlobalStore();
            await globalStore.AddRecentSessionAsync(_sessionFolder, Title, Description);
        }
        catch (Exception ex)
        {
            StatusText = $"Ingest error: {ex.Message}";
        }
        finally
        {
            IsIngesting = false;
        }
    }
}
