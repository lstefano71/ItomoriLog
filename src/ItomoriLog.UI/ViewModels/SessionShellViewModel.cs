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
    private TimelineViewModel? _timeline;
    private FacetPanelViewModel? _facetPanel;

    /// <summary>
    /// When detection confidence is at or above this threshold, auto-ingest proceeds
    /// without showing the DetectionWizard. Default: 0.95.
    /// </summary>
    public double AutoIngestConfidenceThreshold { get; set; } = 0.95;

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

    public TimelineViewModel? Timeline
    {
        get => _timeline;
        set => this.RaiseAndSetIfChanged(ref _timeline, value);
    }

    public FacetPanelViewModel? FacetPanel
    {
        get => _facetPanel;
        set => this.RaiseAndSetIfChanged(ref _facetPanel, value);
    }

    public ReactiveCommand<Unit, Unit> CloseSessionCommand { get; }
    public ReactiveCommand<IReadOnlyList<string>, Unit> IngestFilesCommand { get; }

    /// <summary>
    /// Checks if file detection confidence warrants auto-ingest (≥ threshold)
    /// or if the wizard should be shown.
    /// </summary>
    public bool ShouldAutoIngest(EngineResult engineResult)
    {
        if (engineResult.Detection is null) return false;
        if (engineResult.NeedsDisambiguation) return false;
        return engineResult.Detection.Confidence >= AutoIngestConfidenceThreshold;
    }

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

        // Initialize child VMs with a persistent connection factory
        var logsFactory = new DuckLakeConnectionFactory(dbPath);
        var logsConn = await logsFactory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(logsConn);

        LogsPage = new LogsPageViewModel(logsFactory, DefaultTimezone);
        Timeline = new TimelineViewModel(logsFactory);
        FacetPanel = new FacetPanelViewModel(logsFactory);

        WireChildViewModels();

        // Load initial timeline and facets
        await Timeline.LoadCoarseBinsAsync();
        await FacetPanel.RefreshAsync();
    }

    private void WireChildViewModels()
    {
        if (Timeline is null || FacetPanel is null || LogsPage is null) return;

        // Timeline selection → FilterState time window on LogsPage
        Timeline.TimeRangeSelected += (start, end) =>
        {
            LogsPage.StartUtc = start;
            LogsPage.EndUtc = end;
            FacetPanel.UpdateTimeWindow(start, end);
        };

        // Facet selection changes → FilterState levels/sources on LogsPage
        FacetPanel.SelectionChanged += (levels, sources) =>
        {
            LogsPage.SelectedLevels = new System.Collections.ObjectModel.ObservableCollection<string>(levels);
            LogsPage.SelectedSources = new System.Collections.ObjectModel.ObservableCollection<string>(sources);
        };
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
            var detectionEngine = new DetectionEngine();

            // Pre-detect to check confidence for auto-ingest
            var allHighConfidence = true;
            foreach (var path in filePaths)
            {
                if (path.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
                    continue; // ZIP files always auto-processed

                using var sniffStream = new MemoryStream();
                using (var fs = File.OpenRead(path))
                {
                    var buffer = new byte[Math.Min(fs.Length, 256 * 1024)];
                    var bytesRead = await fs.ReadAsync(buffer);
                    sniffStream.Write(buffer, 0, bytesRead);
                }
                sniffStream.Position = 0;

                var engineResult = detectionEngine.Detect(sniffStream, Path.GetFileName(path));
                if (!ShouldAutoIngest(engineResult))
                {
                    allHighConfidence = false;
                    break;
                }
            }

            if (!allHighConfidence)
            {
                StatusText = "Detection confidence below threshold — wizard recommended";
                // In a full implementation, this would navigate to DetectionWizardView.
                // Proceed with standard ingest as graceful degradation.
            }

            var orchestrator = new IngestOrchestrator(conn);
            var result = await orchestrator.IngestFilesAsync(filePaths, defaultTz);

            RecordCount += result.TotalRows;
            StatusText = $"{RecordCount:N0} records | {result.FilesProcessed} files ingested, {result.Skips.Count} skips | {_sessionFolder}";

            // Update global store
            using var globalStore = new GlobalStore();
            await globalStore.AddRecentSessionAsync(_sessionFolder, Title, Description);

            // Refresh timeline and facets after ingest
            if (Timeline is not null) await Timeline.LoadCoarseBinsAsync();
            if (FacetPanel is not null) { FacetPanel.InvalidateCache(); await FacetPanel.RefreshAsync(); }
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
