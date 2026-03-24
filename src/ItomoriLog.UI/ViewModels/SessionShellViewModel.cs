using System.Reactive;
using System.Reactive.Linq;
using ReactiveUI;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Export;
using ItomoriLog.Core.Query;

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
    private CrashRecoveryViewModel? _crashRecovery;
    private CrashRecoveryService? _crashRecoveryService;
    private ExistingFileAction _existingFileAction = ExistingFileAction.Skip;

    /// <summary>
    /// When detection confidence is at or above this threshold, auto-ingest proceeds
    /// without showing the DetectionWizard. Default: 0.95.
    /// </summary>
    public double AutoIngestConfidenceThreshold { get; set; } = 0.95;

    public SessionShellViewModel(MainWindowViewModel main, string sessionFolder)
    {
        _main = main;
        _sessionFolder = sessionFolder;

        CloseSessionCommand = ReactiveCommand.Create(() =>
        {
            ReleaseSessionLock();
            _main.NavigateToWelcome();
        });
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

    public ExistingFileAction ExistingFileAction
    {
        get => _existingFileAction;
        set => this.RaiseAndSetIfChanged(ref _existingFileAction, value);
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

    public CrashRecoveryViewModel? CrashRecovery
    {
        get => _crashRecovery;
        set => this.RaiseAndSetIfChanged(ref _crashRecovery, value);
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
        _crashRecoveryService = new CrashRecoveryService(_sessionFolder);
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

        var recoveryStatus = await _crashRecoveryService.CheckAsync(conn);
        _crashRecoveryService.AcquireLock();
        CrashRecovery = new CrashRecoveryViewModel(
            recoveryStatus,
            onResume: () =>
            {
                _ = Task.Run(async () =>
                {
                    await _crashRecoveryService.MarkRunsAbandonedAsync(conn);
                    _crashRecoveryService.AcquireLock();
                });
            },
            onDismiss: () => { });

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
        FacetPanel.SelectionChanged += (levels, excludedLevels, sources, excludedSources) =>
        {
            LogsPage.SelectedLevels = new System.Collections.ObjectModel.ObservableCollection<string>(levels);
            LogsPage.ExcludedLevels = new System.Collections.ObjectModel.ObservableCollection<string>(excludedLevels);
            LogsPage.SelectedSources = new System.Collections.ObjectModel.ObservableCollection<string>(sources);
            LogsPage.ExcludedSources = new System.Collections.ObjectModel.ObservableCollection<string>(excludedSources);
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

            var planner = new FileIngestPlanner(conn);
            var plan = await planner.PlanAsync(filePaths, ExistingFileAction, CancellationToken.None);

            foreach (var skipped in plan.SkippedFiles)
            {
                StatusText = $"Skipped: {Path.GetFileName(skipped.SourcePath)} ({skipped.Reason})";
            }

            foreach (var segmentId in plan.SegmentsToReingest)
            {
                var reingest = new ReingestService(conn);
                var reingestResult = await reingest.ReingestSegmentAsync(segmentId, defaultTz, CancellationToken.None);
                if (!reingestResult.Success)
                {
                    StatusText = $"Re-ingest failed for segment {segmentId}: {reingestResult.Error}";
                }
            }

            if (plan.FilesToIngest.Count == 0 && plan.SegmentsToReingest.Count > 0)
            {
                StatusText = $"{RecordCount:N0} records | Re-ingested {plan.SegmentsToReingest.Count} segment(s) | {_sessionFolder}";
                if (Timeline is not null) await Timeline.LoadCoarseBinsAsync();
                if (FacetPanel is not null) { FacetPanel.InvalidateCache(); await FacetPanel.RefreshAsync(); }
                return;
            }

            if (plan.FilesToIngest.Count == 0)
            {
                StatusText = $"{RecordCount:N0} records | No new files to ingest | {_sessionFolder}";
                return;
            }

            var detectionEngine = new DetectionEngine();

            // Pre-detect to check confidence for auto-ingest
            var candidates = await BuildDetectionCandidatesAsync(plan.FilesToIngest, detectionEngine);
            var allHighConfidence = candidates.All(c => ShouldAutoIngest(c.EngineResult));
            if (!allHighConfidence && candidates.Count > 0)
            {
                var wizard = new DetectionWizardViewModel(
                    candidates.Select(c => c.Candidate).ToList(),
                    candidate => candidate.PreviewLines ?? ["(preview unavailable)"]);
                var tcs = new TaskCompletionSource<DetectionCandidate?>(
                    TaskCreationOptions.RunContinuationsAsynchronously);

                using var confirmSub = wizard.ConfirmCommand.Subscribe(selected =>
                {
                    _main.CurrentView = this;
                    tcs.TrySetResult(selected);
                });
                using var cancelSub = wizard.CancelCommand.Subscribe(_ =>
                {
                    _main.CurrentView = this;
                    tcs.TrySetResult(null);
                });

                _main.CurrentView = wizard;
                var selected = await tcs.Task;

                if (selected is null)
                {
                    StatusText = "Ingest cancelled from detection wizard.";
                    return;
                }
            }

            var orchestrator = new IngestOrchestrator(conn);
            var result = await orchestrator.IngestFilesAsync(plan.FilesToIngest, defaultTz);

            RecordCount += result.TotalRows;
            StatusText =
                $"{RecordCount:N0} records | {result.FilesProcessed} files ingested, {plan.SegmentsToReingest.Count} segments re-ingested, {result.Skips.Count + plan.SkippedFiles.Count} skips | {_sessionFolder}";

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

    private static async Task<List<(EngineResult EngineResult, DetectionCandidate Candidate)>> BuildDetectionCandidatesAsync(
        IReadOnlyList<string> filePaths,
        DetectionEngine detectionEngine)
    {
        var candidates = new List<(EngineResult EngineResult, DetectionCandidate Candidate)>();
        foreach (var path in filePaths.Where(p => !p.EndsWith(".zip", StringComparison.OrdinalIgnoreCase)))
        {
            using var sniffStream = new MemoryStream();
            await using (var fs = File.OpenRead(path))
            {
                var buffer = new byte[Math.Min(fs.Length, 256 * 1024)];
                var bytesRead = await fs.ReadAsync(buffer);
                sniffStream.Write(buffer, 0, bytesRead);
            }
            sniffStream.Position = 0;

            var engineResult = detectionEngine.Detect(sniffStream, Path.GetFileName(path));
            if (engineResult.Detection is null)
                continue;

            candidates.Add((engineResult, new DetectionCandidate(
                FormatName: engineResult.Detection.Boundary.GetType().Name.Replace("Boundary", ""),
                Confidence: engineResult.Detection.Confidence,
                Detection: engineResult.Detection,
                SourcePath: path,
                PreviewLines: await ReadPreviewLinesAsync(path))));
        }

        return candidates
            .OrderByDescending(c => c.Candidate.Confidence)
            .ToList();
    }

    private static async Task<IReadOnlyList<string>> ReadPreviewLinesAsync(string path)
    {
        var result = new List<string>();
        await using var fs = File.OpenRead(path);
        using var reader = new StreamReader(fs);
        while (result.Count < 5)
        {
            var line = await reader.ReadLineAsync();
            if (line is null)
                break;
            if (!string.IsNullOrWhiteSpace(line))
                result.Add(line);
        }

        return result.Count > 0 ? result : ["(no records parsed)"];
    }

    public FilterState BuildCurrentFilterState()
    {
        if (LogsPage is null)
            return FilterState.Empty;
        return LogsPage.BuildCurrentFilterState();
    }

    public ExportOptions BuildExportOptions(ExportScope scope, ExportFormat format, string outputPath)
    {
        var filter = scope == ExportScope.CurrentView ? BuildCurrentFilterState() : null;
        return new ExportOptions(
            Format: format,
            OutputPath: outputPath,
            Filter: filter,
            Scope: scope,
            SessionTitle: Title,
            SessionDescription: Description,
            SessionFolder: SessionFolder);
    }

    public async Task<long> ExecuteExportAsync(
        ExportOptions options,
        IProgress<ExportProgress> progress,
        CancellationToken ct)
    {
        var dbPath = SessionPaths.GetDbPath(_sessionFolder);
        using var factory = new DuckLakeConnectionFactory(dbPath);
        var conn = await factory.GetConnectionAsync(ct);
        var service = new ExportService(conn);
        return await service.ExportAsync(options, progress, ct);
    }

    public void ReleaseSessionLock() => _crashRecoveryService?.ReleaseLock();
}
