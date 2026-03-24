using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.ComponentModel;
using ReactiveUI;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Export;
using ItomoriLog.Core.Query;
using System.Collections.ObjectModel;
using DuckDB.NET.Data;

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
    private bool _isEditingHeader;
    private string _editableTitle = "";
    private string _editableDescription = "";
    private string _editableTimezone = "";
    private double _overallRecordsPerSecond;
    private double? _overallEtaSeconds;
    private long _overallBytesProcessed;
    private long _overallBytesTotal;
    private DateTimeOffset? _lastOverallUpdateUtc;
    private long _lastOverallRecords;
    private readonly DetectionEngine _detectionEngine = new();
    private readonly SemaphoreSlim _sniffConcurrency = new(4, 4);

    /// <summary>
    /// When detection confidence is at or above this threshold, auto-ingest proceeds
    /// without user confirmation. Default: 0.95.
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
        StageFilesCommand = ReactiveCommand.Create<IReadOnlyList<string>>(StagePaths);
        RemoveStagedSourceCommand = ReactiveCommand.Create<StagedSourceItemViewModel>(RemoveStagedSource);
        ClearStagedSourcesCommand = ReactiveCommand.Create(ClearStagedSources);
        StartIngestionCommand = ReactiveCommand.CreateFromTask(StartIngestionAsync);
        ConfirmDetectionCommand = ReactiveCommand.Create<StagedSourceItemViewModel>(ConfirmDetectionChoice);
        ReingestSourceCommand = ReactiveCommand.CreateFromTask<StagedSourceItemViewModel>(ReingestSourceAsync);
        BeginHeaderEditCommand = ReactiveCommand.Create(BeginHeaderEdit);
        SaveHeaderCommand = ReactiveCommand.CreateFromTask(SaveHeaderAsync);
        CancelHeaderEditCommand = ReactiveCommand.Create(CancelHeaderEdit);
        IngestFilesCommand = ReactiveCommand.CreateFromTask<IReadOnlyList<string>>(IngestFilesAsync);
        StagedSources = [];

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
        set
        {
            this.RaiseAndSetIfChanged(ref _isIngesting, value);
            this.RaisePropertyChanged(nameof(CanStartIngestion));
            this.RaisePropertyChanged(nameof(CanModifyStagedSources));
        }
    }

    public ExistingFileAction ExistingFileAction
    {
        get => _existingFileAction;
        set => this.RaiseAndSetIfChanged(ref _existingFileAction, value);
    }

    public ObservableCollection<StagedSourceItemViewModel> StagedSources { get; }

    public bool IsEditingHeader
    {
        get => _isEditingHeader;
        set => this.RaiseAndSetIfChanged(ref _isEditingHeader, value);
    }

    public string EditableTitle
    {
        get => _editableTitle;
        set => this.RaiseAndSetIfChanged(ref _editableTitle, value);
    }

    public string EditableDescription
    {
        get => _editableDescription;
        set => this.RaiseAndSetIfChanged(ref _editableDescription, value);
    }

    public string EditableTimezone
    {
        get => _editableTimezone;
        set => this.RaiseAndSetIfChanged(ref _editableTimezone, value);
    }

    public long OverallBytesProcessed
    {
        get => _overallBytesProcessed;
        private set => this.RaiseAndSetIfChanged(ref _overallBytesProcessed, value);
    }

    public long OverallBytesTotal
    {
        get => _overallBytesTotal;
        private set => this.RaiseAndSetIfChanged(ref _overallBytesTotal, value);
    }

    public double OverallRecordsPerSecond
    {
        get => _overallRecordsPerSecond;
        private set => this.RaiseAndSetIfChanged(ref _overallRecordsPerSecond, value);
    }

    public double? OverallEtaSeconds
    {
        get => _overallEtaSeconds;
        private set => this.RaiseAndSetIfChanged(ref _overallEtaSeconds, value);
    }

    public string OverallProgressDisplay =>
        OverallBytesTotal > 0
            ? $"{FormatBytes(OverallBytesProcessed)} / {FormatBytes(OverallBytesTotal)}"
            : FormatBytes(OverallBytesProcessed);

    public string OverallThroughputDisplay =>
        OverallRecordsPerSecond > 0 ? $"{OverallRecordsPerSecond:N1} rec/s" : "-";

    public string OverallEtaDisplay =>
        OverallEtaSeconds.HasValue && OverallEtaSeconds.Value > 0 ? TimeSpan.FromSeconds(OverallEtaSeconds.Value).ToString(@"hh\:mm\:ss") : "-";

    public bool HasPendingDetectionReview => StagedSources.Any(s => s.RequiresDetectionReview);
    public bool HasSniffingInProgress => StagedSources.Any(s => s.IsSniffing);
    public bool CanStartIngestion =>
        !IsIngesting
        && StagedSources.Count > 0
        && !HasPendingDetectionReview
        && !HasSniffingInProgress;
    public bool CanModifyStagedSources => true;

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
    public ReactiveCommand<IReadOnlyList<string>, Unit> StageFilesCommand { get; }
    public ReactiveCommand<StagedSourceItemViewModel, Unit> RemoveStagedSourceCommand { get; }
    public ReactiveCommand<Unit, Unit> ClearStagedSourcesCommand { get; }
    public ReactiveCommand<Unit, Unit> StartIngestionCommand { get; }
    public ReactiveCommand<StagedSourceItemViewModel, Unit> ReingestSourceCommand { get; }
    public ReactiveCommand<StagedSourceItemViewModel, Unit> ConfirmDetectionCommand { get; }
    public ReactiveCommand<Unit, Unit> BeginHeaderEditCommand { get; }
    public ReactiveCommand<Unit, Unit> SaveHeaderCommand { get; }
    public ReactiveCommand<Unit, Unit> CancelHeaderEditCommand { get; }
    public ReactiveCommand<IReadOnlyList<string>, Unit> IngestFilesCommand { get; }

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
            DefaultTimezone = SessionDefaults.ResolveDefaultTimezone(header.DefaultTimezone);
            EditableTitle = Title;
            EditableDescription = Description;
            EditableTimezone = DefaultTimezone;
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
        RaiseIngestionReadinessProperties();
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
        if (HasSniffingInProgress)
        {
            StatusText = "Please wait for detection sniffing to complete.";
            return;
        }
        if (HasPendingDetectionReview)
        {
            StatusText = "Please review low-confidence format guesses before ingestion.";
            return;
        }

        IsIngesting = true;
        StatusText = $"Ingesting {filePaths.Count} staged source(s)...";
        ResetPerItemProgress();
        ResetOverallProgress();
        this.RaisePropertyChanged(nameof(OverallProgressDisplay));
        this.RaisePropertyChanged(nameof(OverallThroughputDisplay));
        this.RaisePropertyChanged(nameof(OverallEtaDisplay));

        try
        {
            var dbPath = SessionPaths.GetDbPath(_sessionFolder);
            using var factory = new DuckLakeConnectionFactory(dbPath);
            var conn = await factory.GetConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);
            var defaultTz = BuildDefaultTimeBasisConfig();

            var planner = new FileIngestPlanner(conn);
            var plan = await planner.PlanAsync(filePaths, ExistingFileAction, CancellationToken.None);

            foreach (var skipped in plan.SkippedFiles)
                StatusText = $"Skipped: {Path.GetFileName(skipped.SourcePath)} ({skipped.Reason})";

            foreach (var segmentId in plan.SegmentsToReingest)
            {
                var reingest = new ReingestService(conn);
                var reingestResult = await reingest.ReingestSegmentAsync(segmentId, defaultTz, CancellationToken.None);
                if (!reingestResult.Success)
                    StatusText = $"Re-ingest failed for segment {segmentId}: {reingestResult.Error}";
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

            var overrides = BuildDetectionOverrides(plan.FilesToIngest);
            var progress = new Progress<IngestProgressUpdate>(OnIngestProgress);
            var orchestrator = new IngestOrchestrator(conn, detectionOverrides: overrides);
            var result = await orchestrator.IngestFilesAsync(plan.FilesToIngest, defaultTz, progress);

            RecordCount += result.TotalRows;
            StatusText =
                $"{RecordCount:N0} records | {result.FilesProcessed} files ingested, {plan.SegmentsToReingest.Count} segments re-ingested, {result.Skips.Count + plan.SkippedFiles.Count} skips | {_sessionFolder}";

            using var globalStore = new GlobalStore();
            await globalStore.AddRecentSessionAsync(_sessionFolder, Title, Description);

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
            RaiseIngestionReadinessProperties();
            this.RaisePropertyChanged(nameof(OverallProgressDisplay));
            this.RaisePropertyChanged(nameof(OverallThroughputDisplay));
            this.RaisePropertyChanged(nameof(OverallEtaDisplay));
        }
    }

    private Dictionary<string, DetectionResult> BuildDetectionOverrides(IReadOnlyList<string> filePaths)
    {
        var result = new Dictionary<string, DetectionResult>(StringComparer.OrdinalIgnoreCase);
        var byPath = StagedSources
            .Where(s => !s.IsDirectory)
            .ToDictionary(
                s => Path.GetFullPath(s.SourcePath),
                s => s,
                StringComparer.OrdinalIgnoreCase);

        foreach (var filePath in filePaths)
        {
            var canonical = Path.GetFullPath(filePath);
            if (byPath.TryGetValue(canonical, out var item) &&
                item.TryGetSelectedDetection(out var detection))
            {
                result[canonical] = detection;
            }
        }

        return result;
    }

    private async Task ReingestSourceAsync(StagedSourceItemViewModel? item)
    {
        if (item is null || item.IsDirectory)
            return;
        if (IsIngesting)
        {
            StatusText = "Cannot re-ingest while ingestion is active.";
            return;
        }

        var dbPath = SessionPaths.GetDbPath(_sessionFolder);
        using var factory = new DuckLakeConnectionFactory(dbPath);
        var conn = await factory.GetConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var segmentIds = await ResolveSegmentsForSourceAsync(conn, item.SourcePath);
        if (segmentIds.Count == 0)
        {
            StatusText = $"No ingested segments found for {Path.GetFileName(item.SourcePath)}.";
            return;
        }

        var defaultTz = BuildDefaultTimeBasisConfig();
        var service = new ReingestService(conn);
        var overrideDetection = item.TryGetSelectedDetection(out var selectedDetection)
            ? selectedDetection
            : null;
        var ok = 0;
        var failed = 0;
        long replacedRows = 0;

        foreach (var segmentId in segmentIds)
        {
            var result = await service.ReingestSegmentAsync(
                segmentId,
                defaultTz,
                CancellationToken.None,
                overrideDetection: overrideDetection);
            if (result.Success)
            {
                ok++;
                replacedRows += result.NewRowCount;
            }
            else
            {
                failed++;
                StatusText = $"Re-ingest failed for {Path.GetFileName(item.SourcePath)} segment {segmentId}: {result.Error}";
            }
        }

        if (ok > 0)
            RecordCount = await ReadRecordCountAsync(conn);

        if (Timeline is not null) await Timeline.LoadCoarseBinsAsync();
        if (FacetPanel is not null) { FacetPanel.InvalidateCache(); await FacetPanel.RefreshAsync(); }

        StatusText = failed == 0
            ? $"{RecordCount:N0} records | Re-ingested {ok} segment(s), {replacedRows:N0} rows refreshed | {_sessionFolder}"
            : $"{RecordCount:N0} records | Re-ingest completed with {ok} success, {failed} failure | {_sessionFolder}";
    }

    private static async Task<long> ReadRecordCountAsync(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM logs";
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt64(result);
    }

    private static async Task<List<string>> ResolveSegmentsForSourceAsync(DuckDBConnection conn, string sourcePath)
    {
        var canonical = Path.GetFullPath(sourcePath);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT segment_id
            FROM segments
            WHERE active = TRUE
              AND (source_path = $1 OR source_path LIKE $2)
            ORDER BY segment_id
            """;
        cmd.Parameters.Add(new DuckDBParameter { Value = canonical });
        cmd.Parameters.Add(new DuckDBParameter { Value = $"{canonical}!%" });

        var segmentIds = new List<string>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            segmentIds.Add(reader.GetString(0));
        return segmentIds;
    }

    private void StartBackgroundSniff(StagedSourceItemViewModel item)
    {
        if (item.IsDirectory)
            return;

        item.MarkSniffing();
        RaiseIngestionReadinessProperties();

        _ = Task.Run(async () =>
        {
            await _sniffConcurrency.WaitAsync();
            try
            {
                await RunSniffAsync(item);
            }
            finally
            {
                _sniffConcurrency.Release();
                RunOnUi(RaiseIngestionReadinessProperties);
            }
        });
    }

    private async Task RunSniffAsync(StagedSourceItemViewModel item)
    {
        var sourcePath = item.SourcePath;
        if (!File.Exists(sourcePath))
        {
            RunOnUi(() => item.MarkDetectionFailed("File not found."));
            return;
        }

        if (sourcePath.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
        {
            RunOnUi(() =>
                item.ApplyDetectionChoices([], needsReview: false, "ZIP archive; entry formats are detected during ingest."));
            return;
        }

        try
        {
            await using var fs = File.OpenRead(sourcePath);
            var buffer = new byte[Math.Min(fs.Length, 256 * 1024)];
            var bytesRead = await fs.ReadAsync(buffer);
            using var sniff = new MemoryStream();
            if (bytesRead > 0)
                sniff.Write(buffer, 0, bytesRead);
            sniff.Position = 0;

            var candidates = _detectionEngine.DetectCandidates(sniff, Path.GetFileName(sourcePath));
            if (candidates.Count == 0)
            {
                RunOnUi(() => item.MarkDetectionFailed("No viable format candidates."));
                return;
            }

            var ranked = candidates
                .Select(c => new DetectionChoiceViewModel(
                    FormatName: FormatBoundaryName(c.Boundary),
                    Confidence: c.Confidence,
                    Detection: c))
                .OrderByDescending(c => c.Confidence)
                .ToList();

            var top = ranked[0];
            var closeSecond = ranked.Count > 1 && Math.Abs(ranked[0].Confidence - ranked[1].Confidence) < 0.02;
            var needsReview = top.Confidence < AutoIngestConfidenceThreshold || closeSecond;

            RunOnUi(() =>
                item.ApplyDetectionChoices(
                    ranked,
                    needsReview,
                    needsReview
                        ? $"Low confidence ({top.ConfidenceDisplay}) for {top.FormatName}. Select a format before ingesting."
                        : $"Detected {top.FormatName} ({top.ConfidenceDisplay})."));
        }
        catch (Exception ex)
        {
            RunOnUi(() => item.MarkDetectionFailed(ex.Message));
        }
    }

    private static string FormatBoundaryName(RecordBoundarySpec boundary) =>
        boundary switch
        {
            CsvBoundary => "CSV",
            JsonNdBoundary => "NDJSON",
            TextSoRBoundary => "Text",
            _ => boundary.GetType().Name.Replace("Boundary", "", StringComparison.Ordinal)
        };

    private static void RunOnUi(Action action)
    {
        RxApp.MainThreadScheduler.Schedule(Unit.Default, (_, _) =>
        {
            action();
            return Disposable.Empty;
        });
    }

    private void RaiseIngestionReadinessProperties()
    {
        this.RaisePropertyChanged(nameof(HasPendingDetectionReview));
        this.RaisePropertyChanged(nameof(HasSniffingInProgress));
        this.RaisePropertyChanged(nameof(CanStartIngestion));
    }

    private TimeBasisConfig BuildDefaultTimeBasisConfig()
    {
        var tz = SessionDefaults.ResolveDefaultTimezone(DefaultTimezone);
        if (!SessionDefaults.IsValidTimezoneId(tz))
            return new TimeBasisConfig(TimeBasis.Local);

        if (string.Equals(tz, TimeZoneInfo.Local.Id, StringComparison.OrdinalIgnoreCase))
            return new TimeBasisConfig(TimeBasis.Local);

        if (string.Equals(tz, "UTC", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tz, "Etc/UTC", StringComparison.OrdinalIgnoreCase))
        {
            return new TimeBasisConfig(TimeBasis.Utc);
        }

        return new TimeBasisConfig(TimeBasis.Zone, TimeZoneId: tz);
    }

    private void AttachStagedSource(StagedSourceItemViewModel item)
    {
        item.PropertyChanged += OnStagedSourcePropertyChanged;
        StartBackgroundSniff(item);
    }

    private void DetachStagedSource(StagedSourceItemViewModel item) =>
        item.PropertyChanged -= OnStagedSourcePropertyChanged;

    private void OnStagedSourcePropertyChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName is nameof(StagedSourceItemViewModel.RequiresDetectionReview)
            or nameof(StagedSourceItemViewModel.IsSniffing)
            or nameof(StagedSourceItemViewModel.SelectedDetectionChoice))
        {
            RaiseIngestionReadinessProperties();
        }
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

    private void StagePaths(IReadOnlyList<string> paths)
    {
        foreach (var path in paths.Where(p => !string.IsNullOrWhiteSpace(p)))
        {
            var fullPath = Path.GetFullPath(path);
            if (StagedSources.Any(s => string.Equals(s.SourcePath, fullPath, StringComparison.OrdinalIgnoreCase)))
                continue;

            var isDirectory = Directory.Exists(fullPath);
            if (!isDirectory && !File.Exists(fullPath))
                continue;

            var item = new StagedSourceItemViewModel(fullPath, isDirectory);
            StagedSources.Add(item);
            AttachStagedSource(item);
        }

        RaiseIngestionReadinessProperties();
    }

    private void RemoveStagedSource(StagedSourceItemViewModel? item)
    {
        if (item is null) return;
        if (!item.CanEdit) return;
        DetachStagedSource(item);
        StagedSources.Remove(item);
        RaiseIngestionReadinessProperties();
    }

    private void ClearStagedSources()
    {
        var removable = StagedSources.Where(s => s.CanEdit).ToList();
        foreach (var item in removable)
        {
            DetachStagedSource(item);
            StagedSources.Remove(item);
        }
        RaiseIngestionReadinessProperties();
    }

    private async Task StartIngestionAsync()
    {
        if (IsIngesting) return;
        var stagedPaths = StagedSources.Select(s => s.SourcePath).ToList();
        if (stagedPaths.Count == 0) return;
        await IngestFilesAsync(stagedPaths);
        RaiseIngestionReadinessProperties();
    }

    private void BeginHeaderEdit()
    {
        EditableTitle = Title;
        EditableDescription = Description;
        EditableTimezone = DefaultTimezone;
        IsEditingHeader = true;
    }

    private async Task SaveHeaderAsync()
    {
        var title = string.IsNullOrWhiteSpace(EditableTitle) ? Title : EditableTitle.Trim();
        var description = string.IsNullOrWhiteSpace(EditableDescription) ? "" : EditableDescription.Trim();
        var timezone = SessionDefaults.ResolveDefaultTimezone(EditableTimezone);
        if (!SessionDefaults.IsValidTimezoneId(timezone))
        {
            StatusText = $"Invalid timezone '{timezone}'.";
            return;
        }

        var dbPath = SessionPaths.GetDbPath(_sessionFolder);
        using var factory = new DuckLakeConnectionFactory(dbPath);
        var store = new SessionStore(factory);
        await store.UpdateHeaderAsync(title: title, description: description, defaultTimezone: timezone);

        Title = title;
        Description = description;
        DefaultTimezone = timezone;
        IsEditingHeader = false;

        using var globalStore = new GlobalStore();
        await globalStore.AddRecentSessionAsync(_sessionFolder, Title, Description);
    }

    private void CancelHeaderEdit()
    {
        IsEditingHeader = false;
    }

    private void OnIngestProgress(IngestProgressUpdate update)
    {
        var sourcePath = Path.GetFullPath(update.SourcePath);
        var bangIndex = sourcePath.IndexOf('!');
        var archivePath = bangIndex > 0 ? sourcePath[..bangIndex] : sourcePath;

        var item = StagedSources.FirstOrDefault(s =>
            string.Equals(Path.GetFullPath(s.SourcePath), sourcePath, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(Path.GetFullPath(s.SourcePath), archivePath, StringComparison.OrdinalIgnoreCase));
        if (item is not null)
        {
            item.ApplyProgress(
                phase: update.Phase.ToString(),
                bytesProcessed: update.BytesProcessed,
                bytesTotal: update.BytesTotal,
                recordsProcessed: update.RecordsProcessed,
                message: update.Message);
        }

        RecalculateOverallProgress();
    }

    private void ResetPerItemProgress()
    {
        foreach (var item in StagedSources)
        {
            item.ApplyProgress(
                phase: "Queued",
                bytesProcessed: 0,
                bytesTotal: 0,
                recordsProcessed: 0,
                message: "Queued");
        }
    }

    private void RecalculateOverallProgress()
    {
        var now = DateTimeOffset.UtcNow;
        var totalBytesProcessed = StagedSources.Sum(s => s.BytesProcessed);
        var totalBytesTotal = StagedSources.Sum(s => s.BytesTotal);
        var totalRecords = StagedSources.Sum(s => s.RecordsProcessed);

        if (_lastOverallUpdateUtc.HasValue)
        {
            var elapsed = (now - _lastOverallUpdateUtc.Value).TotalSeconds;
            if (elapsed > 0)
            {
                var deltaRecords = totalRecords - _lastOverallRecords;
                if (deltaRecords >= 0)
                    OverallRecordsPerSecond = deltaRecords / elapsed;
            }
        }

        _lastOverallUpdateUtc = now;
        _lastOverallRecords = totalRecords;

        OverallBytesProcessed = totalBytesProcessed;
        OverallBytesTotal = totalBytesTotal;

        if (OverallRecordsPerSecond > 0 && totalBytesTotal > 0 && totalBytesProcessed < totalBytesTotal && totalRecords > 0)
        {
            var processedRatio = Math.Clamp((double)totalBytesProcessed / totalBytesTotal, 0.0, 1.0);
            var estimatedTotalRecords = totalRecords / Math.Max(processedRatio, 0.01);
            var remainingRecords = Math.Max(estimatedTotalRecords - totalRecords, 0);
            OverallEtaSeconds = remainingRecords / OverallRecordsPerSecond;
        }
        else
        {
            OverallEtaSeconds = null;
        }

        this.RaisePropertyChanged(nameof(OverallProgressDisplay));
        this.RaisePropertyChanged(nameof(OverallThroughputDisplay));
        this.RaisePropertyChanged(nameof(OverallEtaDisplay));
    }

    private void ResetOverallProgress()
    {
        _lastOverallUpdateUtc = null;
        _lastOverallRecords = 0;
        OverallRecordsPerSecond = 0;
        OverallEtaSeconds = null;
        OverallBytesProcessed = 0;
        OverallBytesTotal = 0;
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024d:N1} KB";
        if (bytes < 1024L * 1024 * 1024) return $"{bytes / (1024d * 1024):N1} MB";
        return $"{bytes / (1024d * 1024 * 1024):N1} GB";
    }

    private void ConfirmDetectionChoice(StagedSourceItemViewModel? item)
    {
        if (item is null)
            return;

        item.ConfirmDetectionSelection();
        RaiseIngestionReadinessProperties();
    }
}
