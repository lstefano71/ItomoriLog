using DuckDB.NET.Data;

using ItomoriLog.Core.Export;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Storage;

using ReactiveUI;

using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Text;
using System.Text.Json;

namespace ItomoriLog.UI.ViewModels;

public class SessionShellViewModel : ViewModelBase
{
    private static readonly TimeSpan MinStableRateWindow = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MinLiveBrowseRefreshInterval = TimeSpan.FromSeconds(1);

    private readonly MainWindowViewModel _main;
    private readonly string _sessionFolder;
    private readonly DetectionEngine _detectionEngine = new();
    private readonly SemaphoreSlim _sniffConcurrency = new(4, 4);
    private readonly SemaphoreSlim _liveBrowseRefreshGate = new(1, 1);
    private readonly HashSet<string> _activeIngestPaths = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, Dictionary<string, IngestProgressUpdate>> _archiveEntryProgress = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _liveBrowseRefreshLock = new();

    private DuckLakeConnectionFactory? _sessionFactory;
    private string _title = "";
    private string _description = "";
    private string _defaultTimezone = "";
    private string _statusText = "Opening session...";
    private long _recordCount;
    private bool _isIngesting;
    private bool _isEditingHeader;
    private string _editableTitle = "";
    private string _editableDescription = "";
    private string _editableTimezone = "";
    private ExistingFileAction _existingFileAction = ExistingFileAction.Skip;
    private double _overallRecordsPerSecond;
    private double? _overallEtaSeconds;
    private long _overallBytesProcessed;
    private long _overallBytesTotal;
    private long _ingestBaselineRecordCount;
    private DateTimeOffset? _overallProgressStartedUtc;
    private LogsPageViewModel? _logsPage;
    private TimelineViewModel? _timeline;
    private FacetPanelViewModel? _facetPanel;
    private CrashRecoveryViewModel? _crashRecovery;
    private CrashRecoveryService? _crashRecoveryService;
    private bool _isStagingPaneOpen = true;
    private StagedSourceItemViewModel? _selectedStagedSource;
    private bool _recoveryBannerDismissed;
    private bool _liveBrowseRefreshQueued;
    private bool _liveBrowseRefreshDirty;
    private DateTimeOffset _lastLiveBrowseRefreshUtc = DateTimeOffset.MinValue;

    public SessionShellViewModel(MainWindowViewModel main, string sessionFolder)
    {
        _main = main;
        _sessionFolder = sessionFolder;

        CloseSessionCommand = ReactiveCommand.Create(() => {
            ReleaseSessionLock();
            _main.NavigateToWelcome();
        });
        ToggleStagingPaneCommand = ReactiveCommand.Create(ToggleStagingPane);
        StageFilesCommand = ReactiveCommand.CreateFromTask<IReadOnlyList<string>>(StagePathsAsync);
        RemoveStagedSourceCommand = ReactiveCommand.Create<StagedSourceItemViewModel>(RemoveStagedSource);
        ClearStagedSourcesCommand = ReactiveCommand.Create(ClearStagedSources);
        StartIngestionCommand = ReactiveCommand.CreateFromTask(StartIngestionAsync);
        ReingestSourceCommand = ReactiveCommand.CreateFromTask<StagedSourceItemViewModel>(ReingestSourceAsync);
        ConfirmDetectionCommand = ReactiveCommand.Create<StagedSourceItemViewModel>(ConfirmDetectionChoice);
        BeginHeaderEditCommand = ReactiveCommand.Create(BeginHeaderEdit);
        SaveHeaderCommand = ReactiveCommand.CreateFromTask(SaveHeaderAsync);
        CancelHeaderEditCommand = ReactiveCommand.Create(CancelHeaderEdit);
        IngestFilesCommand = ReactiveCommand.CreateFromTask<IReadOnlyList<string>>(IngestFilesAsync);
        StagedSources = [];

        Observable.FromAsync(LoadHeaderAsync).Subscribe();
    }

    /// <summary>
    /// When detection confidence is at or above this threshold, auto-ingest proceeds
    /// without user confirmation.
    /// </summary>
    public double AutoIngestConfidenceThreshold { get; set; } = 0.95;

    public string Title {
        get => _title;
        set => this.RaiseAndSetIfChanged(ref _title, value);
    }

    public string Description {
        get => _description;
        set => this.RaiseAndSetIfChanged(ref _description, value);
    }

    public string DefaultTimezone {
        get => _defaultTimezone;
        set {
            this.RaiseAndSetIfChanged(ref _defaultTimezone, value);
            this.RaisePropertyChanged(nameof(SessionTimezoneSummary));
            LogsPage?.SetDisplayTimezone(SessionDefaults.ResolveDefaultTimezone(value));
        }
    }

    public string SessionFolder => _sessionFolder;

    public string StatusText {
        get => _statusText;
        set => this.RaiseAndSetIfChanged(ref _statusText, value);
    }

    public long RecordCount {
        get => _recordCount;
        set => this.RaiseAndSetIfChanged(ref _recordCount, value);
    }

    public bool IsIngesting {
        get => _isIngesting;
        set {
            this.RaiseAndSetIfChanged(ref _isIngesting, value);
            RaiseStagingStateProperties();
        }
    }

    public ExistingFileAction ExistingFileAction {
        get => _existingFileAction;
        set => this.RaiseAndSetIfChanged(ref _existingFileAction, value);
    }

    public ObservableCollection<StagedSourceItemViewModel> StagedSources { get; }

    public bool IsEditingHeader {
        get => _isEditingHeader;
        set => this.RaiseAndSetIfChanged(ref _isEditingHeader, value);
    }

    public string EditableTitle {
        get => _editableTitle;
        set => this.RaiseAndSetIfChanged(ref _editableTitle, value);
    }

    public string EditableDescription {
        get => _editableDescription;
        set => this.RaiseAndSetIfChanged(ref _editableDescription, value);
    }

    public string EditableTimezone {
        get => _editableTimezone;
        set => this.RaiseAndSetIfChanged(ref _editableTimezone, value);
    }

    public long OverallBytesProcessed {
        get => _overallBytesProcessed;
        private set => this.RaiseAndSetIfChanged(ref _overallBytesProcessed, value);
    }

    public long OverallBytesTotal {
        get => _overallBytesTotal;
        private set => this.RaiseAndSetIfChanged(ref _overallBytesTotal, value);
    }

    public double OverallRecordsPerSecond {
        get => _overallRecordsPerSecond;
        private set => this.RaiseAndSetIfChanged(ref _overallRecordsPerSecond, value);
    }

    public double? OverallEtaSeconds {
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
        OverallEtaSeconds.HasValue && OverallEtaSeconds.Value > 0
            ? TimeSpan.FromSeconds(OverallEtaSeconds.Value).ToString(@"hh\:mm\:ss")
            : "-";

    public bool HasPendingDetectionReview => StagedSources.Any(s => s.RequiresDetectionReview);
    public bool HasInvalidDetectionOverrides => StagedSources.Any(s => s.HasOverrideValidationError);
    public bool HasSniffingInProgress => StagedSources.Any(s => s.IsSniffing);
    public bool HasStagedSources => StagedSources.Count > 0;
    public bool HasRunnableStagedSources => StagedSources.Any(s => s.Phase != nameof(IngestFilePhase.Completed));
    public bool CanStartIngestion => !IsIngesting
        && HasRunnableStagedSources
        && !HasPendingDetectionReview
        && !HasInvalidDetectionOverrides
        && !HasSniffingInProgress;
    public bool CanModifyStagedSources => !IsIngesting;
    public bool HasSelectedStagedSource => SelectedStagedSource is not null;
    public bool CanConfirmSelectedDetection => SelectedStagedSource?.RequiresDetectionReview == true && !IsIngesting;
    public bool CanRemoveSelectedStagedSource => SelectedStagedSource?.CanEdit == true && !IsIngesting;
    public bool CanReingestSelectedSource => SelectedStagedSource?.CanReingest == true && !IsIngesting;

    public string SessionTimezoneSummary => DescribeSessionTimezone();

    public string StagingSummary {
        get {
            if (StagedSources.Count == 0)
                return "No staged files.";

            var parts = new List<string> { $"{StagedSources.Count} file(s)" };

            var reviewCount = StagedSources.Count(s => s.RequiresDetectionReview);
            if (reviewCount > 0)
                parts.Add($"{reviewCount} need review");

            var sniffingCount = StagedSources.Count(s => s.IsSniffing);
            if (sniffingCount > 0)
                parts.Add($"{sniffingCount} sniffing");

            var completedCount = StagedSources.Count(s => s.Phase == nameof(IngestFilePhase.Completed));
            if (completedCount > 0)
                parts.Add($"{completedCount} completed");

            return string.Join(" · ", parts);
        }
    }

    public string StagingPaneToggleText =>
        IsStagingPaneOpen
            ? "Hide queue"
            : HasStagedSources
                ? $"Show queue ({StagedSources.Count})"
                : "Show queue";

    public LogsPageViewModel? LogsPage {
        get => _logsPage;
        set => this.RaiseAndSetIfChanged(ref _logsPage, value);
    }

    public TimelineViewModel? Timeline {
        get => _timeline;
        set => this.RaiseAndSetIfChanged(ref _timeline, value);
    }

    public FacetPanelViewModel? FacetPanel {
        get => _facetPanel;
        set => this.RaiseAndSetIfChanged(ref _facetPanel, value);
    }

    public CrashRecoveryViewModel? CrashRecovery {
        get => _crashRecovery;
        set => this.RaiseAndSetIfChanged(ref _crashRecovery, value);
    }

    public bool IsStagingPaneOpen {
        get => _isStagingPaneOpen;
        private set {
            this.RaiseAndSetIfChanged(ref _isStagingPaneOpen, value);
            this.RaisePropertyChanged(nameof(StagingPaneToggleText));
        }
    }

    public StagedSourceItemViewModel? SelectedStagedSource {
        get => _selectedStagedSource;
        set {
            this.RaiseAndSetIfChanged(ref _selectedStagedSource, value);
            this.RaisePropertyChanged(nameof(HasSelectedStagedSource));
            this.RaisePropertyChanged(nameof(CanConfirmSelectedDetection));
            this.RaisePropertyChanged(nameof(CanRemoveSelectedStagedSource));
            this.RaisePropertyChanged(nameof(CanReingestSelectedSource));
        }
    }

    public ReactiveCommand<Unit, Unit> CloseSessionCommand { get; }
    public ReactiveCommand<Unit, Unit> ToggleStagingPaneCommand { get; }
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

    public event Action? OpenFilePickerRequested;

    public void OpenStagingPane() => IsStagingPaneOpen = true;

    public void RequestOpenFilePicker() => OpenFilePickerRequested?.Invoke();

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
        var conn = await GetSessionConnectionAsync(ct);
        var service = new ExportService(conn);
        return await service.ExportAsync(options, progress, ct);
    }

    public void ReleaseSessionLock()
    {
        _crashRecoveryService?.ReleaseLock();
        _sessionFactory?.Dispose();
        _sessionFactory = null;
    }

    private async Task LoadHeaderAsync()
    {
        var dbPath = SessionPaths.GetDbPath(_sessionFolder);
        _crashRecoveryService = new CrashRecoveryService(_sessionFolder);
        SessionHeader? header;
        long recordCount;
        CrashRecoveryStatus recoveryStatus;

        using (var bootstrapFactory = new DuckLakeConnectionFactory(dbPath)) {
            var bootstrapConnection = await bootstrapFactory.GetConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(bootstrapConnection);

            var store = new SessionStore(bootstrapFactory);
            header = await store.ReadHeaderAsync();
            recordCount = await ReadRecordCountAsync(bootstrapConnection);
            recoveryStatus = await _crashRecoveryService.CheckAsync(bootstrapConnection);
        }

        if (header is not null) {
            Title = header.Title;
            Description = header.Description ?? "";
            DefaultTimezone = SessionDefaults.ResolveDefaultTimezone(header.DefaultTimezone);
            EditableTitle = Title;
            EditableDescription = Description;
            EditableTimezone = DefaultTimezone;
        } else {
            DefaultTimezone = SessionDefaults.ResolveDefaultTimezone(null);
            EditableTimezone = DefaultTimezone;
        }

        RecordCount = recordCount;
        _crashRecoveryService.AcquireLock();
        CrashRecovery = !_recoveryBannerDismissed && recoveryStatus.CanResume
            ? new CrashRecoveryViewModel(
                recoveryStatus,
                onResume: ResumeInterruptedIngestionAsync,
                onDismiss: DismissRecoveryBanner)
            : new CrashRecoveryViewModel();

        var logsFactory = GetSessionFactory();
        LogsPage = new LogsPageViewModel(logsFactory, DefaultTimezone);
        Timeline = new TimelineViewModel(logsFactory);
        FacetPanel = new FacetPanelViewModel(logsFactory);

        WireChildViewModels();
        IsStagingPaneOpen = RecordCount == 0;

        await RefreshBrowseAsync(resetFilters: false, invalidateCache: true);
        StatusText = BuildSessionStatus(RecordCount == 0 ? "Session ready for ingestion." : "Session loaded.");
        RaiseStagingStateProperties();
    }

    private void WireChildViewModels()
    {
        if (Timeline is null || FacetPanel is null || LogsPage is null)
            return;

        Timeline.TimeRangeSelected += (start, end) => {
            LogsPage.StartUtc = start;
            LogsPage.EndUtc = end;
            FacetPanel.UpdateTimeWindow(start, end);
        };

        Timeline.TimeRangeCleared += () => {
            LogsPage.StartUtc = null;
            LogsPage.EndUtc = null;
            FacetPanel.UpdateTimeWindow(null, null);
        };

        FacetPanel.SelectionChanged += (levels, excludedLevels, sources, excludedSources) => {
            LogsPage.SelectedLevels = new ObservableCollection<string>(levels);
            LogsPage.ExcludedLevels = new ObservableCollection<string>(excludedLevels);
            LogsPage.SelectedSources = new ObservableCollection<string>(sources);
            LogsPage.ExcludedSources = new ObservableCollection<string>(excludedSources);
        };

        LogsPage.TimelineFilterChanged += filter => _ = Timeline.ApplyMatchFilterAsync(filter);
    }

    private async Task ResumeInterruptedIngestionAsync()
    {
        if (_crashRecoveryService is null)
            return;

        _recoveryBannerDismissed = true;
        CrashRecovery = new CrashRecoveryViewModel();
        StatusText = BuildSessionStatus("Recovering interrupted ingest...");

        var conn = await GetSessionConnectionAsync();
        await SchemaInitializer.EnsureSchemaAsync(conn);

        var resumablePaths = await _crashRecoveryService.RecoverInterruptedIngestionAsync(conn);
        _crashRecoveryService.AcquireLock();

        if (resumablePaths.Count == 0) {
            StatusText = BuildSessionStatus("Nothing resumable was found.");
            return;
        }

        var stagedItems = StagePathsCore(resumablePaths, openPane: true, queueSniff: false);
        foreach (var item in stagedItems)
            item.MarkPendingResume(SessionTimezoneSummary);

        await SniffStagedSourcesAsync(stagedItems);

        if (stagedItems.Any(item => item.RequiresDetectionReview)) {
            SelectedStagedSource = stagedItems.First(item => item.RequiresDetectionReview);
            StatusText = BuildSessionStatus("Interrupted ingest recovered. Review low-confidence guesses before resuming.");
            return;
        }

        StatusText = BuildSessionStatus($"Resuming interrupted ingest for {stagedItems.Count} file(s)...");
        await IngestFilesAsync(stagedItems.Select(item => item.SourcePath).ToArray());
    }

    private async Task IngestFilesAsync(IReadOnlyList<string> filePaths)
    {
        if (filePaths.Count == 0)
            return;

        if (HasSniffingInProgress) {
            OpenStagingPane();
            StatusText = BuildSessionStatus("Please wait for sniffing to finish.");
            return;
        }

        if (HasPendingDetectionReview) {
            OpenStagingPane();
            SelectedStagedSource = StagedSources.FirstOrDefault(s => s.RequiresDetectionReview) ?? SelectedStagedSource;
            StatusText = BuildSessionStatus("Review low-confidence guesses before ingesting.");
            return;
        }

        var requestedPaths = filePaths
            .Where(path => !string.IsNullOrWhiteSpace(path))
            .Select(SourcePathHelper.Normalize)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (requestedPaths.Length == 0)
            return;

        var wasEmptySession = RecordCount == 0;

        IsIngesting = true;
        _archiveEntryProgress.Clear();
        _activeIngestPaths.Clear();
        foreach (var path in requestedPaths)
            _activeIngestPaths.Add(path);

        ResetPerItemProgress(requestedPaths);
        ResetOverallProgress();
        StatusText = BuildSessionStatus($"Ingesting {requestedPaths.Length} staged source(s)...");

        try {
            var conn = await GetSessionConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);

            var defaultTz = BuildDefaultTimeBasisConfig();
            var planner = new FileIngestPlanner(conn);
            var plan = await planner.PlanAsync(requestedPaths, ExistingFileAction, CancellationToken.None);

            var skippedPaths = new HashSet<string>(
                plan.SkippedFiles.Select(skip => SourcePathHelper.Normalize(skip.SourcePath)),
                StringComparer.OrdinalIgnoreCase);
            var filesToIngest = new HashSet<string>(
                plan.FilesToIngest.Select(SourcePathHelper.Normalize),
                StringComparer.OrdinalIgnoreCase);
            var reingestRequestedPaths = requestedPaths
                .Where(path => !skippedPaths.Contains(path) && !filesToIngest.Contains(path))
                .ToArray();

            foreach (var skipped in plan.SkippedFiles) {
                if (TryFindStagedSource(skipped.SourcePath, out var skippedItem)) {
                    skippedItem.ApplyProgress(
                        phase: nameof(IngestFilePhase.Skipped),
                        bytesProcessed: skippedItem.BytesProcessed,
                        bytesTotal: skippedItem.BytesTotal,
                        recordsProcessed: skippedItem.RecordsProcessed,
                        message: skipped.Reason);
                }
            }

            foreach (var reingestPath in reingestRequestedPaths) {
                if (TryFindStagedSource(reingestPath, out var reingestItem)) {
                    reingestItem.ApplyProgress(
                        phase: nameof(IngestFilePhase.Ingesting),
                        bytesProcessed: 0,
                        bytesTotal: 0,
                        recordsProcessed: 0,
                        message: "Re-ingesting existing segment...");
                }
            }

            var successfulReingests = 0;
            var reingestFailures = 0;
            foreach (var segmentId in plan.SegmentsToReingest) {
                var reingest = new ReingestService(conn);
                var reingestResult = await reingest.ReingestSegmentAsync(
                    segmentId,
                    defaultTz,
                    CancellationToken.None);
                if (!reingestResult.Success) {
                    StatusText = BuildSessionStatus($"Re-ingest failed for segment {segmentId}: {reingestResult.Error}");
                    reingestFailures++;
                } else {
                    successfulReingests++;
                }
            }

            if (plan.SegmentsToReingest.Count > 0)
                RecordCount = await ReadRecordCountAsync(conn);

            foreach (var reingestPath in reingestRequestedPaths) {
                if (TryFindStagedSource(reingestPath, out var reingestItem)) {
                    reingestItem.ApplyProgress(
                        phase: nameof(IngestFilePhase.Completed),
                        bytesProcessed: reingestItem.BytesProcessed,
                        bytesTotal: reingestItem.BytesTotal,
                        recordsProcessed: reingestItem.RecordsProcessed,
                        message: "Re-ingested.");
                }
            }

            if (plan.FilesToIngest.Count == 0 && plan.SegmentsToReingest.Count > 0) {
                RecordCount = await ReadRecordCountAsync(conn);
                await RefreshBrowseAsync(resetFilters: wasEmptySession, invalidateCache: true);
                AutoHideStagingPaneIfAllCompleted();
                var reingestOnlyDetail = reingestFailures == 0
                    ? $"Re-ingested {successfulReingests} segment(s)."
                    : $"Re-ingest finished with {successfulReingests} success and {reingestFailures} failure.";
                StatusText = BuildSessionStatus(reingestOnlyDetail);
                return;
            }

            if (plan.FilesToIngest.Count == 0) {
                StatusText = BuildSessionStatus("No new files needed ingestion.");
                return;
            }

            var overrides = BuildDetectionOverrides(plan.FilesToIngest);
            var progress = new Progress<IngestProgressUpdate>(OnIngestProgress);
            var visibility = new Progress<IngestVisibilityUpdate>(OnIngestVisibilityCommitted);
            var orchestrator = new IngestOrchestrator(conn, formatOverrides: overrides);
            _ingestBaselineRecordCount = RecordCount;
            var result = await orchestrator.IngestFilesAsync(plan.FilesToIngest, defaultTz, progress, visibility);

            RecordCount = await ReadRecordCountAsync(conn);

            if (result.Status == "completed") {
                using var globalStore = new GlobalStore();
                foreach (var ingestedPath in plan.FilesToIngest) {
                    if (TryFindStagedSource(ingestedPath, out var ingestedItem)) {
                        ingestedItem.ApplyProgress(
                            phase: nameof(IngestFilePhase.Completed),
                            bytesProcessed: ingestedItem.BytesTotal > 0 ? ingestedItem.BytesTotal : ingestedItem.BytesProcessed,
                            bytesTotal: ingestedItem.BytesTotal,
                            recordsProcessed: ingestedItem.RecordsProcessed,
                            message: "Completed");

                        await PersistFeedbackRuleAsync(globalStore, ingestedItem);
                    }
                }
                await globalStore.AddRecentSessionAsync(_sessionFolder, Title, Description);
            } else {
                using var globalStore = new GlobalStore();
                await globalStore.AddRecentSessionAsync(_sessionFolder, Title, Description);
            }

            await RefreshBrowseAsync(resetFilters: wasEmptySession && result.TotalRows > 0, invalidateCache: true);
            AutoHideStagingPaneIfAllCompleted();

            StatusText = result.Status == "completed"
                ? BuildSessionStatus(
                    reingestFailures == 0
                        ? $"{result.FilesProcessed} file(s) ingested, {successfulReingests} segment(s) re-ingested, {result.TotalRows:N0} rows added."
                        : $"{result.FilesProcessed} file(s) ingested, {successfulReingests} segment(s) re-ingested, {reingestFailures} failed, {result.TotalRows:N0} rows added.")
                : BuildSessionStatus("Ingest failed.");
        } catch (Exception ex) {
            StatusText = BuildSessionStatus($"Ingest error: {ex.Message}");
        } finally {
            lock (_liveBrowseRefreshLock) {
                _liveBrowseRefreshQueued = false;
                _liveBrowseRefreshDirty = false;
            }
            _activeIngestPaths.Clear();
            IsIngesting = false;
            RaiseStagingStateProperties();
            ResetOverallProgress();
        }
    }

    private Dictionary<string, FileFormatOverride> BuildDetectionOverrides(IReadOnlyList<string> filePaths)
    {
        var result = new Dictionary<string, FileFormatOverride>(StringComparer.OrdinalIgnoreCase);
        var byPath = StagedSources
            .Where(s => !s.IsDirectory)
            .ToDictionary(
                s => SourcePathHelper.Normalize(s.SourcePath),
                s => s,
                StringComparer.OrdinalIgnoreCase);

        foreach (var filePath in filePaths) {
            var canonical = SourcePathHelper.Normalize(filePath);
            if (byPath.TryGetValue(canonical, out var item)
                && item.TryBuildSelectedOverride(out var formatOverride)) {
                result[canonical] = formatOverride;
            }
        }

        return result;
    }

    private async Task ReingestSourceAsync(StagedSourceItemViewModel? item)
    {
        if (item is null || item.IsDirectory)
            return;

        if (IsIngesting) {
            StatusText = BuildSessionStatus("Cannot re-ingest while ingestion is active.");
            return;
        }

        IsIngesting = true;
        item.ApplyProgress(
            phase: nameof(IngestFilePhase.Ingesting),
            bytesProcessed: 0,
            bytesTotal: 0,
            recordsProcessed: 0,
            message: "Re-ingesting existing segments...");

        try {
            var conn = await GetSessionConnectionAsync();
            await SchemaInitializer.EnsureSchemaAsync(conn);

            var segmentIds = await ResolveSegmentsForSourceAsync(conn, item.SourcePath);
            if (segmentIds.Count == 0) {
                item.ApplyProgress(
                    phase: nameof(IngestFilePhase.Failed),
                    bytesProcessed: 0,
                    bytesTotal: 0,
                    recordsProcessed: 0,
                    message: "No ingested segments found.");
                StatusText = BuildSessionStatus($"No ingested segments found for {Path.GetFileName(item.SourcePath)}.");
                return;
            }

            var defaultTz = BuildDefaultTimeBasisConfig();
            var service = new ReingestService(conn);
            var formatOverride = item.TryBuildSelectedOverride(out var selectedOverride)
                ? selectedOverride
                : null;
            var ok = 0;
            var failed = 0;
            long replacedRows = 0;

            foreach (var segmentId in segmentIds) {
                var result = await service.ReingestSegmentAsync(
                    segmentId,
                    defaultTz,
                    CancellationToken.None,
                    formatOverride: formatOverride);
                if (result.Success) {
                    ok++;
                    replacedRows += result.NewRowCount;
                } else {
                    failed++;
                    StatusText = BuildSessionStatus($"Re-ingest failed for segment {segmentId}: {result.Error}");
                }
            }

            RecordCount = await ReadRecordCountAsync(conn);
            await RefreshBrowseAsync(resetFilters: false, invalidateCache: true);

            item.ApplyProgress(
                phase: failed == 0 ? nameof(IngestFilePhase.Completed) : nameof(IngestFilePhase.Failed),
                bytesProcessed: item.BytesProcessed,
                bytesTotal: item.BytesTotal,
                recordsProcessed: replacedRows,
                message: failed == 0 ? "Re-ingested." : "Re-ingest completed with failures.");

            if (failed == 0) {
                using var globalStore = new GlobalStore();
                await PersistFeedbackRuleAsync(globalStore, item);
            }

            StatusText = failed == 0
                ? BuildSessionStatus($"Re-ingested {ok} segment(s), refreshing {replacedRows:N0} rows.")
                : BuildSessionStatus($"Re-ingest finished with {ok} success and {failed} failure.");
        } finally {
            IsIngesting = false;
        }
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
        var canonical = SourcePathHelper.Normalize(sourcePath);
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

    private void QueueSniff(IReadOnlyList<StagedSourceItemViewModel> items)
    {
        foreach (var item in items.Where(item => !item.IsDirectory)) {
            item.MarkSniffing();
            _ = Task.Run(async () => await SniffItemWithGateAsync(item));
        }

        RaiseStagingStateProperties();
    }

    private Task SniffStagedSourcesAsync(IReadOnlyList<StagedSourceItemViewModel> items)
    {
        var sniffable = items.Where(item => !item.IsDirectory).ToArray();
        foreach (var item in sniffable)
            item.MarkSniffing();

        RaiseStagingStateProperties();
        return Task.WhenAll(sniffable.Select(SniffItemWithGateAsync));
    }

    private async Task SniffItemWithGateAsync(StagedSourceItemViewModel item)
    {
        await _sniffConcurrency.WaitAsync();
        try {
            await RunSniffAsync(item);
        } finally {
            _sniffConcurrency.Release();
            RunOnUi(RaiseStagingStateProperties);
        }
    }

    private async Task RunSniffAsync(StagedSourceItemViewModel item)
    {
        var sourcePath = SourcePathHelper.Normalize(item.SourcePath);

        try {
            List<DetectionChoiceViewModel> ranked;
            var isArchive = false;
            if (SourcePathHelper.TrySplitArchiveEntry(sourcePath, out var archivePath, out var entryFullName)) {
                if (!File.Exists(archivePath)) {
                    RunOnUi(() => item.MarkDetectionFailed("Archive file not found."));
                    return;
                }

                ranked = await BuildRankedDetectionChoicesAsync(
                    ZipHandler.OpenRead(archivePath, entryFullName),
                    Path.GetFileName(entryFullName));
            } else {
                if (!File.Exists(sourcePath)) {
                    RunOnUi(() => item.MarkDetectionFailed("File not found."));
                    return;
                }

                isArchive = SourcePathHelper.IsArchiveFilePath(sourcePath);
                ranked = isArchive
                    ? await BuildArchivePreviewChoicesAsync(sourcePath)
                    : await BuildRankedDetectionChoicesAsync(File.OpenRead(sourcePath), Path.GetFileName(sourcePath));
            }

            using var globalStore = new GlobalStore();
            var templateKey = FeedbackTemplateKeyBuilder.BuildKey(sourcePath);
            var feedbackRules = await globalStore.GetFeedbackRulesAsync(sourcePath, "detection");
            var feedbackSuggestions = feedbackRules
                .Select(BuildFeedbackSuggestion)
                .ToArray();

            if (ranked.Count == 0) {
                RunOnUi(() => item.MarkDetectionFailed(
                    isArchive
                        ? "Archive contains no sniffable log entries."
                        : "No viable format candidates."));
                return;
            }

            var top = ranked[0];
            var closeSecond = !isArchive && ranked.Count > 1 && Math.Abs(ranked[0].Confidence - ranked[1].Confidence) < 0.02;
            var needsReview = !isArchive && (top.Confidence < AutoIngestConfidenceThreshold || closeSecond);
            var statusMessage = isArchive
                ? $"Archive preview ready ({ranked.Count} entr{(ranked.Count == 1 ? "y" : "ies")} sniffed). Select an entry to inspect its guessed profile."
                : needsReview
                    ? $"Low confidence guess ({top.ConfidenceDisplay}) for {top.FormatName}. Review before ingesting."
                    : $"Detected {top.FormatName} ({top.ConfidenceDisplay}).";
            if (feedbackSuggestions.Length > 0)
                statusMessage = $"{statusMessage} {feedbackSuggestions.Length} learned suggestion{(feedbackSuggestions.Length == 1 ? string.Empty : "s")} available.";

            RunOnUi(() => {
                item.ApplyDetectionChoices(
                    ranked,
                    needsReview,
                    statusMessage);
                item.ApplyFeedbackSuggestions(templateKey, feedbackSuggestions);

                if (item.RequiresDetectionReview) {
                    OpenStagingPane();
                    SelectedStagedSource = item;
                }
            });
        } catch (Exception ex) {
            RunOnUi(() => item.MarkDetectionFailed(ex.Message));
        }
    }

    private async Task<List<DetectionChoiceViewModel>> BuildRankedDetectionChoicesAsync(Stream stream, string sourceName)
    {
        await using var ownedStream = stream;
        var sniffBuffer = await StreamSampling.ReadPrefixAsync(stream, 128 * 1024);
        using var encodingStream = new MemoryStream(sniffBuffer, writable: false);
        var encoding = EncodingDetector.Detect(encodingStream);
        using var sniff = new MemoryStream(sniffBuffer, writable: false);
        var sampleLines = CsvPreviewHelper.ReadNonEmptyLines(sniffBuffer, sniffBuffer.Length, encoding, 32);

        var candidates = _detectionEngine.DetectCandidates(sniff, sourceName);
        return candidates
            .Select(candidate => BuildDetectionChoice(candidate, encoding, sampleLines))
            .OrderByDescending(choice => choice.Confidence)
            .ToList();
    }

    private async Task<List<DetectionChoiceViewModel>> BuildArchivePreviewChoicesAsync(string zipPath)
    {
        var choices = new List<DetectionChoiceViewModel>();
        foreach (var entry in ZipHandler.EnumerateEntries(zipPath).Take(12)) {
            var ranked = await BuildRankedDetectionChoicesAsync(
                ZipHandler.OpenRead(zipPath, entry.EntryName),
                entry.EntryName);
            var top = ranked.FirstOrDefault();
            if (top is null)
                continue;

            choices.Add(BuildArchivePreviewChoice(zipPath, entry, top));
        }

        return choices;
    }

    private static DetectionChoiceViewModel BuildArchivePreviewChoice(
        string zipPath,
        ZipFileEntry entry,
        DetectionChoiceViewModel choice)
    {
        var details = new List<DetectionDetailViewModel>
        {
            new("Archive", Path.GetFileName(zipPath)),
            new("Entry", entry.EntryName)
        };
        details.AddRange(choice.Details);

        return new DetectionChoiceViewModel(
            FormatName: choice.FormatName,
            ShortLabel: entry.EntryName,
            Summary: $"{entry.EntryName} · {choice.Summary}",
            Confidence: choice.Confidence,
            Detection: choice.Detection,
            Details: details,
            Notes: choice.Notes,
            EncodingCodePage: choice.EncodingCodePage,
            EncodingDisplay: choice.EncodingDisplay,
            SampleLines: choice.SampleLines);
    }

    private DetectionChoiceViewModel BuildDetectionChoice(
        DetectionResult detection,
        Encoding encoding,
        IReadOnlyList<string> sampleLines)
    {
        var formatName = FormatBoundaryName(detection.Boundary);
        var shortLabel = BuildShortLabel(detection);
        var sample = TryReadSampleTimestamp(detection, sampleLines);
        var details = BuildDetectionDetails(detection, encoding, sample);
        var summary = BuildDetectionSummary(formatName, shortLabel, encoding, sample);

        return new DetectionChoiceViewModel(
            FormatName: formatName,
            ShortLabel: shortLabel,
            Summary: summary,
            Confidence: detection.Confidence,
            Detection: detection,
            Details: details,
            Notes: detection.Notes,
            EncodingCodePage: encoding.CodePage,
            EncodingDisplay: EncodingDetector.Describe(encoding),
            SampleLines: sampleLines);
    }

    private IReadOnlyList<DetectionDetailViewModel> BuildDetectionDetails(
        DetectionResult detection,
        Encoding encoding,
        SampleTimestampInfo? sample)
    {
        var details = new List<DetectionDetailViewModel>
        {
            new("Encoding", EncodingDetector.Describe(encoding)),
            new("Timezone", DescribeTimezoneHandling(sample))
        };

        switch (detection.Boundary) {
            case CsvBoundary csv:
                details.Add(new("Structure", "CSV"));
                details.Add(new("Delimiter", DescribeDelimiter(csv.Delimiter)));
                details.Add(new("Header", csv.HasHeader ? "Detected" : "No header"));
                if (csv.ColumnNames is { Length: > 0 }) {
                    details.Add(new(
                        "Columns",
                        csv.ColumnNames.Length <= 4
                            ? string.Join(", ", csv.ColumnNames)
                            : $"{string.Join(", ", csv.ColumnNames.Take(4))}, ... ({csv.ColumnNames.Length} total)"));
                }
                details.Add(new("Timestamp field", ExtractTimestampLabel(detection)));
                break;

            case JsonNdBoundary json:
                details.Add(new("Structure", "NDJSON"));
                details.Add(new("Timestamp field", json.TimestampFieldPath ?? ExtractTimestampLabel(detection)));
                break;

            case TextSoRBoundary text:
                details.Add(new("Structure", "Text"));
                details.Add(new("Timestamp pattern", text.PatternName ?? ExtractTimestampLabel(detection)));
                break;
        }

        if (sample is not null) {
            details.Add(new("Timestamp sample", sample.ParsedText));
            if (!string.IsNullOrWhiteSpace(sample.AlternateText))
                details.Add(new("Alternate sample", sample.AlternateText!));
            details.Add(new("Offset in sample", sample.HasExplicitOffset ? "Explicit offset / Z preserved" : "Bare timestamp"));
            if (sample.UsedTwoDigitYear)
                details.Add(new("Parsing note", "Two-digit year window applied"));
        }

        return details;
    }

    private string BuildDetectionSummary(
        string formatName,
        string shortLabel,
        Encoding encoding,
        SampleTimestampInfo? sample)
    {
        var parts = new List<string> { formatName };
        if (!string.IsNullOrWhiteSpace(shortLabel))
            parts.Add(shortLabel);

        parts.Add(EncodingDetector.Describe(encoding));
        parts.Add(sample?.HasExplicitOffset == true ? "source offset preserved" : DescribeSessionTimezone());
        return string.Join(" · ", parts);
    }

    private SampleTimestampInfo? TryReadSampleTimestamp(
        DetectionResult detection,
        IReadOnlyList<string> sampleLines)
    {
        if (detection.Extractor is not ITimestampExtractorWithMetadata extractorWithMetadata)
            return null;

        var raw = TryBuildSampleRecord(detection, sampleLines);
        if (raw is null)
            return null;

        if (!extractorWithMetadata.TryExtractWithMetadata(raw, out var extraction))
            return null;

        return new SampleTimestampInfo(
            extraction.ParsedText,
            extraction.AlternateText,
            extraction.HasExplicitOffset,
            extraction.UsedTwoDigitYear);
    }

    private static RawRecord? TryBuildSampleRecord(
        DetectionResult detection,
        IReadOnlyList<string> lines)
    {
        if (lines.Count == 0)
            return null;

        return detection.Boundary switch {
            TextSoRBoundary => new RawRecord(lines[0], lines[0], 1, 0),
            JsonNdBoundary => BuildJsonSample(lines),
            CsvBoundary csv => BuildCsvSample(lines, csv),
            _ => null
        };
    }

    private static RawRecord? BuildJsonSample(IReadOnlyList<string> lines)
    {
        foreach (var line in lines) {
            if (!LooksLikeJsonObject(line))
                continue;

            try {
                using var doc = JsonDocument.Parse(line);
                if (doc.RootElement.ValueKind != JsonValueKind.Object)
                    continue;

                var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (var prop in doc.RootElement.EnumerateObject())
                    fields[prop.Name] = prop.Value.ValueKind == JsonValueKind.String
                        ? prop.Value.GetString() ?? ""
                        : prop.Value.GetRawText();

                return new RawRecord(line, line, 1, 0, fields);
            } catch {
                // ignore malformed preview rows
            }
        }

        return null;
    }

    private static bool LooksLikeJsonObject(string line)
    {
        var trimmed = line.Trim();
        return trimmed.Length >= 2
            && trimmed[0] == '{'
            && trimmed[^1] == '}'
            && trimmed.Contains(':');
    }

    private static RawRecord? BuildCsvSample(IReadOnlyList<string> lines, CsvBoundary csv)
    {
        var dataIndex = csv.HasHeader ? 1 : 0;
        if (lines.Count <= dataIndex)
            return null;

        var dataLine = lines[dataIndex];
        var columnNames = csv.ColumnNames;
        if ((columnNames is null || columnNames.Length == 0) && csv.HasHeader && lines.Count > 0)
            columnNames = CsvPreviewHelper.SplitLine(lines[0], csv.Delimiter, csv.Quote);

        if (columnNames is null || columnNames.Length == 0)
            return new RawRecord(dataLine, dataLine, dataIndex + 1, 0);

        var values = CsvPreviewHelper.SplitLine(dataLine, csv.Delimiter, csv.Quote);
        var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < columnNames.Length && i < values.Length; i++)
            fields[columnNames[i]] = values[i];

        return new RawRecord(dataLine, dataLine, dataIndex + 1, 0, fields);
    }

    private string BuildShortLabel(DetectionResult detection) =>
        detection.Boundary switch {
            CsvBoundary => ExtractTimestampLabel(detection),
            JsonNdBoundary json => json.TimestampFieldPath ?? ExtractTimestampLabel(detection),
            TextSoRBoundary text => text.PatternName ?? ExtractTimestampLabel(detection),
            _ => detection.Extractor.Description
        };

    private static string ExtractTimestampLabel(DetectionResult detection)
    {
        var description = detection.Extractor.Description;
        if (description.StartsWith("CsvField(", StringComparison.Ordinal))
            return description["CsvField(".Length..^1];
        if (description.StartsWith("CsvComposite(", StringComparison.Ordinal))
            return description["CsvComposite(".Length..^1].Replace("+", " + ", StringComparison.Ordinal);
        if (description.StartsWith("JsonField(", StringComparison.Ordinal))
            return description["JsonField(".Length..^1];
        if (description.StartsWith("RegexGroup(", StringComparison.Ordinal))
            return description["RegexGroup(".Length..^1];

        return description;
    }

    private string DescribeTimezoneHandling(SampleTimestampInfo? sample)
    {
        if (sample?.HasExplicitOffset == true)
            return "Source offset preserved";

        var timeBasis = BuildDefaultTimeBasisConfig();
        return timeBasis.Basis switch {
            TimeBasis.Utc => "Bare timestamps use UTC",
            TimeBasis.Zone when !string.IsNullOrWhiteSpace(timeBasis.TimeZoneId) =>
                $"Bare timestamps use {timeBasis.TimeZoneId}",
            TimeBasis.FixedOffset when timeBasis.OffsetMinutes.HasValue =>
                $"Bare timestamps use UTC{FormatOffset(timeBasis.OffsetMinutes.Value)}",
            _ => $"Bare timestamps use {TimeZoneInfo.Local.Id}"
        };
    }

    private static string DescribeDelimiter(char delimiter) =>
        delimiter == '\t' ? "Tab" : delimiter.ToString();

    private static string FormatOffset(int offsetMinutes)
    {
        var offset = TimeSpan.FromMinutes(offsetMinutes);
        return $"{(offset >= TimeSpan.Zero ? "+" : "-")}{Math.Abs(offset.Hours):00}:{Math.Abs(offset.Minutes):00}";
    }

    private static string FormatBoundaryName(RecordBoundarySpec boundary) =>
        boundary switch {
            CsvBoundary => "CSV",
            JsonNdBoundary => "NDJSON",
            TextSoRBoundary => "Text",
            _ => boundary.GetType().Name.Replace("Boundary", "", StringComparison.Ordinal)
        };

    private static void RunOnUi(Action action)
    {
        RxApp.MainThreadScheduler.Schedule(Unit.Default, (_, _) => {
            action();
            return Disposable.Empty;
        });
    }

    private static Task<T> RunOnUiAsync<T>(Func<T> action)
    {
        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        RxApp.MainThreadScheduler.Schedule(Unit.Default, (_, _) => {
            try {
                tcs.TrySetResult(action());
            } catch (Exception ex) {
                tcs.TrySetException(ex);
            }

            return Disposable.Empty;
        });

        return tcs.Task;
    }

    private void RaiseStagingStateProperties()
    {
        this.RaisePropertyChanged(nameof(HasPendingDetectionReview));
        this.RaisePropertyChanged(nameof(HasInvalidDetectionOverrides));
        this.RaisePropertyChanged(nameof(HasSniffingInProgress));
        this.RaisePropertyChanged(nameof(HasStagedSources));
        this.RaisePropertyChanged(nameof(HasRunnableStagedSources));
        this.RaisePropertyChanged(nameof(CanStartIngestion));
        this.RaisePropertyChanged(nameof(CanModifyStagedSources));
        this.RaisePropertyChanged(nameof(StagingSummary));
        this.RaisePropertyChanged(nameof(StagingPaneToggleText));
        this.RaisePropertyChanged(nameof(CanConfirmSelectedDetection));
        this.RaisePropertyChanged(nameof(CanRemoveSelectedStagedSource));
        this.RaisePropertyChanged(nameof(CanReingestSelectedSource));
    }

    private TimeBasisConfig BuildDefaultTimeBasisConfig()
    {
        var tz = SessionDefaults.ResolveDefaultTimezone(DefaultTimezone);
        if (!SessionDefaults.IsValidTimezoneId(tz))
            return new TimeBasisConfig(TimeBasis.Local);

        if (string.Equals(tz, TimeZoneInfo.Local.Id, StringComparison.OrdinalIgnoreCase))
            return new TimeBasisConfig(TimeBasis.Local);

        if (string.Equals(tz, "UTC", StringComparison.OrdinalIgnoreCase)
            || string.Equals(tz, "Etc/UTC", StringComparison.OrdinalIgnoreCase)) {
            return new TimeBasisConfig(TimeBasis.Utc);
        }

        return new TimeBasisConfig(TimeBasis.Zone, TimeZoneId: tz);
    }

    private string DescribeSessionTimezone()
    {
        var timeBasis = BuildDefaultTimeBasisConfig();
        return timeBasis.Basis switch {
            TimeBasis.Utc => "UTC",
            TimeBasis.Zone when !string.IsNullOrWhiteSpace(timeBasis.TimeZoneId) => timeBasis.TimeZoneId!,
            TimeBasis.FixedOffset when timeBasis.OffsetMinutes.HasValue => $"UTC{FormatOffset(timeBasis.OffsetMinutes.Value)}",
            _ => TimeZoneInfo.Local.Id
        };
    }

    private void AttachStagedSource(StagedSourceItemViewModel item) =>
        item.PropertyChanged += OnStagedSourcePropertyChanged;

    private void DetachStagedSource(StagedSourceItemViewModel item) =>
        item.PropertyChanged -= OnStagedSourcePropertyChanged;

    private void OnStagedSourcePropertyChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName is nameof(StagedSourceItemViewModel.RequiresDetectionReview)
            or nameof(StagedSourceItemViewModel.HasOverrideValidationError)
            or nameof(StagedSourceItemViewModel.IsSniffing)
            or nameof(StagedSourceItemViewModel.SelectedDetectionChoice)
            or nameof(StagedSourceItemViewModel.Phase)) {
            RaiseStagingStateProperties();
        }
    }

    private List<StagedSourceItemViewModel> StagePathsCore(
        IReadOnlyList<string> paths,
        bool openPane,
        bool queueSniff) =>
        StageExpandedPathsCore(ExpandInputPaths(paths).ToArray(), openPane, queueSniff);

    private List<StagedSourceItemViewModel> StageExpandedPathsCore(
        IReadOnlyList<string> expandedPaths,
        bool openPane,
        bool queueSniff)
    {
        var stagedItems = new List<StagedSourceItemViewModel>();
        var newlyAdded = new List<StagedSourceItemViewModel>();

        foreach (var fullPath in expandedPaths) {
            var existing = StagedSources.FirstOrDefault(item =>
                string.Equals(SourcePathHelper.Normalize(item.SourcePath), fullPath, StringComparison.OrdinalIgnoreCase));
            if (existing is not null) {
                stagedItems.Add(existing);
                continue;
            }

            var item = new StagedSourceItemViewModel(fullPath, isDirectory: false);
            StagedSources.Add(item);
            AttachStagedSource(item);
            stagedItems.Add(item);
            newlyAdded.Add(item);
        }

        if (openPane && stagedItems.Count > 0)
            OpenStagingPane();

        if (SelectedStagedSource is null && stagedItems.Count > 0)
            SelectedStagedSource = stagedItems[0];
        else if (newlyAdded.Count > 0)
            SelectedStagedSource = newlyAdded[0];

        if (queueSniff && newlyAdded.Count > 0)
            QueueSniff(newlyAdded);

        RaiseStagingStateProperties();
        return stagedItems;
    }

    private async Task StagePathsAsync(IReadOnlyList<string> paths)
    {
        if (paths.Count == 0) {
            StatusText = BuildSessionStatus("No files were staged.");
            return;
        }

        OpenStagingPane();
        StatusText = BuildSessionStatus($"Staging {paths.Count} path(s)...");
        await Task.Yield();

        var expandedPaths = await Task.Run(() => ExpandInputPaths(paths).ToArray());
        var stagedItems = await RunOnUiAsync(() => StageExpandedPathsCore(expandedPaths, openPane: false, queueSniff: true));
        if (stagedItems.Count == 0) {
            await RunOnUiAsync(() => {
                StatusText = BuildSessionStatus("No files were staged.");
                return 0;
            });
            return;
        }

        await RunOnUiAsync(() => {
            StatusText = BuildSessionStatus($"Staged {stagedItems.Count} file(s).");
            return 0;
        });
    }

    private static IEnumerable<string> ExpandInputPaths(IReadOnlyList<string> inputPaths)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var rawPath in inputPaths.Where(path => !string.IsNullOrWhiteSpace(path))) {
            if (SourcePathHelper.TrySplitArchiveEntry(rawPath, out var archivePath, out var entryFullName)) {
                var entryPath = SourcePathHelper.CombineArchiveEntryPath(archivePath, entryFullName);
                if (seen.Add(entryPath))
                    yield return entryPath;
                continue;
            }

            var fullPath = Path.GetFullPath(rawPath);
            if (File.Exists(fullPath)) {
                if (SourcePathHelper.IsArchiveFilePath(fullPath)) {
                    IEnumerable<ZipFileEntry> entries;
                    try {
                        entries = ZipHandler.EnumerateEntries(fullPath).ToArray();
                    } catch {
                        continue;
                    }

                    foreach (var entry in entries) {
                        if (seen.Add(entry.SourcePath))
                            yield return entry.SourcePath;
                    }
                } else if (seen.Add(fullPath)) {
                    yield return fullPath;
                }
                continue;
            }

            if (Directory.Exists(fullPath)) {
                IEnumerable<string> files;
                try {
                    files = Directory.EnumerateFiles(fullPath, "*", SearchOption.AllDirectories);
                } catch {
                    continue;
                }

                foreach (var file in files) {
                    var expanded = Path.GetFullPath(file);
                    if (SourcePathHelper.IsArchiveFilePath(expanded)) {
                        IEnumerable<ZipFileEntry> entries;
                        try {
                            entries = ZipHandler.EnumerateEntries(expanded).ToArray();
                        } catch {
                            continue;
                        }

                        foreach (var entry in entries) {
                            if (seen.Add(entry.SourcePath))
                                yield return entry.SourcePath;
                        }

                        continue;
                    }

                    if (seen.Add(expanded))
                        yield return expanded;
                }

                continue;
            }

            if (seen.Add(fullPath))
                yield return fullPath;
        }
    }

    private void RemoveStagedSource(StagedSourceItemViewModel? item)
    {
        if (item is null || !item.CanEdit)
            return;

        var index = StagedSources.IndexOf(item);
        DetachStagedSource(item);
        StagedSources.Remove(item);

        if (ReferenceEquals(SelectedStagedSource, item)) {
            SelectedStagedSource = StagedSources.Count == 0
                ? null
                : StagedSources[Math.Clamp(index, 0, StagedSources.Count - 1)];
        }

        RaiseStagingStateProperties();
    }

    private void ClearStagedSources()
    {
        var removable = StagedSources.Where(s => s.CanEdit).ToList();
        foreach (var item in removable) {
            DetachStagedSource(item);
            StagedSources.Remove(item);
        }

        if (SelectedStagedSource is not null && !StagedSources.Contains(SelectedStagedSource))
            SelectedStagedSource = StagedSources.FirstOrDefault();

        RaiseStagingStateProperties();
    }

    private async Task StartIngestionAsync()
    {
        if (IsIngesting)
            return;

        if (HasPendingDetectionReview) {
            OpenStagingPane();
            SelectedStagedSource = StagedSources.FirstOrDefault(source => source.RequiresDetectionReview);
            StatusText = BuildSessionStatus("Review low-confidence guesses before ingesting.");
            return;
        }

        var stagedPaths = StagedSources
            .Where(source => source.Phase != nameof(IngestFilePhase.Completed))
            .Select(source => source.SourcePath)
            .ToArray();
        if (stagedPaths.Length == 0) {
            StatusText = BuildSessionStatus("No staged files are waiting to ingest.");
            return;
        }

        await IngestFilesAsync(stagedPaths);
        RaiseStagingStateProperties();
    }

    private void ToggleStagingPane() => IsStagingPaneOpen = !IsStagingPaneOpen;

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
        if (!SessionDefaults.IsValidTimezoneId(timezone)) {
            StatusText = BuildSessionStatus($"Invalid timezone '{timezone}'.");
            return;
        }

        var store = new SessionStore(GetSessionFactory());
        await store.UpdateHeaderAsync(title: title, description: description, defaultTimezone: timezone);

        Title = title;
        Description = description;
        DefaultTimezone = timezone;
        IsEditingHeader = false;

        using var globalStore = new GlobalStore();
        await globalStore.AddRecentSessionAsync(_sessionFolder, Title, Description);

        if (LogsPage is not null)
            LogsPage.SetDisplayTimezone(DefaultTimezone);

        StatusText = BuildSessionStatus("Session header updated.");
    }

    private void CancelHeaderEdit() => IsEditingHeader = false;

    private void OnIngestProgress(IngestProgressUpdate update)
    {
        var sourcePath = SourcePathHelper.Normalize(update.SourcePath);
        var isArchiveEntry = SourcePathHelper.TrySplitArchiveEntry(sourcePath, out var archivePath, out _);
        archivePath ??= sourcePath;

        var item = StagedSources.FirstOrDefault(staged =>
            string.Equals(SourcePathHelper.Normalize(staged.SourcePath), sourcePath, StringComparison.OrdinalIgnoreCase)
            || string.Equals(SourcePathHelper.Normalize(staged.SourcePath), archivePath, StringComparison.OrdinalIgnoreCase));
        if (item is not null) {
            if (isArchiveEntry && string.Equals(SourcePathHelper.Normalize(item.SourcePath), archivePath, StringComparison.OrdinalIgnoreCase)) {
                if (!_archiveEntryProgress.TryGetValue(archivePath, out var entryUpdates)) {
                    entryUpdates = new Dictionary<string, IngestProgressUpdate>(StringComparer.OrdinalIgnoreCase);
                    _archiveEntryProgress[archivePath] = entryUpdates;
                }

                entryUpdates[sourcePath] = update;
                var aggregate = AggregateArchiveProgress(entryUpdates.Values, update.Message);
                item.ApplyProgress(
                    phase: aggregate.Phase,
                    bytesProcessed: aggregate.BytesProcessed,
                    bytesTotal: aggregate.BytesTotal,
                    recordsProcessed: aggregate.RecordsProcessed,
                    message: aggregate.Message);
            } else {
                item.ApplyProgress(
                    phase: update.Phase.ToString(),
                    bytesProcessed: update.BytesProcessed,
                    bytesTotal: update.BytesTotal,
                    recordsProcessed: update.RecordsProcessed,
                    message: update.Message);
            }
        }

        RecalculateOverallProgress();
    }

    private void OnIngestVisibilityCommitted(IngestVisibilityUpdate update)
    {
        if (!IsIngesting || update.RowsCommitted <= 0)
            return;

        RecordCount = _ingestBaselineRecordCount + update.TotalRowsCommitted;
        RequestLiveBrowseRefresh();
    }

    private void ResetPerItemProgress(IReadOnlyList<string> filePaths)
    {
        var canonicalPaths = new HashSet<string>(filePaths.Select(SourcePathHelper.Normalize), StringComparer.OrdinalIgnoreCase);
        foreach (var item in StagedSources.Where(item => canonicalPaths.Contains(SourcePathHelper.Normalize(item.SourcePath)))) {
            _archiveEntryProgress.Remove(SourcePathHelper.Normalize(item.SourcePath));
            item.ApplyProgress(
                phase: nameof(IngestFilePhase.Queued),
                bytesProcessed: 0,
                bytesTotal: 0,
                recordsProcessed: 0,
                message: "Queued");
        }
    }

    private void RecalculateOverallProgress()
    {
        if (_activeIngestPaths.Count == 0)
            return;

        var activeItems = StagedSources
            .Where(item => _activeIngestPaths.Contains(SourcePathHelper.Normalize(item.SourcePath)))
            .ToArray();
        if (activeItems.Length == 0)
            return;

        var totalBytesProcessed = activeItems.Sum(item => item.BytesProcessed);
        var totalBytesTotal = activeItems.Sum(item => item.BytesTotal);
        var totalRecords = activeItems.Sum(item => item.RecordsProcessed);
        OverallBytesProcessed = totalBytesProcessed;
        OverallBytesTotal = totalBytesTotal;
        var now = DateTimeOffset.UtcNow;

        if (!_overallProgressStartedUtc.HasValue && (totalBytesProcessed > 0 || totalRecords > 0))
            _overallProgressStartedUtc = now;

        if (_overallProgressStartedUtc.HasValue) {
            var elapsed = (now - _overallProgressStartedUtc.Value).TotalSeconds;
            if (elapsed >= MinStableRateWindow.TotalSeconds && totalRecords > 0)
                OverallRecordsPerSecond = totalRecords / elapsed;
            else
                OverallRecordsPerSecond = 0;

            var bytesPerSecond = elapsed >= MinStableRateWindow.TotalSeconds && totalBytesProcessed > 0
                ? totalBytesProcessed / elapsed
                : 0;

            if (bytesPerSecond > 0 && totalBytesTotal > 0 && totalBytesProcessed > 0 && totalBytesProcessed < totalBytesTotal)
                OverallEtaSeconds = Math.Max((totalBytesTotal - totalBytesProcessed) / bytesPerSecond, 1);
            else
                OverallEtaSeconds = null;
        } else {
            OverallRecordsPerSecond = 0;
            OverallEtaSeconds = null;
        }

        this.RaisePropertyChanged(nameof(OverallProgressDisplay));
        this.RaisePropertyChanged(nameof(OverallThroughputDisplay));
        this.RaisePropertyChanged(nameof(OverallEtaDisplay));
    }

    private void ResetOverallProgress()
    {
        _overallProgressStartedUtc = null;
        OverallRecordsPerSecond = 0;
        OverallEtaSeconds = null;
        OverallBytesProcessed = 0;
        OverallBytesTotal = 0;
        this.RaisePropertyChanged(nameof(OverallProgressDisplay));
        this.RaisePropertyChanged(nameof(OverallThroughputDisplay));
        this.RaisePropertyChanged(nameof(OverallEtaDisplay));
    }

    private async Task RefreshBrowseAsync(bool resetFilters, bool invalidateCache)
    {
        if (LogsPage is null || Timeline is null || FacetPanel is null)
            return;

        if (resetFilters) {
            Timeline.ClearSelection();
            await Timeline.LoadCoarseBinsAsync();
        } else {
            await Timeline.RefreshDataAsync(preserveViewport: true);
            LogsPage.StartUtc = Timeline.SelectedStart;
            LogsPage.EndUtc = Timeline.SelectedEnd;
            FacetPanel.FilterStart = Timeline.SelectedStart;
            FacetPanel.FilterEnd = Timeline.SelectedEnd;
        }

        FacetPanel.InvalidateCache();
        if (resetFilters)
            FacetPanel.ResetFilters();

        await FacetPanel.RefreshAsync();

        if (resetFilters)
            await LogsPage.ResetFiltersAndRefreshAsync(invalidateCache);
        else
            await LogsPage.RefreshResultsAsync(invalidateCache, preserveLoadedRowCount: true);
    }

    private void RequestLiveBrowseRefresh()
    {
        lock (_liveBrowseRefreshLock) {
            if (_liveBrowseRefreshQueued) {
                _liveBrowseRefreshDirty = true;
                return;
            }

            _liveBrowseRefreshQueued = true;
            _liveBrowseRefreshDirty = false;
        }

        _ = Task.Run(ProcessPendingLiveBrowseRefreshesAsync);
    }

    private async Task ProcessPendingLiveBrowseRefreshesAsync()
    {
        while (true) {
            var delay = TimeSpan.Zero;
            lock (_liveBrowseRefreshLock) {
                var elapsed = DateTimeOffset.UtcNow - _lastLiveBrowseRefreshUtc;
                if (_lastLiveBrowseRefreshUtc != DateTimeOffset.MinValue
                    && elapsed < MinLiveBrowseRefreshInterval) {
                    delay = MinLiveBrowseRefreshInterval - elapsed;
                }
            }

            if (delay > TimeSpan.Zero)
                await Task.Delay(delay);

            await _liveBrowseRefreshGate.WaitAsync();
            try {
                _lastLiveBrowseRefreshUtc = DateTimeOffset.UtcNow;
                await RefreshBrowseDuringIngestAsync();
            } catch (Exception ex) {
                RunOnUi(() => StatusText = BuildSessionStatus($"Browse refresh failed during ingest: {ex.Message}"));
            } finally {
                _liveBrowseRefreshGate.Release();
            }

            lock (_liveBrowseRefreshLock) {
                if (!_liveBrowseRefreshDirty || !IsIngesting) {
                    _liveBrowseRefreshQueued = false;
                    _liveBrowseRefreshDirty = false;
                    return;
                }

                _liveBrowseRefreshDirty = false;
            }
        }
    }

    private async Task RefreshBrowseDuringIngestAsync()
    {
        await RefreshBrowseAsync(resetFilters: false, invalidateCache: true);
    }

    private DuckLakeConnectionFactory GetSessionFactory()
    {
        _sessionFactory ??= new DuckLakeConnectionFactory(SessionPaths.GetDbPath(_sessionFolder));
        return _sessionFactory;
    }

    private async Task<DuckDBConnection> GetSessionConnectionAsync(CancellationToken ct = default)
        => await GetSessionFactory().GetConnectionAsync(ct);

    private void AutoHideStagingPaneIfAllCompleted()
    {
        if (StagedSources.Count > 0 && StagedSources.All(item => item.Phase == nameof(IngestFilePhase.Completed)))
            IsStagingPaneOpen = false;
    }

    private static (string Phase, long BytesProcessed, long BytesTotal, long RecordsProcessed, string? Message) AggregateArchiveProgress(
        IEnumerable<IngestProgressUpdate> updates,
        string? latestMessage)
    {
        var snapshot = updates.ToArray();
        if (snapshot.Length == 0)
            return (nameof(IngestFilePhase.Queued), 0, 0, 0, latestMessage);

        var phase = snapshot.Any(update => update.Phase == IngestFilePhase.Failed)
            ? IngestFilePhase.Failed
            : snapshot.Any(update => update.Phase == IngestFilePhase.Ingesting)
                ? IngestFilePhase.Ingesting
                : snapshot.Any(update => update.Phase == IngestFilePhase.Sniffing)
                    ? IngestFilePhase.Sniffing
                    : snapshot.All(update => update.Phase is IngestFilePhase.Completed or IngestFilePhase.Skipped)
                        ? IngestFilePhase.Completed
                        : snapshot[^1].Phase;

        var finishedEntries = snapshot.Count(update => update.Phase is IngestFilePhase.Completed or IngestFilePhase.Skipped);
        var message = snapshot.Length > 1
            ? $"{finishedEntries}/{snapshot.Length} archive entries processed"
            : latestMessage;

        return (
            phase.ToString(),
            snapshot.Sum(update => update.BytesProcessed),
            snapshot.Sum(update => update.BytesTotal),
            snapshot.Sum(update => update.RecordsProcessed),
            message);
    }

    private bool TryFindStagedSource(string sourcePath, out StagedSourceItemViewModel item)
    {
        var canonical = SourcePathHelper.Normalize(sourcePath);
        item = StagedSources.FirstOrDefault(staged =>
            string.Equals(SourcePathHelper.Normalize(staged.SourcePath), canonical, StringComparison.OrdinalIgnoreCase))!;
        return item is not null;
    }

    private static async Task PersistFeedbackRuleAsync(GlobalStore globalStore, StagedSourceItemViewModel item)
    {
        if (!item.ShouldPersistFeedbackRule || !item.TryBuildSelectedOverride(out var formatOverride))
            return;

        var candidate = BuildFeedbackRuleCandidate(item, formatOverride);
        await globalStore.AddOrUpdateFeedbackRuleAsync(candidate);
    }

    private static FeedbackRuleCandidate BuildFeedbackRuleCandidate(StagedSourceItemViewModel item, FileFormatOverride formatOverride)
    {
        var templateKey = FeedbackTemplateKeyBuilder.BuildKey(item.SourcePath);
        var sourceName = FeedbackTemplateKeyBuilder.GetSourceName(item.SourcePath);
        var timeBasis = formatOverride.TimeBasisOverride;
        var csvBoundary = formatOverride.Detection.Boundary as CsvBoundary;

        return new FeedbackRuleCandidate(
            RuleType: "detection",
            TemplateKey: templateKey,
            Config: new FeedbackRuleConfig(
                SourceName: sourceName,
                FormatKind: FormatBoundaryName(formatOverride.Detection.Boundary),
                Summary: item.DetectionSummary,
                EncodingCodePage: formatOverride.EncodingOverride?.CodePage,
                TimeBasis: timeBasis?.Basis.ToString(),
                OffsetMinutes: timeBasis?.OffsetMinutes,
                TimeZoneId: timeBasis?.TimeZoneId,
                TimestampExpression: formatOverride.Detection.Extractor.Description,
                CsvDelimiter: csvBoundary?.Delimiter.ToString(),
                CsvHasHeader: csvBoundary?.HasHeader),
            Confidence: 1.0,
            Source: "user");
    }

    private static FeedbackSuggestionViewModel BuildFeedbackSuggestion(FeedbackRuleEntry rule)
    {
        var details = new List<string>
        {
            $"template {rule.TemplateKey}"
        };

        if (!string.IsNullOrWhiteSpace(rule.Config.TimestampExpression))
            details.Add(rule.Config.TimestampExpression!);

        if (!string.IsNullOrWhiteSpace(rule.Config.TimeBasis))
            details.Add(rule.Config.TimeBasis!);

        if (rule.Config.OffsetMinutes.HasValue)
            details.Add($"UTC{FormatOffset(rule.Config.OffsetMinutes.Value)}");
        else if (!string.IsNullOrWhiteSpace(rule.Config.TimeZoneId))
            details.Add(rule.Config.TimeZoneId!);

        if (!string.IsNullOrWhiteSpace(rule.Config.CsvDelimiter))
            details.Add($"delimiter {rule.Config.CsvDelimiter}");

        return new FeedbackSuggestionViewModel(
            Summary: rule.Config.Summary,
            Detail: string.Join(" · ", details),
            Confidence: rule.Confidence,
            UseCount: rule.UseCount);
    }

    private string BuildSessionStatus(string detail) =>
        $"{detail} | {_sessionFolder}";

    private void DismissRecoveryBanner()
    {
        _recoveryBannerDismissed = true;
        CrashRecovery = new CrashRecoveryViewModel();
        StatusText = BuildSessionStatus("Recovery dismissed.");
    }

    private void ConfirmDetectionChoice(StagedSourceItemViewModel? item)
    {
        if (item is null)
            return;

        item.ConfirmDetectionSelection();
        RaiseStagingStateProperties();
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024d:N1} KB";
        if (bytes < 1024L * 1024 * 1024) return $"{bytes / (1024d * 1024):N1} MB";
        return $"{bytes / (1024d * 1024 * 1024):N1} GB";
    }

    private sealed record SampleTimestampInfo(
        string ParsedText,
        string? AlternateText,
        bool HasExplicitOffset,
        bool UsedTwoDigitYear);
}
