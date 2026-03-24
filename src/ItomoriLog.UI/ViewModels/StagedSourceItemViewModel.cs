using ReactiveUI;
using System.Collections.ObjectModel;
using ItomoriLog.Core.Ingest;
using System.Linq;

namespace ItomoriLog.UI.ViewModels;

public sealed record DetectionChoiceViewModel(
    string FormatName,
    double Confidence,
    DetectionResult Detection)
{
    public string DisplayName => $"{FormatName} ({Confidence:P0})";
    public string ConfidenceDisplay => $"{Confidence:P0}";
}

public class StagedSourceItemViewModel : ReactiveObject
{
    private string _sourcePath;
    private bool _isDirectory;
    private string _phase = "Queued";
    private string? _message;
    private long _bytesProcessed;
    private long _bytesTotal;
    private long _recordsProcessed;
    private double _recordsPerSecond;
    private double? _etaSeconds;
    private DateTimeOffset? _lastUpdateUtc;
    private long _lastRecordsSnapshot;
    private string _detectionStatus = "Pending";
    private bool _requiresDetectionReview;
    private bool _isDetectionUserConfirmed;
    private bool _isSniffing;
    private bool _updatingSelectionInternally;
    private DetectionChoiceViewModel? _selectedDetectionChoice;

    public StagedSourceItemViewModel(string sourcePath, bool isDirectory)
    {
        _sourcePath = sourcePath;
        _isDirectory = isDirectory;
        DetectionChoices = [];
        if (isDirectory)
        {
            _detectionStatus = "Directory (expanded on ingest)";
            _isDetectionUserConfirmed = true;
        }
    }

    public string SourcePath
    {
        get => _sourcePath;
        set => this.RaiseAndSetIfChanged(ref _sourcePath, value);
    }

    public bool IsDirectory
    {
        get => _isDirectory;
        set => this.RaiseAndSetIfChanged(ref _isDirectory, value);
    }

    public string Phase
    {
        get => _phase;
        set => this.RaiseAndSetIfChanged(ref _phase, value);
    }

    public string? Message
    {
        get => _message;
        set => this.RaiseAndSetIfChanged(ref _message, value);
    }

    public long BytesProcessed
    {
        get => _bytesProcessed;
        set => this.RaiseAndSetIfChanged(ref _bytesProcessed, value);
    }

    public long BytesTotal
    {
        get => _bytesTotal;
        set => this.RaiseAndSetIfChanged(ref _bytesTotal, value);
    }

    public long RecordsProcessed
    {
        get => _recordsProcessed;
        set => this.RaiseAndSetIfChanged(ref _recordsProcessed, value);
    }

    public double RecordsPerSecond
    {
        get => _recordsPerSecond;
        set => this.RaiseAndSetIfChanged(ref _recordsPerSecond, value);
    }

    public double? EtaSeconds
    {
        get => _etaSeconds;
        set => this.RaiseAndSetIfChanged(ref _etaSeconds, value);
    }

    public double ProgressRatio =>
        BytesTotal > 0
            ? Math.Clamp((double)BytesProcessed / BytesTotal, 0.0, 1.0)
            : 0;

    public string ProgressDisplay =>
        BytesTotal > 0
            ? $"{FormatBytes(BytesProcessed)} / {FormatBytes(BytesTotal)}"
            : FormatBytes(BytesProcessed);

    public string ThroughputDisplay =>
        RecordsPerSecond > 0 ? $"{RecordsPerSecond:N1} rec/s" : "-";

    public string EtaDisplay =>
        EtaSeconds.HasValue && EtaSeconds.Value > 0 ? TimeSpan.FromSeconds(EtaSeconds.Value).ToString(@"hh\:mm\:ss") : "-";

    public bool CanEdit => Phase is "Queued" or "Completed" or "Skipped" or "Failed";
    public bool IsSniffing
    {
        get => _isSniffing;
        set => this.RaiseAndSetIfChanged(ref _isSniffing, value);
    }

    public ObservableCollection<DetectionChoiceViewModel> DetectionChoices { get; }

    public DetectionChoiceViewModel? SelectedDetectionChoice
    {
        get => _selectedDetectionChoice;
        set
        {
            var changed = !EqualityComparer<DetectionChoiceViewModel?>.Default.Equals(_selectedDetectionChoice, value);
            this.RaiseAndSetIfChanged(ref _selectedDetectionChoice, value);
            if (changed)
            {
                if (!_updatingSelectionInternally)
                {
                    _isDetectionUserConfirmed = value is not null;
                    _requiresDetectionReview = false;
                }
                this.RaisePropertyChanged(nameof(HasDetectionChoice));
                this.RaisePropertyChanged(nameof(DetectionBadge));
                this.RaisePropertyChanged(nameof(RequiresDetectionReview));
            }
        }
    }

    public bool HasDetectionChoice => SelectedDetectionChoice is not null;

    public string DetectionStatus
    {
        get => _detectionStatus;
        set => this.RaiseAndSetIfChanged(ref _detectionStatus, value);
    }

    public bool RequiresDetectionReview
    {
        get => _requiresDetectionReview;
        private set => this.RaiseAndSetIfChanged(ref _requiresDetectionReview, value);
    }

    public bool CanSelectDetection => !IsDirectory && DetectionChoices.Count > 0 && !IsSniffing;

    public string DetectionBadge
    {
        get
        {
            if (IsDirectory)
                return "Dir";
            if (IsSniffing)
                return "Sniffing";
            if (SelectedDetectionChoice is null)
                return "Unknown";
            if (_requiresDetectionReview && !_isDetectionUserConfirmed)
                return "Review";
            return SelectedDetectionChoice.Confidence >= 0.95 ? "High" : "Selected";
        }
    }

    public bool CanReingest => !IsDirectory && Phase is "Completed" or "Skipped" or "Failed";

    public bool TryGetSelectedDetection(out DetectionResult detection)
    {
        if (SelectedDetectionChoice is not null && (!_requiresDetectionReview || _isDetectionUserConfirmed))
        {
            detection = SelectedDetectionChoice.Detection;
            return true;
        }

        detection = null!;
        return false;
    }

    public void MarkSniffing()
    {
        IsSniffing = true;
        DetectionStatus = "Sniffing format candidates...";
        this.RaisePropertyChanged(nameof(DetectionBadge));
        this.RaisePropertyChanged(nameof(CanSelectDetection));
    }

    public void ApplyDetectionChoices(IReadOnlyList<DetectionChoiceViewModel> choices, bool needsReview, string? statusMessage = null)
    {
        DetectionChoices.Clear();
        foreach (var choice in choices)
            DetectionChoices.Add(choice);

        _updatingSelectionInternally = true;
        SelectedDetectionChoice = DetectionChoices.FirstOrDefault();
        _updatingSelectionInternally = false;

        _requiresDetectionReview = needsReview;
        _isDetectionUserConfirmed = !needsReview && SelectedDetectionChoice is not null;
        IsSniffing = false;

        DetectionStatus = statusMessage ??
            (SelectedDetectionChoice is null
                ? "No viable format detected"
                : needsReview
                    ? "Detection confidence is low — please review."
                    : $"Detected {SelectedDetectionChoice.FormatName} ({SelectedDetectionChoice.ConfidenceDisplay})");

        this.RaisePropertyChanged(nameof(HasDetectionChoice));
        this.RaisePropertyChanged(nameof(RequiresDetectionReview));
        this.RaisePropertyChanged(nameof(CanSelectDetection));
        this.RaisePropertyChanged(nameof(DetectionBadge));
    }

    public void MarkDetectionFailed(string reason)
    {
        DetectionChoices.Clear();
        _updatingSelectionInternally = true;
        SelectedDetectionChoice = null;
        _updatingSelectionInternally = false;
        _requiresDetectionReview = false;
        _isDetectionUserConfirmed = false;
        IsSniffing = false;
        DetectionStatus = string.IsNullOrWhiteSpace(reason) ? "Detection failed" : $"Detection failed: {reason}";
        this.RaisePropertyChanged(nameof(HasDetectionChoice));
        this.RaisePropertyChanged(nameof(RequiresDetectionReview));
        this.RaisePropertyChanged(nameof(CanSelectDetection));
        this.RaisePropertyChanged(nameof(DetectionBadge));
    }

    public void ConfirmDetectionSelection()
    {
        if (SelectedDetectionChoice is null)
            return;

        _isDetectionUserConfirmed = true;
        _requiresDetectionReview = false;
        DetectionStatus = $"Selected {SelectedDetectionChoice.FormatName} ({SelectedDetectionChoice.ConfidenceDisplay}).";
        this.RaisePropertyChanged(nameof(RequiresDetectionReview));
        this.RaisePropertyChanged(nameof(DetectionBadge));
    }

    public void ApplyProgress(
        string phase,
        long bytesProcessed,
        long bytesTotal,
        long recordsProcessed,
        string? message)
    {
        var now = DateTimeOffset.UtcNow;
        if (_lastUpdateUtc.HasValue)
        {
            var elapsed = (now - _lastUpdateUtc.Value).TotalSeconds;
            if (elapsed > 0)
            {
                var deltaRecords = recordsProcessed - _lastRecordsSnapshot;
                if (deltaRecords >= 0)
                    RecordsPerSecond = deltaRecords / elapsed;
            }
        }

        _lastUpdateUtc = now;
        _lastRecordsSnapshot = recordsProcessed;

        Phase = phase;
        Message = message;
        BytesProcessed = bytesProcessed;
        BytesTotal = bytesTotal;
        RecordsProcessed = recordsProcessed;

        if (RecordsPerSecond > 0 && bytesTotal > 0 && bytesProcessed < bytesTotal && recordsProcessed > 0)
        {
            var processedRatio = Math.Clamp((double)bytesProcessed / bytesTotal, 0.0, 1.0);
            var estimatedTotalRecords = recordsProcessed / Math.Max(processedRatio, 0.01);
            var remainingRecords = Math.Max(estimatedTotalRecords - recordsProcessed, 0);
            EtaSeconds = remainingRecords / RecordsPerSecond;
        }
        else
        {
            EtaSeconds = null;
        }

        this.RaisePropertyChanged(nameof(ProgressRatio));
        this.RaisePropertyChanged(nameof(ProgressDisplay));
        this.RaisePropertyChanged(nameof(ThroughputDisplay));
        this.RaisePropertyChanged(nameof(EtaDisplay));
        this.RaisePropertyChanged(nameof(CanEdit));
        this.RaisePropertyChanged(nameof(CanReingest));
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024d:N1} KB";
        if (bytes < 1024L * 1024 * 1024) return $"{bytes / (1024d * 1024):N1} MB";
        return $"{bytes / (1024d * 1024 * 1024):N1} GB";
    }
}
