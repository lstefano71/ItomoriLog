using ReactiveUI;
using System.Collections.ObjectModel;
using ItomoriLog.Core.Ingest;

namespace ItomoriLog.UI.ViewModels;

public sealed record DetectionDetailViewModel(string Label, string Value);

public sealed record DetectionChoiceViewModel(
    string FormatName,
    string ShortLabel,
    string Summary,
    double Confidence,
    DetectionResult Detection,
    IReadOnlyList<DetectionDetailViewModel> Details,
    string? Notes)
{
    public string DisplayName =>
        string.IsNullOrWhiteSpace(ShortLabel)
            ? $"{FormatName} ({Confidence:P0})"
            : $"{FormatName} · {ShortLabel} ({Confidence:P0})";

    public string ConfidenceDisplay => $"{Confidence:P0}";
}

public class StagedSourceItemViewModel : ReactiveObject
{
    private static readonly TimeSpan MinStableRateWindow = TimeSpan.FromSeconds(1);

    private string _sourcePath;
    private bool _isDirectory;
    private string _phase = "Queued";
    private string? _message;
    private long _bytesProcessed;
    private long _bytesTotal;
    private long _recordsProcessed;
    private double _recordsPerSecond;
    private double? _etaSeconds;
    private DateTimeOffset? _progressStartedUtc;
    private string _detectionStatus = "Pending";
    private bool _requiresDetectionReview;
    private bool _isDetectionUserConfirmed;
    private bool _isSniffing;
    private bool _updatingSelectionInternally;
    private bool _isResumePending;
    private DetectionChoiceViewModel? _selectedDetectionChoice;

    public StagedSourceItemViewModel(string sourcePath, bool isDirectory)
    {
        _sourcePath = sourcePath;
        _isDirectory = isDirectory;
        DetectionChoices = [];
        if (isDirectory)
        {
            _detectionStatus = "Folder staged";
            _isDetectionUserConfirmed = true;
        }
    }

    public string SourcePath
    {
        get => _sourcePath;
        set
        {
            this.RaiseAndSetIfChanged(ref _sourcePath, value);
            RaiseComputedStateProperties();
        }
    }

    public bool IsDirectory
    {
        get => _isDirectory;
        set
        {
            this.RaiseAndSetIfChanged(ref _isDirectory, value);
            RaiseComputedStateProperties();
        }
    }

    public string SourceName =>
        Path.GetFileName(SourcePath.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)) is { Length: > 0 } name
            ? name
            : SourcePath;

    public string Phase
    {
        get => _phase;
        set
        {
            this.RaiseAndSetIfChanged(ref _phase, value);
            RaiseComputedStateProperties();
        }
    }

    public string? Message
    {
        get => _message;
        set
        {
            this.RaiseAndSetIfChanged(ref _message, value);
            RaiseComputedStateProperties();
        }
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
        set
        {
            this.RaiseAndSetIfChanged(ref _recordsProcessed, value);
            RaiseComputedStateProperties();
        }
    }

    public double RecordsPerSecond
    {
        get => _recordsPerSecond;
        set
        {
            this.RaiseAndSetIfChanged(ref _recordsPerSecond, value);
            RaiseComputedStateProperties();
        }
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
        set
        {
            this.RaiseAndSetIfChanged(ref _isSniffing, value);
            RaiseComputedStateProperties();
        }
    }

    public ObservableCollection<DetectionChoiceViewModel> DetectionChoices { get; }

    public DetectionChoiceViewModel? SelectedDetectionChoice
    {
        get => _selectedDetectionChoice;
        set
        {
            var changed = !EqualityComparer<DetectionChoiceViewModel?>.Default.Equals(_selectedDetectionChoice, value);
            this.RaiseAndSetIfChanged(ref _selectedDetectionChoice, value);
            if (!changed)
                return;

            if (!_updatingSelectionInternally)
            {
                _isResumePending = false;
                _isDetectionUserConfirmed = value is not null;
                _requiresDetectionReview = false;
            }

            this.RaisePropertyChanged(nameof(HasDetectionChoice));
            this.RaisePropertyChanged(nameof(RequiresDetectionReview));
            this.RaisePropertyChanged(nameof(HasDetectionDetails));
            this.RaisePropertyChanged(nameof(DetectionDetails));
            this.RaisePropertyChanged(nameof(DetectionNotes));
            this.RaisePropertyChanged(nameof(HasDetectionNotes));
            RaiseComputedStateProperties();
        }
    }

    public bool HasDetectionChoice => SelectedDetectionChoice is not null;

    public string DetectionStatus
    {
        get => _detectionStatus;
        set
        {
            this.RaiseAndSetIfChanged(ref _detectionStatus, value);
            RaiseComputedStateProperties();
        }
    }

    public bool RequiresDetectionReview
    {
        get => _requiresDetectionReview;
        private set
        {
            this.RaiseAndSetIfChanged(ref _requiresDetectionReview, value);
            RaiseComputedStateProperties();
        }
    }

    public bool CanSelectDetection => !IsDirectory && DetectionChoices.Count > 0 && !IsSniffing;

    public IReadOnlyList<DetectionDetailViewModel> DetectionDetails =>
        SelectedDetectionChoice?.Details ?? [];

    public bool HasDetectionDetails => DetectionDetails.Count > 0;

    public string DetectionNotes => SelectedDetectionChoice?.Notes ?? string.Empty;

    public bool HasDetectionNotes => !string.IsNullOrWhiteSpace(DetectionNotes);

    public string DetectionSummary =>
        SelectedDetectionChoice?.Summary
        ?? (IsSniffing
            ? "Sniffing timestamp, timezone, encoding, and structure..."
            : DetectionStatus);

    public string ConfidenceDisplay
    {
        get
        {
            if (IsDirectory)
                return "N/A";
            if (IsSniffing)
                return "...";
            if (SelectedDetectionChoice is not null)
                return SelectedDetectionChoice.ConfidenceDisplay;
            return "-";
        }
    }

    public string DetectionBadge
    {
        get
        {
            if (IsDirectory)
                return "Folder";
            if (IsSniffing)
                return "Sniff";
            if (DetectionStatus.StartsWith("Archive", StringComparison.OrdinalIgnoreCase)
                || DetectionStatus.StartsWith("ZIP", StringComparison.OrdinalIgnoreCase))
                return "Archive";
            if (_isResumePending)
                return "Resume";
            if (RequiresDetectionReview && !_isDetectionUserConfirmed)
                return "Review";
            if (Phase == "Ingesting")
                return "Ingest";
            if (Phase == "Completed")
                return "Done";
            if (Phase == "Failed")
                return "Failed";
            if (Phase == "Skipped")
                return "Skipped";
            if (SelectedDetectionChoice is null)
                return "Pending";
            return SelectedDetectionChoice.Confidence >= 0.95 ? "High" : "Ready";
        }
    }

    public string QueueSummary
    {
        get
        {
            var parts = new List<string>();
            if (_isResumePending)
                parts.Add("Ready to resume interrupted ingest");

            if (!string.IsNullOrWhiteSpace(DetectionSummary))
                parts.Add(DetectionSummary);

            if (Phase is not "Queued")
                parts.Add(Phase);

            if (RecordsProcessed > 0)
                parts.Add($"{RecordsProcessed:N0} rows");

            return string.Join(" · ", parts.Where(part => !string.IsNullOrWhiteSpace(part)));
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
        _isResumePending = false;
        IsSniffing = true;
        DetectionStatus = "Sniffing format candidates...";
        this.RaisePropertyChanged(nameof(CanSelectDetection));
    }

    public void MarkPendingResume(string sessionTimezoneDescription)
    {
        DetectionChoices.Clear();
        _updatingSelectionInternally = true;
        SelectedDetectionChoice = null;
        _updatingSelectionInternally = false;
        _requiresDetectionReview = false;
        _isDetectionUserConfirmed = true;
        _isResumePending = true;
        IsSniffing = false;
        DetectionStatus = $"Interrupted ingest detected — resume will use {sessionTimezoneDescription}.";
        this.RaisePropertyChanged(nameof(CanSelectDetection));
        this.RaisePropertyChanged(nameof(HasDetectionDetails));
        this.RaisePropertyChanged(nameof(DetectionDetails));
        this.RaisePropertyChanged(nameof(DetectionNotes));
        this.RaisePropertyChanged(nameof(HasDetectionNotes));
        RaiseComputedStateProperties();
    }

    public void ApplyDetectionChoices(IReadOnlyList<DetectionChoiceViewModel> choices, bool needsReview, string? statusMessage = null)
    {
        DetectionChoices.Clear();
        foreach (var choice in choices)
            DetectionChoices.Add(choice);

        _updatingSelectionInternally = true;
        SelectedDetectionChoice = DetectionChoices.FirstOrDefault();
        _updatingSelectionInternally = false;

        _isResumePending = false;
        _requiresDetectionReview = needsReview;
        _isDetectionUserConfirmed = !needsReview && SelectedDetectionChoice is not null;
        IsSniffing = false;

        DetectionStatus = statusMessage ??
            (SelectedDetectionChoice is null
                ? "No viable format detected"
                : needsReview
                    ? $"Low confidence guess ({SelectedDetectionChoice.ConfidenceDisplay}) — confirm or override before ingest."
                    : $"Detected {SelectedDetectionChoice.FormatName} ({SelectedDetectionChoice.ConfidenceDisplay}).");

        this.RaisePropertyChanged(nameof(HasDetectionChoice));
        this.RaisePropertyChanged(nameof(RequiresDetectionReview));
        this.RaisePropertyChanged(nameof(CanSelectDetection));
        this.RaisePropertyChanged(nameof(HasDetectionDetails));
        this.RaisePropertyChanged(nameof(DetectionDetails));
        this.RaisePropertyChanged(nameof(DetectionNotes));
        this.RaisePropertyChanged(nameof(HasDetectionNotes));
        RaiseComputedStateProperties();
    }

    public void MarkDetectionFailed(string reason)
    {
        DetectionChoices.Clear();
        _updatingSelectionInternally = true;
        SelectedDetectionChoice = null;
        _updatingSelectionInternally = false;
        _requiresDetectionReview = false;
        _isDetectionUserConfirmed = false;
        _isResumePending = false;
        IsSniffing = false;
        DetectionStatus = string.IsNullOrWhiteSpace(reason) ? "Detection failed" : $"Detection failed: {reason}";
        this.RaisePropertyChanged(nameof(HasDetectionChoice));
        this.RaisePropertyChanged(nameof(RequiresDetectionReview));
        this.RaisePropertyChanged(nameof(CanSelectDetection));
        this.RaisePropertyChanged(nameof(HasDetectionDetails));
        this.RaisePropertyChanged(nameof(DetectionDetails));
        this.RaisePropertyChanged(nameof(DetectionNotes));
        this.RaisePropertyChanged(nameof(HasDetectionNotes));
        RaiseComputedStateProperties();
    }

    public void ConfirmDetectionSelection()
    {
        if (SelectedDetectionChoice is null)
            return;

        _isDetectionUserConfirmed = true;
        _requiresDetectionReview = false;
        DetectionStatus = $"Using {SelectedDetectionChoice.FormatName} ({SelectedDetectionChoice.ConfidenceDisplay}).";
        this.RaisePropertyChanged(nameof(RequiresDetectionReview));
        RaiseComputedStateProperties();
    }

    public void ApplyProgress(
        string phase,
        long bytesProcessed,
        long bytesTotal,
        long recordsProcessed,
        string? message)
    {
        var now = DateTimeOffset.UtcNow;
        var isIngesting = string.Equals(phase, nameof(IngestFilePhase.Ingesting), StringComparison.Ordinal);
        var isReset = string.Equals(phase, nameof(IngestFilePhase.Queued), StringComparison.Ordinal)
            && bytesProcessed == 0
            && recordsProcessed == 0;

        Phase = phase;
        Message = message;
        BytesProcessed = bytesProcessed;
        BytesTotal = bytesTotal;
        RecordsProcessed = recordsProcessed;

        if (isReset)
        {
            _progressStartedUtc = null;
            RecordsPerSecond = 0;
            EtaSeconds = null;
        }
        else
        {
            if (!_progressStartedUtc.HasValue && (bytesProcessed > 0 || recordsProcessed > 0))
                _progressStartedUtc = now;

            if (_progressStartedUtc.HasValue)
            {
                var elapsed = (now - _progressStartedUtc.Value).TotalSeconds;
                if (elapsed >= MinStableRateWindow.TotalSeconds && recordsProcessed > 0)
                    RecordsPerSecond = recordsProcessed / elapsed;
                else
                    RecordsPerSecond = 0;

                var bytesPerSecond = elapsed >= MinStableRateWindow.TotalSeconds && bytesProcessed > 0
                    ? bytesProcessed / elapsed
                    : 0;

                if (isIngesting && bytesTotal > 0 && bytesProcessed > 0 && bytesProcessed < bytesTotal && bytesPerSecond > 0)
                    EtaSeconds = Math.Max((bytesTotal - bytesProcessed) / bytesPerSecond, 1);
                else
                    EtaSeconds = null;
            }
            else
            {
                RecordsPerSecond = 0;
                EtaSeconds = null;
            }
        }

        this.RaisePropertyChanged(nameof(ProgressRatio));
        this.RaisePropertyChanged(nameof(ProgressDisplay));
        this.RaisePropertyChanged(nameof(ThroughputDisplay));
        this.RaisePropertyChanged(nameof(EtaDisplay));
        this.RaisePropertyChanged(nameof(CanEdit));
        this.RaisePropertyChanged(nameof(CanReingest));
        RaiseComputedStateProperties();
    }

    private void RaiseComputedStateProperties()
    {
        this.RaisePropertyChanged(nameof(SourceName));
        this.RaisePropertyChanged(nameof(DetectionBadge));
        this.RaisePropertyChanged(nameof(DetectionSummary));
        this.RaisePropertyChanged(nameof(ConfidenceDisplay));
        this.RaisePropertyChanged(nameof(QueueSummary));
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024d:N1} KB";
        if (bytes < 1024L * 1024 * 1024) return $"{bytes / (1024d * 1024):N1} MB";
        return $"{bytes / (1024d * 1024 * 1024):N1} GB";
    }
}
