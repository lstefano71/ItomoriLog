using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Ingest.Extractors;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;

using ReactiveUI;

using System.Collections.ObjectModel;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace ItomoriLog.UI.ViewModels;

public sealed record DetectionDetailViewModel(string Label, string Value);

public sealed record DetectionChoiceViewModel(
    string FormatName,
    string ShortLabel,
    string Summary,
    double Confidence,
    DetectionResult Detection,
    IReadOnlyList<DetectionDetailViewModel> Details,
    string? Notes,
    int EncodingCodePage,
    string EncodingDisplay,
    IReadOnlyList<string> SampleLines)
{
    public string DisplayName =>
        string.IsNullOrWhiteSpace(ShortLabel)
            ? $"{FormatName} ({Confidence:P0})"
            : $"{FormatName} · {ShortLabel} ({Confidence:P0})";

    public string ConfidenceDisplay => $"{Confidence:P0}";
}

public sealed record EncodingChoiceViewModel(string Label, int CodePage)
{
    public Encoding ResolveEncoding()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        return Encoding.GetEncoding(CodePage);
    }
}

public sealed record DelimiterChoiceViewModel(string Label, char Value);

public sealed record FeedbackSuggestionViewModel(
    string Summary,
    string Detail,
    double Confidence,
    int UseCount)
{
    public string ConfidenceDisplay => $"{Confidence:P0}";

    public string UseCountDisplay =>
        UseCount == 1 ? "used once" : $"used {UseCount} times";
}

public partial class StagedSourceItemViewModel : ReactiveObject
{
    private static readonly TimeSpan MinStableRateWindow = TimeSpan.FromSeconds(1);
    private static readonly Regex FixedOffsetRegex = FixedOffsetRegexImpl();

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
    private bool _wasDetectionLowConfidence;
    private bool _isSniffing;
    private bool _updatingSelectionInternally;
    private bool _isResumePending;
    private DetectionChoiceViewModel? _selectedDetectionChoice;
    private EncodingChoiceViewModel? _selectedEncodingChoice;
    private DelimiterChoiceViewModel? _selectedDelimiterChoice;
    private bool _csvHasHeader;
    private string _filenameTemplateKey = string.Empty;
    private string _timestampOverrideText = string.Empty;
    private string _timezoneOverrideText = string.Empty;

    public StagedSourceItemViewModel(string sourcePath, bool isDirectory)
    {
        _sourcePath = sourcePath;
        _isDirectory = isDirectory;
        DetectionChoices = [];
        FeedbackSuggestions = [];
        EncodingChoices =
        [
            new EncodingChoiceViewModel("UTF-8", 65001),
            new EncodingChoiceViewModel("UTF-16 LE", 1200),
            new EncodingChoiceViewModel("UTF-16 BE", 1201),
            new EncodingChoiceViewModel("Windows-1252", 1252)
        ];
        DelimiterChoices =
        [
            new DelimiterChoiceViewModel("Comma (,)", ','),
            new DelimiterChoiceViewModel("Semicolon (;)", ';'),
            new DelimiterChoiceViewModel("Tab", '\t'),
            new DelimiterChoiceViewModel("Pipe (|)", '|')
        ];

        if (isDirectory) {
            _detectionStatus = "Folder staged";
            _isDetectionUserConfirmed = true;
        }
    }

    public string SourcePath {
        get => _sourcePath;
        set {
            this.RaiseAndSetIfChanged(ref _sourcePath, value);
            RaiseComputedStateProperties();
        }
    }

    [GeneratedRegex(@"^(?:UTC)?(?<sign>[+-])(?<hours>\d{1,2}):(?<minutes>\d{2})$", RegexOptions.IgnoreCase)]
    private static partial Regex FixedOffsetRegexImpl();

    public bool IsDirectory {
        get => _isDirectory;
        set {
            this.RaiseAndSetIfChanged(ref _isDirectory, value);
            RaiseComputedStateProperties();
        }
    }

    public string SourceName =>
        Path.GetFileName(SourcePath.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)) is { Length: > 0 } name
            ? name
            : SourcePath;

    public string Phase {
        get => _phase;
        set {
            this.RaiseAndSetIfChanged(ref _phase, value);
            RaiseComputedStateProperties();
        }
    }

    public string? Message {
        get => _message;
        set {
            this.RaiseAndSetIfChanged(ref _message, value);
            RaiseComputedStateProperties();
        }
    }

    public long BytesProcessed {
        get => _bytesProcessed;
        set => this.RaiseAndSetIfChanged(ref _bytesProcessed, value);
    }

    public long BytesTotal {
        get => _bytesTotal;
        set => this.RaiseAndSetIfChanged(ref _bytesTotal, value);
    }

    public long RecordsProcessed {
        get => _recordsProcessed;
        set {
            this.RaiseAndSetIfChanged(ref _recordsProcessed, value);
            RaiseComputedStateProperties();
        }
    }

    public double RecordsPerSecond {
        get => _recordsPerSecond;
        set {
            this.RaiseAndSetIfChanged(ref _recordsPerSecond, value);
            RaiseComputedStateProperties();
        }
    }

    public double? EtaSeconds {
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

    public bool IsSniffing {
        get => _isSniffing;
        set {
            this.RaiseAndSetIfChanged(ref _isSniffing, value);
            RaiseComputedStateProperties();
            RaiseOverrideProperties();
        }
    }

    public ObservableCollection<DetectionChoiceViewModel> DetectionChoices { get; }

    public ObservableCollection<EncodingChoiceViewModel> EncodingChoices { get; }

    public ObservableCollection<DelimiterChoiceViewModel> DelimiterChoices { get; }

    public ObservableCollection<FeedbackSuggestionViewModel> FeedbackSuggestions { get; }

    public DetectionChoiceViewModel? SelectedDetectionChoice {
        get => _selectedDetectionChoice;
        set {
            var changed = !EqualityComparer<DetectionChoiceViewModel?>.Default.Equals(_selectedDetectionChoice, value);
            this.RaiseAndSetIfChanged(ref _selectedDetectionChoice, value);
            if (!changed)
                return;

            if (!_updatingSelectionInternally) {
                _isResumePending = false;
                _isDetectionUserConfirmed = value is not null;
                _requiresDetectionReview = false;
            }

            ResetOverrideEditorFromSelection();
            this.RaisePropertyChanged(nameof(HasDetectionChoice));
            this.RaisePropertyChanged(nameof(RequiresDetectionReview));
            this.RaisePropertyChanged(nameof(HasDetectionDetails));
            this.RaisePropertyChanged(nameof(DetectionDetails));
            this.RaisePropertyChanged(nameof(DetectionNotes));
            this.RaisePropertyChanged(nameof(HasDetectionNotes));
            RaiseComputedStateProperties();
        }
    }

    public EncodingChoiceViewModel? SelectedEncodingChoice {
        get => _selectedEncodingChoice;
        set {
            this.RaiseAndSetIfChanged(ref _selectedEncodingChoice, value);
            RaiseOverrideProperties();
            RaiseComputedStateProperties();
        }
    }

    public DelimiterChoiceViewModel? SelectedDelimiterChoice {
        get => _selectedDelimiterChoice;
        set {
            this.RaiseAndSetIfChanged(ref _selectedDelimiterChoice, value);
            RaiseOverrideProperties();
            RaiseComputedStateProperties();
        }
    }

    public bool CsvHasHeader {
        get => _csvHasHeader;
        set {
            this.RaiseAndSetIfChanged(ref _csvHasHeader, value);
            RaiseOverrideProperties();
            RaiseComputedStateProperties();
        }
    }

    public string TimestampOverrideText {
        get => _timestampOverrideText;
        set {
            this.RaiseAndSetIfChanged(ref _timestampOverrideText, value);
            RaiseOverrideProperties();
            RaiseComputedStateProperties();
        }
    }

    public string TimezoneOverrideText {
        get => _timezoneOverrideText;
        set {
            this.RaiseAndSetIfChanged(ref _timezoneOverrideText, value);
            RaiseOverrideProperties();
            RaiseComputedStateProperties();
        }
    }

    public bool HasDetectionChoice => SelectedDetectionChoice is not null;

    public string DetectionStatus {
        get => _detectionStatus;
        set {
            this.RaiseAndSetIfChanged(ref _detectionStatus, value);
            RaiseComputedStateProperties();
        }
    }

    public bool RequiresDetectionReview {
        get => _requiresDetectionReview;
        private set {
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

    public string FilenameTemplateKey {
        get => _filenameTemplateKey;
        private set {
            this.RaiseAndSetIfChanged(ref _filenameTemplateKey, value);
            RaiseComputedStateProperties();
        }
    }

    public bool HasFilenameTemplateKey => !string.IsNullOrWhiteSpace(FilenameTemplateKey);

    public bool HasFeedbackSuggestions => FeedbackSuggestions.Count > 0;

    public string FeedbackSuggestionSummary =>
        FeedbackSuggestions.Count == 0
            ? string.Empty
            : $"{FeedbackSuggestions.Count} learned suggestion{(FeedbackSuggestions.Count == 1 ? string.Empty : "s")}";

    public bool HasEditableOverrideRecipe =>
        SelectedDetectionChoice is not null
        && !IsDirectory
        && !_isResumePending;

    public bool HasEditableTimestampOverride =>
        SelectedDetectionChoice?.Detection.Boundary is CsvBoundary or JsonNdBoundary;

    public bool HasEditableCsvStructure =>
        SelectedDetectionChoice?.Detection.Boundary is CsvBoundary;

    public string TimestampOverrideLabel =>
        SelectedDetectionChoice?.Detection.Boundary switch {
            CsvBoundary => "Timestamp field(s)",
            JsonNdBoundary => "Timestamp field",
            _ => "Timestamp"
        };

    public string TimestampOverrideWatermark =>
        SelectedDetectionChoice?.Detection.Boundary switch {
            CsvBoundary => "Column0 + Column1 or Timestamp",
            JsonNdBoundary => "timestamp or nested.path",
            _ => string.Empty
        };

    public string TimestampOverrideHint =>
        SelectedDetectionChoice?.Detection.Boundary switch {
            CsvBoundary => "Use one field or multiple fields joined with '+' or ','.",
            JsonNdBoundary => "Enter the JSON field path to use as the timestamp source.",
            _ => string.Empty
        };

    public string CsvColumnPreview =>
        SelectedDetectionChoice is { } choice && choice.Detection.Boundary is CsvBoundary
            ? string.Join(", ", GetEffectiveCsvColumnNames(choice))
            : string.Empty;

    public string CsvStructureHint =>
        "Changing delimiter or header regenerates the CSV column list from the current sample.";

    public string EncodingOverrideHint => "Override file encoding independently from the detected timestamp/profile.";

    public string TimezoneOverrideHint => "Blank = session default. Also accepts UTC, Local, Europe/Rome, or UTC+02:00.";

    public string OverrideValidationMessage =>
        SelectedDetectionChoice is null || !HasEditableOverrideRecipe
            ? string.Empty
            : ValidateOverrideRecipe(SelectedDetectionChoice) ?? string.Empty;

    public bool HasOverrideValidationError => !string.IsNullOrWhiteSpace(OverrideValidationMessage);

    public string DetectionSummary =>
        SelectedDetectionChoice is { } choice
            ? BuildEffectiveDetectionSummary(choice)
            : (IsSniffing
                ? "Sniffing timestamp, timezone, encoding, and structure..."
                : DetectionStatus);

    public string ConfidenceDisplay {
        get {
            if (IsDirectory)
                return "N/A";
            if (IsSniffing)
                return "...";
            if (SelectedDetectionChoice is not null)
                return SelectedDetectionChoice.ConfidenceDisplay;
            return "-";
        }
    }

    public string DetectionBadge {
        get {
            if (IsDirectory)
                return "Folder";
            if (IsSniffing)
                return "Sniff";
            if (DetectionStatus.StartsWith("Archive", StringComparison.OrdinalIgnoreCase)
                || DetectionStatus.StartsWith("ZIP", StringComparison.OrdinalIgnoreCase))
                return "Archive";
            if (_isResumePending)
                return "Resume";
            if (HasOverrideValidationError)
                return "Fix";
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

    public string QueueSummary {
        get {
            var parts = new List<string>();
            if (_isResumePending)
                parts.Add("Ready to resume interrupted ingest");

            if (!string.IsNullOrWhiteSpace(DetectionSummary))
                parts.Add(DetectionSummary);

            if (HasFeedbackSuggestions)
                parts.Add(FeedbackSuggestionSummary);

            if (Phase is not "Queued")
                parts.Add(Phase);

            if (RecordsProcessed > 0)
                parts.Add($"{RecordsProcessed:N0} rows");

            return string.Join(" · ", parts.Where(part => !string.IsNullOrWhiteSpace(part)));
        }
    }

    public bool CanReingest => !IsDirectory && Phase is "Completed" or "Skipped" or "Failed";

    public bool ShouldPersistFeedbackRule =>
        SelectedDetectionChoice is not null
        && (HasUserEditedRecipe || HasAlternateDetectionSelection || _wasDetectionLowConfidence);

    public bool TryBuildSelectedOverride(out FileFormatOverride formatOverride)
    {
        formatOverride = null!;

        if (SelectedDetectionChoice is null || (_requiresDetectionReview && !_isDetectionUserConfirmed))
            return false;

        if (HasOverrideValidationError)
            return false;

        var detection = BuildDetectionOverride(SelectedDetectionChoice);
        if (!TryParseTimeBasisOverride(TimezoneOverrideText, out var timeBasisOverride))
            return false;

        formatOverride = new FileFormatOverride(
            SourcePath,
            detection,
            SelectedEncodingChoice?.ResolveEncoding(),
            timeBasisOverride);
        return true;
    }

    public bool TryGetSelectedDetection(out DetectionResult detection)
    {
        if (TryBuildSelectedOverride(out var formatOverride)) {
            detection = formatOverride.Detection;
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
        ClearFeedbackSuggestions();
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
        _wasDetectionLowConfidence = false;
        _isResumePending = true;
        IsSniffing = false;
        ResetOverrideEditorFromSelection();
        ClearFeedbackSuggestions();
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
        _wasDetectionLowConfidence = needsReview;
        _isDetectionUserConfirmed = !needsReview && SelectedDetectionChoice is not null;
        IsSniffing = false;
        ResetOverrideEditorFromSelection();

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
        _wasDetectionLowConfidence = false;
        _isResumePending = false;
        IsSniffing = false;
        ResetOverrideEditorFromSelection();
        ClearFeedbackSuggestions();
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
        if (SelectedDetectionChoice is null || HasOverrideValidationError)
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

        if (isReset) {
            _progressStartedUtc = null;
            RecordsPerSecond = 0;
            EtaSeconds = null;
        } else {
            if (!_progressStartedUtc.HasValue && (bytesProcessed > 0 || recordsProcessed > 0))
                _progressStartedUtc = now;

            if (_progressStartedUtc.HasValue) {
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
            } else {
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

    private void ResetOverrideEditorFromSelection()
    {
        if (SelectedDetectionChoice is null) {
            _selectedEncodingChoice = null;
            _selectedDelimiterChoice = null;
            _csvHasHeader = false;
            _timestampOverrideText = string.Empty;
            _timezoneOverrideText = string.Empty;
            RaiseOverrideProperties();
            return;
        }

        _selectedEncodingChoice = EncodingChoices.FirstOrDefault(choice => choice.CodePage == SelectedDetectionChoice.EncodingCodePage)
            ?? EncodingChoices.FirstOrDefault();
        if (SelectedDetectionChoice.Detection.Boundary is CsvBoundary csv) {
            _selectedDelimiterChoice = DelimiterChoices.FirstOrDefault(choice => choice.Value == csv.Delimiter)
                ?? DelimiterChoices.FirstOrDefault();
            _csvHasHeader = csv.HasHeader;
        } else {
            _selectedDelimiterChoice = null;
            _csvHasHeader = false;
        }
        _timestampOverrideText = ExtractTimestampOverrideValue(SelectedDetectionChoice.Detection);
        _timezoneOverrideText = string.Empty;
        RaiseOverrideProperties();
    }

    private DetectionResult BuildDetectionOverride(DetectionChoiceViewModel choice)
    {
        var detection = choice.Detection;
        var notes = AppendOverrideNotes(detection.Notes);

        return detection.Boundary switch {
            CsvBoundary csv => new DetectionResult(
                detection.Confidence,
                BuildCsvBoundaryOverride(choice, csv),
                new CompositeCsvTsExtractor(ParseTimestampOverrideFields(TimestampOverrideText)),
                notes),

            JsonNdBoundary => new DetectionResult(
                detection.Confidence,
                new JsonNdBoundary(TimestampOverrideText.Trim()),
                new JsonTimestampExtractor(TimestampOverrideText.Trim()),
                notes),

            _ => new DetectionResult(
                detection.Confidence,
                detection.Boundary,
                detection.Extractor,
                notes)
        };
    }

    private string? ValidateOverrideRecipe(DetectionChoiceViewModel choice)
    {
        if (SelectedEncodingChoice is null)
            return "Select an encoding.";

        if (!TryParseTimeBasisOverride(TimezoneOverrideText, out _))
            return "Timezone override must be blank, UTC, Local, UTC+/-HH:MM, or a valid timezone ID.";

        return choice.Detection.Boundary switch {
            CsvBoundary when SelectedDelimiterChoice is null =>
                "Select a CSV delimiter.",
            CsvBoundary when ParseTimestampOverrideFields(TimestampOverrideText).Length == 0 =>
                "Enter one or more CSV timestamp fields.",
            CsvBoundary when !TimestampFieldsExistInCsvColumns(choice) =>
                $"Timestamp fields must match the current CSV columns: {string.Join(", ", GetEffectiveCsvColumnNames(choice))}.",
            JsonNdBoundary when string.IsNullOrWhiteSpace(TimestampOverrideText) =>
                "Enter the JSON timestamp field path.",
            _ => null
        };
    }

    private string BuildEffectiveDetectionSummary(DetectionChoiceViewModel choice)
    {
        var parts = new List<string> { choice.FormatName };

        var timestampPart = HasEditableTimestampOverride
            ? TimestampOverrideText.Trim()
            : choice.ShortLabel;
        if (!string.IsNullOrWhiteSpace(timestampPart))
            parts.Add(timestampPart);

        if (SelectedEncodingChoice is not null)
            parts.Add(SelectedEncodingChoice.Label);

        if (choice.Detection.Boundary is CsvBoundary) {
            if (SelectedDelimiterChoice is not null)
                parts.Add(SelectedDelimiterChoice.Label);

            parts.Add(CsvHasHeader ? "header row" : "no header");
        }

        parts.Add(DescribeTimezoneSelection(TimezoneOverrideText));
        return string.Join(" · ", parts.Where(part => !string.IsNullOrWhiteSpace(part)));
    }

    private void RaiseComputedStateProperties()
    {
        this.RaisePropertyChanged(nameof(SourceName));
        this.RaisePropertyChanged(nameof(DetectionBadge));
        this.RaisePropertyChanged(nameof(DetectionSummary));
        this.RaisePropertyChanged(nameof(ConfidenceDisplay));
        this.RaisePropertyChanged(nameof(QueueSummary));
        this.RaisePropertyChanged(nameof(FilenameTemplateKey));
        this.RaisePropertyChanged(nameof(HasFilenameTemplateKey));
        this.RaisePropertyChanged(nameof(HasFeedbackSuggestions));
        this.RaisePropertyChanged(nameof(FeedbackSuggestionSummary));
    }

    private void RaiseOverrideProperties()
    {
        this.RaisePropertyChanged(nameof(SelectedEncodingChoice));
        this.RaisePropertyChanged(nameof(SelectedDelimiterChoice));
        this.RaisePropertyChanged(nameof(CsvHasHeader));
        this.RaisePropertyChanged(nameof(TimestampOverrideText));
        this.RaisePropertyChanged(nameof(TimezoneOverrideText));
        this.RaisePropertyChanged(nameof(HasEditableOverrideRecipe));
        this.RaisePropertyChanged(nameof(HasEditableTimestampOverride));
        this.RaisePropertyChanged(nameof(HasEditableCsvStructure));
        this.RaisePropertyChanged(nameof(TimestampOverrideLabel));
        this.RaisePropertyChanged(nameof(TimestampOverrideWatermark));
        this.RaisePropertyChanged(nameof(TimestampOverrideHint));
        this.RaisePropertyChanged(nameof(CsvColumnPreview));
        this.RaisePropertyChanged(nameof(CsvStructureHint));
        this.RaisePropertyChanged(nameof(EncodingOverrideHint));
        this.RaisePropertyChanged(nameof(TimezoneOverrideHint));
        this.RaisePropertyChanged(nameof(OverrideValidationMessage));
        this.RaisePropertyChanged(nameof(HasOverrideValidationError));
        RaiseComputedStateProperties();
    }

    private static string ExtractTimestampOverrideValue(DetectionResult detection)
    {
        if (detection.Boundary is JsonNdBoundary json && !string.IsNullOrWhiteSpace(json.TimestampFieldPath))
            return json.TimestampFieldPath;

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

    private static string[] ParseTimestampOverrideFields(string value) =>
        value.Split(['+', ','], StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

    private bool HasUserEditedRecipe {
        get {
            if (SelectedDetectionChoice is not { } choice)
                return false;

            if (SelectedEncodingChoice is not null && SelectedEncodingChoice.CodePage != choice.EncodingCodePage)
                return true;

            if (!string.IsNullOrWhiteSpace(TimezoneOverrideText))
                return true;

            if (HasEditableTimestampOverride
                && !string.Equals(
                    NormalizeTimestampOverrideText(TimestampOverrideText),
                    NormalizeTimestampOverrideText(ExtractTimestampOverrideValue(choice.Detection)),
                    StringComparison.OrdinalIgnoreCase)) {
                return true;
            }

            if (choice.Detection.Boundary is CsvBoundary csv) {
                if ((SelectedDelimiterChoice?.Value ?? csv.Delimiter) != csv.Delimiter)
                    return true;

                if (CsvHasHeader != csv.HasHeader)
                    return true;
            }

            return false;
        }
    }

    private bool HasAlternateDetectionSelection =>
        SelectedDetectionChoice is not null
        && DetectionChoices.Count > 0
        && !ReferenceEquals(SelectedDetectionChoice, DetectionChoices[0]);

    private static string NormalizeTimestampOverrideText(string value) =>
        string.Join("+", ParseTimestampOverrideFields(value));

    public void ApplyFeedbackSuggestions(string templateKey, IReadOnlyList<FeedbackSuggestionViewModel> suggestions)
    {
        FilenameTemplateKey = templateKey;
        FeedbackSuggestions.Clear();
        foreach (var suggestion in suggestions)
            FeedbackSuggestions.Add(suggestion);

        RaiseComputedStateProperties();
    }

    private void ClearFeedbackSuggestions()
    {
        FilenameTemplateKey = string.Empty;
        FeedbackSuggestions.Clear();
        RaiseComputedStateProperties();
    }

    private bool TimestampFieldsExistInCsvColumns(DetectionChoiceViewModel choice)
    {
        var fields = ParseTimestampOverrideFields(TimestampOverrideText);
        if (fields.Length == 0)
            return false;

        var available = new HashSet<string>(GetEffectiveCsvColumnNames(choice), StringComparer.OrdinalIgnoreCase);
        return fields.All(available.Contains);
    }

    private string[] GetEffectiveCsvColumnNames(DetectionChoiceViewModel choice)
    {
        if (choice.Detection.Boundary is not CsvBoundary csv)
            return [];

        var delimiter = SelectedDelimiterChoice?.Value ?? csv.Delimiter;
        var hasHeader = CsvHasHeader;
        return CsvPreviewHelper.BuildColumnNames(choice.SampleLines, delimiter, csv.Quote, hasHeader, csv.ColumnNames);
    }

    private CsvBoundary BuildCsvBoundaryOverride(DetectionChoiceViewModel choice, CsvBoundary csv)
    {
        var delimiter = SelectedDelimiterChoice?.Value ?? csv.Delimiter;
        var columnNames = GetEffectiveCsvColumnNames(choice);
        return new CsvBoundary(delimiter, CsvHasHeader, columnNames, csv.Quote);
    }

    private static string DescribeTimezoneSelection(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return "session timezone";

        return TryParseTimeBasisOverride(value, out var timeBasisOverride)
            ? DescribeTimeBasisOverride(timeBasisOverride)
            : value.Trim();
    }

    private static string DescribeTimeBasisOverride(TimeBasisConfig? timeBasisOverride)
    {
        if (timeBasisOverride is null)
            return "session timezone";

        return timeBasisOverride.Basis switch {
            TimeBasis.Utc => "UTC",
            TimeBasis.Local => "Local",
            TimeBasis.Zone when !string.IsNullOrWhiteSpace(timeBasisOverride.TimeZoneId) => timeBasisOverride.TimeZoneId!,
            TimeBasis.FixedOffset when timeBasisOverride.OffsetMinutes.HasValue => $"UTC{FormatOffset(timeBasisOverride.OffsetMinutes.Value)}",
            _ => "session timezone"
        };
    }

    private static bool TryParseTimeBasisOverride(string? value, out TimeBasisConfig? overrideConfig)
    {
        overrideConfig = null;
        if (string.IsNullOrWhiteSpace(value))
            return true;

        var trimmed = value.Trim();
        if (trimmed.Equals("session", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("session default", StringComparison.OrdinalIgnoreCase)) {
            return true;
        }

        if (trimmed.Equals("local", StringComparison.OrdinalIgnoreCase)) {
            overrideConfig = new TimeBasisConfig(TimeBasis.Local);
            return true;
        }

        if (trimmed.Equals("utc", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("etc/utc", StringComparison.OrdinalIgnoreCase)) {
            overrideConfig = new TimeBasisConfig(TimeBasis.Utc);
            return true;
        }

        if (TryParseFixedOffset(trimmed, out var offsetMinutes)) {
            overrideConfig = new TimeBasisConfig(TimeBasis.FixedOffset, offsetMinutes);
            return true;
        }

        if (SessionDefaults.IsValidTimezoneId(trimmed)) {
            overrideConfig = new TimeBasisConfig(TimeBasis.Zone, TimeZoneId: trimmed);
            return true;
        }

        return false;
    }

    private static bool TryParseFixedOffset(string value, out int offsetMinutes)
    {
        offsetMinutes = 0;
        var match = FixedOffsetRegex.Match(value);
        if (!match.Success)
            return false;

        if (!int.TryParse(match.Groups["hours"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var hours)
            || !int.TryParse(match.Groups["minutes"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var minutes)) {
            return false;
        }

        if (hours > 23 || minutes > 59)
            return false;

        offsetMinutes = hours * 60 + minutes;
        if (match.Groups["sign"].Value == "-")
            offsetMinutes = -offsetMinutes;

        return true;
    }

    private static string FormatOffset(int offsetMinutes)
    {
        var offset = TimeSpan.FromMinutes(offsetMinutes);
        return $"{(offset >= TimeSpan.Zero ? "+" : "-")}{Math.Abs(offset.Hours):00}:{Math.Abs(offset.Minutes):00}";
    }

    private string AppendOverrideNotes(string? existingNotes)
    {
        var edits = new List<string>();

        if (SelectedEncodingChoice is not null
            && SelectedDetectionChoice is not null
            && SelectedEncodingChoice.CodePage != SelectedDetectionChoice.EncodingCodePage) {
            edits.Add($"encoding={SelectedEncodingChoice.Label}");
        }

        if (SelectedDetectionChoice?.Detection.Boundary is CsvBoundary csv) {
            if (SelectedDelimiterChoice is not null && SelectedDelimiterChoice.Value != csv.Delimiter)
                edits.Add($"delimiter={SelectedDelimiterChoice.Label}");

            if (CsvHasHeader != csv.HasHeader)
                edits.Add(CsvHasHeader ? "header=present" : "header=absent");
        }

        if (!string.IsNullOrWhiteSpace(TimezoneOverrideText))
            edits.Add($"timezone={DescribeTimezoneSelection(TimezoneOverrideText)}");

        if (HasEditableTimestampOverride && !string.IsNullOrWhiteSpace(TimestampOverrideText))
            edits.Add($"timestamp={TimestampOverrideText.Trim()}");

        if (edits.Count == 0)
            return existingNotes ?? string.Empty;

        var overrideText = $"User override: {string.Join(", ", edits)}";
        return string.IsNullOrWhiteSpace(existingNotes)
            ? overrideText
            : $"{existingNotes} | {overrideText}";
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024d:N1} KB";
        if (bytes < 1024L * 1024 * 1024) return $"{bytes / (1024d * 1024):N1} MB";
        return $"{bytes / (1024d * 1024 * 1024):N1} GB";
    }
}
