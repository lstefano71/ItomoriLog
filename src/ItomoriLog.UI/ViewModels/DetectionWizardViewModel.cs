using System.Collections.ObjectModel;
using System.Reactive;
using System.Reactive.Linq;
using ReactiveUI;
using ItomoriLog.Core.Ingest;

namespace ItomoriLog.UI.ViewModels;

/// <summary>
/// Shown as an overlay when auto-detection confidence is below 95% or disambiguation is needed.
/// Displays format candidates, lets the user pick one, previews records, then confirms.
/// </summary>
public class DetectionWizardViewModel : ViewModelBase
{
    private DetectionCandidate? _selectedCandidate;
    private string _previewText = "";
    private bool _isLoadingPreview;
    private string _statusMessage = "Select a format to preview records";

    public DetectionWizardViewModel(
        IReadOnlyList<DetectionCandidate> candidates,
        Func<DetectionCandidate, IReadOnlyList<string>> previewProvider)
    {
        Candidates = new ObservableCollection<DetectionCandidate>(candidates);
        PreviewProvider = previewProvider;

        ConfirmCommand = ReactiveCommand.Create(
            () => SelectedCandidate,
            this.WhenAnyValue(x => x.SelectedCandidate).Select(c => c is not null));

        CancelCommand = ReactiveCommand.Create(() => (DetectionCandidate?)null);

        this.WhenAnyValue(x => x.SelectedCandidate)
            .Where(c => c is not null)
            .Subscribe(c => LoadPreview(c!));
    }

    public ObservableCollection<DetectionCandidate> Candidates { get; }

    public Func<DetectionCandidate, IReadOnlyList<string>> PreviewProvider { get; }

    public DetectionCandidate? SelectedCandidate
    {
        get => _selectedCandidate;
        set => this.RaiseAndSetIfChanged(ref _selectedCandidate, value);
    }

    public string PreviewText
    {
        get => _previewText;
        set => this.RaiseAndSetIfChanged(ref _previewText, value);
    }

    public bool IsLoadingPreview
    {
        get => _isLoadingPreview;
        set => this.RaiseAndSetIfChanged(ref _isLoadingPreview, value);
    }

    public string StatusMessage
    {
        get => _statusMessage;
        set => this.RaiseAndSetIfChanged(ref _statusMessage, value);
    }

    /// <summary>Returns the selected candidate on confirm, or null on cancel.</summary>
    public ReactiveCommand<Unit, DetectionCandidate?> ConfirmCommand { get; }

    /// <summary>Returns null to signal cancellation.</summary>
    public ReactiveCommand<Unit, DetectionCandidate?> CancelCommand { get; }

    private void LoadPreview(DetectionCandidate candidate)
    {
        IsLoadingPreview = true;
        try
        {
            var lines = PreviewProvider(candidate);
            PreviewText = lines.Count > 0
                ? string.Join(Environment.NewLine, lines)
                : "(no records parsed)";
            StatusMessage = $"Showing {lines.Count} preview record(s) for {candidate.FormatName}";
        }
        catch (Exception ex)
        {
            PreviewText = $"Error loading preview: {ex.Message}";
            StatusMessage = "Preview failed";
        }
        finally
        {
            IsLoadingPreview = false;
        }
    }
}

/// <summary>
/// A candidate format produced by the detection engine.
/// </summary>
public sealed record DetectionCandidate(
    string FormatName,
    double Confidence,
    DetectionResult Detection)
{
    public string ConfidenceDisplay => $"{Confidence:P0}";
}
