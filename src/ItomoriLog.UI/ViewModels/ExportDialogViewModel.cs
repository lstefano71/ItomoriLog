using ItomoriLog.Core.Export;

using ReactiveUI;

using System.Reactive;
using System.Reactive.Linq;

namespace ItomoriLog.UI.ViewModels;

public class ExportDialogViewModel : ViewModelBase
{
    private bool _isOpen;
    private ExportFormat _selectedFormat = ExportFormat.Csv;
    private string _outputPath = "";
    private ExportScope _selectedScope = ExportScope.CurrentView;
    private bool _isExporting;
    private double _progressPercent;
    private string _progressText = "";
    private string _errorText = "";
    private SessionShellViewModel? _boundSession;

    public ExportDialogViewModel()
    {
        ExportCommand = ReactiveCommand.CreateFromTask(
            DoExportAsync,
            this.WhenAnyValue(x => x.OutputPath, x => x.IsExporting,
                (path, exporting) => !string.IsNullOrWhiteSpace(path) && !exporting));

        CancelCommand = ReactiveCommand.Create(() => { IsOpen = false; });

        BrowseCommand = ReactiveCommand.Create(() => { });
    }

    public bool IsOpen {
        get => _isOpen;
        set => this.RaiseAndSetIfChanged(ref _isOpen, value);
    }

    public ExportFormat SelectedFormat {
        get => _selectedFormat;
        set => this.RaiseAndSetIfChanged(ref _selectedFormat, value);
    }

    public bool IsCsv {
        get => _selectedFormat == ExportFormat.Csv;
        set { if (value) SelectedFormat = ExportFormat.Csv; }
    }

    public bool IsJsonLines {
        get => _selectedFormat == ExportFormat.JsonLines;
        set { if (value) SelectedFormat = ExportFormat.JsonLines; }
    }

    public bool IsParquet {
        get => _selectedFormat == ExportFormat.Parquet;
        set { if (value) SelectedFormat = ExportFormat.Parquet; }
    }

    public string OutputPath {
        get => _outputPath;
        set => this.RaiseAndSetIfChanged(ref _outputPath, value);
    }

    public ExportScope SelectedScope {
        get => _selectedScope;
        set => this.RaiseAndSetIfChanged(ref _selectedScope, value);
    }

    public bool IsCurrentViewScope {
        get => SelectedScope == ExportScope.CurrentView;
        set { if (value) SelectedScope = ExportScope.CurrentView; }
    }

    public bool IsFullSessionScope {
        get => SelectedScope == ExportScope.FullSession;
        set { if (value) SelectedScope = ExportScope.FullSession; }
    }

    public bool IsExporting {
        get => _isExporting;
        set => this.RaiseAndSetIfChanged(ref _isExporting, value);
    }

    public double ProgressPercent {
        get => _progressPercent;
        set => this.RaiseAndSetIfChanged(ref _progressPercent, value);
    }

    public string ProgressText {
        get => _progressText;
        set => this.RaiseAndSetIfChanged(ref _progressText, value);
    }

    public string ErrorText {
        get => _errorText;
        set => this.RaiseAndSetIfChanged(ref _errorText, value);
    }

    public ReactiveCommand<Unit, Unit> ExportCommand { get; }
    public ReactiveCommand<Unit, Unit> CancelCommand { get; }
    public ReactiveCommand<Unit, Unit> BrowseCommand { get; }

    /// <summary>
    /// Called by the view after file picker completes.
    /// </summary>
    public Func<ExportOptions, IProgress<ExportProgress>, CancellationToken, Task<long>>? ExportCallback { get; set; }

    public void Open()
    {
        ErrorText = "";
        ProgressText = "";
        ProgressPercent = 0;
        if (string.IsNullOrWhiteSpace(OutputPath)) {
            var ext = SelectedFormat switch {
                ExportFormat.Csv => "csv",
                ExportFormat.JsonLines => "jsonl",
                ExportFormat.Parquet => "parquet",
                _ => "csv"
            };
            var stem = SanitizeFileStem(_boundSession?.Title ?? "export");
            OutputPath = Path.Combine(
                _boundSession?.SessionFolder ?? Environment.CurrentDirectory,
                "exports",
                $"{stem}_{DateTime.UtcNow:yyyyMMdd_HHmmss}.{ext}");
        }
        IsOpen = true;
    }

    public void BindSession(SessionShellViewModel session)
    {
        _boundSession = session;
        ExportCallback = session.ExecuteExportAsync;

        if (string.IsNullOrWhiteSpace(OutputPath)) {
            var ext = SelectedFormat switch {
                ExportFormat.Csv => "csv",
                ExportFormat.JsonLines => "jsonl",
                ExportFormat.Parquet => "parquet",
                _ => "csv"
            };
            var stem = SanitizeFileStem(session.Title);
            OutputPath = Path.Combine(session.SessionFolder, "exports", $"{stem}_{DateTime.UtcNow:yyyyMMdd_HHmmss}.{ext}");
        }
    }

    private async Task DoExportAsync()
    {
        if (ExportCallback is null) return;

        IsExporting = true;
        ErrorText = "";

        try {
            var options = _boundSession is not null
                ? _boundSession.BuildExportOptions(SelectedScope, SelectedFormat, OutputPath)
                : new ExportOptions(SelectedFormat, OutputPath, Scope: SelectedScope);
            var progress = new Progress<ExportProgress>(p => {
                ProgressText = p.Status;
                if (p.TotalEstimate > 0)
                    ProgressPercent = (double)p.RowsWritten / p.TotalEstimate * 100;
            });

            var count = await ExportCallback(options, progress, CancellationToken.None);
            ProgressText = $"Export complete — {count:N0} rows exported";
            ProgressPercent = 100;
        } catch (Exception ex) {
            ErrorText = $"Export failed: {ex.Message}";
        } finally {
            IsExporting = false;
        }
    }

    private static string SanitizeFileStem(string value)
    {
        var invalid = Path.GetInvalidFileNameChars();
        var cleaned = new string(value.Select(c => invalid.Contains(c) ? '_' : c).ToArray());
        return string.IsNullOrWhiteSpace(cleaned) ? "export" : cleaned;
    }
}
