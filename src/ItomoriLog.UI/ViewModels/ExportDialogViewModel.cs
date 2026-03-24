using System.Reactive;
using System.Reactive.Linq;
using ReactiveUI;
using ItomoriLog.Core.Export;

namespace ItomoriLog.UI.ViewModels;

public class ExportDialogViewModel : ViewModelBase
{
    private bool _isOpen;
    private ExportFormat _selectedFormat = ExportFormat.Csv;
    private string _outputPath = "";
    private bool _exportCurrentFilter = true;
    private bool _isExporting;
    private double _progressPercent;
    private string _progressText = "";
    private string _errorText = "";

    public ExportDialogViewModel()
    {
        ExportCommand = ReactiveCommand.CreateFromTask(
            DoExportAsync,
            this.WhenAnyValue(x => x.OutputPath, x => x.IsExporting,
                (path, exporting) => !string.IsNullOrWhiteSpace(path) && !exporting));

        CancelCommand = ReactiveCommand.Create(() => { IsOpen = false; });

        BrowseCommand = ReactiveCommand.Create(() => { });
    }

    public bool IsOpen
    {
        get => _isOpen;
        set => this.RaiseAndSetIfChanged(ref _isOpen, value);
    }

    public ExportFormat SelectedFormat
    {
        get => _selectedFormat;
        set => this.RaiseAndSetIfChanged(ref _selectedFormat, value);
    }

    public bool IsCsv
    {
        get => _selectedFormat == ExportFormat.Csv;
        set { if (value) SelectedFormat = ExportFormat.Csv; }
    }

    public bool IsJsonLines
    {
        get => _selectedFormat == ExportFormat.JsonLines;
        set { if (value) SelectedFormat = ExportFormat.JsonLines; }
    }

    public bool IsParquet
    {
        get => _selectedFormat == ExportFormat.Parquet;
        set { if (value) SelectedFormat = ExportFormat.Parquet; }
    }

    public string OutputPath
    {
        get => _outputPath;
        set => this.RaiseAndSetIfChanged(ref _outputPath, value);
    }

    public bool ExportCurrentFilter
    {
        get => _exportCurrentFilter;
        set => this.RaiseAndSetIfChanged(ref _exportCurrentFilter, value);
    }

    public bool IsExporting
    {
        get => _isExporting;
        set => this.RaiseAndSetIfChanged(ref _isExporting, value);
    }

    public double ProgressPercent
    {
        get => _progressPercent;
        set => this.RaiseAndSetIfChanged(ref _progressPercent, value);
    }

    public string ProgressText
    {
        get => _progressText;
        set => this.RaiseAndSetIfChanged(ref _progressText, value);
    }

    public string ErrorText
    {
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
        IsOpen = true;
    }

    private async Task DoExportAsync()
    {
        if (ExportCallback is null) return;

        IsExporting = true;
        ErrorText = "";

        try
        {
            var options = new ExportOptions(SelectedFormat, OutputPath);
            var progress = new Progress<ExportProgress>(p =>
            {
                ProgressText = p.Status;
                if (p.TotalEstimate > 0)
                    ProgressPercent = (double)p.RowsWritten / p.TotalEstimate * 100;
            });

            var count = await ExportCallback(options, progress, CancellationToken.None);
            ProgressText = $"Export complete — {count:N0} rows exported";
            ProgressPercent = 100;
        }
        catch (Exception ex)
        {
            ErrorText = $"Export failed: {ex.Message}";
        }
        finally
        {
            IsExporting = false;
        }
    }
}
