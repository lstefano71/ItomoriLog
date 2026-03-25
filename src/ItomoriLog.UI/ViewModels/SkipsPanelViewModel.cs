using DuckDB.NET.Data;

using ItomoriLog.Core.Model;
using ItomoriLog.Core.Query;

using ReactiveUI;

using System.Collections.ObjectModel;
using System.Reactive;
using System.Reactive.Linq;

namespace ItomoriLog.UI.ViewModels;

public class SkipsPanelViewModel : ViewModelBase
{
    private ObservableCollection<SkipGroupViewModel> _skipGroups = [];
    private SkipSegmentViewModel? _selectedSkip;
    private long _totalSkipCount;
    private SkipReasonCode? _selectedReasonFilter;
    private readonly DuckDBConnection _connection;

    public SkipsPanelViewModel(DuckDBConnection connection)
    {
        _connection = connection;

        LoadCommand = ReactiveCommand.CreateFromTask(LoadSkipsAsync);
        JumpToContextCommand = ReactiveCommand.Create<SkipSegmentViewModel>(OnJumpToContext,
            this.WhenAnyValue(x => x.SelectedSkip).Select(s => s is not null));

        ReasonFilterOptions = [null, .. Enum.GetValues<SkipReasonCode>()];
    }

    public ObservableCollection<SkipGroupViewModel> SkipGroups {
        get => _skipGroups;
        set => this.RaiseAndSetIfChanged(ref _skipGroups, value);
    }

    public SkipSegmentViewModel? SelectedSkip {
        get => _selectedSkip;
        set => this.RaiseAndSetIfChanged(ref _selectedSkip, value);
    }

    public long TotalSkipCount {
        get => _totalSkipCount;
        set => this.RaiseAndSetIfChanged(ref _totalSkipCount, value);
    }

    public SkipReasonCode? SelectedReasonFilter {
        get => _selectedReasonFilter;
        set {
            this.RaiseAndSetIfChanged(ref _selectedReasonFilter, value);
            Observable.StartAsync(LoadSkipsAsync).Subscribe();
        }
    }

    public IReadOnlyList<SkipReasonCode?> ReasonFilterOptions { get; }

    public ReactiveCommand<Unit, Unit> LoadCommand { get; }
    public ReactiveCommand<SkipSegmentViewModel, Unit> JumpToContextCommand { get; }

    public event Action<SkipJumpRequest>? JumpRequested;

    private async Task LoadSkipsAsync()
    {
        var query = new SkipsQuery(_connection);
        var groups = await query.QueryAsync(reasonCodeFilter: _selectedReasonFilter);

        var vms = new ObservableCollection<SkipGroupViewModel>();
        long total = 0;

        foreach (var group in groups) {
            var segmentVms = group.Segments
                .Select(s => new SkipSegmentViewModel(s, group.SourcePath))
                .ToList();
            vms.Add(new SkipGroupViewModel(group.SourcePath, segmentVms));
            total += segmentVms.Sum(s => s.RecordCount);
        }

        SkipGroups = vms;
        TotalSkipCount = total;
    }

    private void OnJumpToContext(SkipSegmentViewModel segment)
    {
        JumpRequested?.Invoke(new SkipJumpRequest(
            segment.StartOffset,
            segment.EndOffset,
            segment.SourcePath));
    }
}

public class SkipGroupViewModel : ViewModelBase
{
    public SkipGroupViewModel(string sourcePath, IReadOnlyList<SkipSegmentViewModel> segments)
    {
        SourcePath = sourcePath;
        Segments = segments;
        TotalRecordCount = segments.Sum(s => s.RecordCount);
    }

    public string SourcePath { get; }
    public IReadOnlyList<SkipSegmentViewModel> Segments { get; }
    public long TotalRecordCount { get; }
}

public class SkipSegmentViewModel : ViewModelBase
{
    public SkipSegmentViewModel(SkipSegmentSummary summary, string sourcePath)
    {
        ReasonCode = summary.ReasonCode;
        StartOffset = summary.StartOffset;
        EndOffset = summary.EndOffset;
        RecordCount = summary.RecordCount;
        SamplePreview = summary.SamplePrefix is not null
            ? (summary.SamplePrefix.Length > 80
                ? string.Concat(summary.SamplePrefix.AsSpan(0, 80), "…")
                : summary.SamplePrefix)
            : null;
        SourcePath = sourcePath;
        IsError = ReasonCode is SkipReasonCode.DecodeError or SkipReasonCode.IOError
            or SkipReasonCode.ZipEntryCorrupt or SkipReasonCode.Abandoned;
    }

    public SkipReasonCode ReasonCode { get; }
    public long? StartOffset { get; }
    public long? EndOffset { get; }
    public long RecordCount { get; }
    public string? SamplePreview { get; }
    public string SourcePath { get; }
    public bool IsError { get; }
}
