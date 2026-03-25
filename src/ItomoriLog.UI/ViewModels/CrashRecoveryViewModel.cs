using ItomoriLog.Core.Storage;

using ReactiveUI;

using System.Reactive;

namespace ItomoriLog.UI.ViewModels;

public class CrashRecoveryViewModel : ViewModelBase
{
    private bool _isVisible;
    private bool _isBusy;
    private string _message = "";
    private readonly Func<Task>? _onResume;
    private readonly Action? _onDismiss;

    public CrashRecoveryViewModel()
    {
        ResumeCommand = ReactiveCommand.CreateFromTask(
            OnResumeAsync,
            this.WhenAnyValue(x => x.IsVisible, x => x.IsBusy, (visible, busy) => visible && !busy));
        DismissCommand = ReactiveCommand.Create(OnDismiss);
    }

    public CrashRecoveryViewModel(
        CrashRecoveryStatus status,
        Func<Task>? onResume = null,
        Action? onDismiss = null)
        : this()
    {
        _onResume = onResume;
        _onDismiss = onDismiss;

        if (status.CanResume) {
            IsVisible = true;
            Message = status.ResumableSourcePaths.Count == 1
                ? "An interrupted ingest can be resumed for 1 staged source."
                : $"An interrupted ingest can be resumed for {status.ResumableSourcePaths.Count} staged sources.";
        }
    }

    public bool IsVisible {
        get => _isVisible;
        set => this.RaiseAndSetIfChanged(ref _isVisible, value);
    }

    public bool IsBusy {
        get => _isBusy;
        set => this.RaiseAndSetIfChanged(ref _isBusy, value);
    }

    public string Message {
        get => _message;
        set => this.RaiseAndSetIfChanged(ref _message, value);
    }

    public ReactiveCommand<Unit, Unit> ResumeCommand { get; }
    public ReactiveCommand<Unit, Unit> DismissCommand { get; }

    private async Task OnResumeAsync()
    {
        IsBusy = true;
        try {
            if (_onResume is not null)
                await _onResume();

            IsVisible = false;
        } finally {
            IsBusy = false;
        }
    }

    private void OnDismiss()
    {
        _onDismiss?.Invoke();
        IsVisible = false;
    }
}
