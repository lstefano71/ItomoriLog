using System.Reactive;
using ReactiveUI;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.UI.ViewModels;

public class CrashRecoveryViewModel : ViewModelBase
{
    private bool _isVisible;
    private string _message = "";
    private readonly Action? _onResume;
    private readonly Action? _onDismiss;

    public CrashRecoveryViewModel()
    {
        ResumeCommand = ReactiveCommand.Create(OnResume);
        DismissCommand = ReactiveCommand.Create(OnDismiss);
    }

    public CrashRecoveryViewModel(CrashRecoveryStatus status, Action? onResume = null, Action? onDismiss = null)
        : this()
    {
        _onResume = onResume;
        _onDismiss = onDismiss;

        if (status.CrashDetected)
        {
            IsVisible = true;
            Message = status.IncompleteSegmentCount > 0
                ? $"Previous ingest was interrupted. {status.IncompleteSegmentCount} segment(s) incomplete."
                : "Previous session did not shut down cleanly.";
        }
    }

    public bool IsVisible
    {
        get => _isVisible;
        set => this.RaiseAndSetIfChanged(ref _isVisible, value);
    }

    public string Message
    {
        get => _message;
        set => this.RaiseAndSetIfChanged(ref _message, value);
    }

    public ReactiveCommand<Unit, Unit> ResumeCommand { get; }
    public ReactiveCommand<Unit, Unit> DismissCommand { get; }

    private void OnResume()
    {
        _onResume?.Invoke();
        IsVisible = false;
    }

    private void OnDismiss()
    {
        _onDismiss?.Invoke();
        IsVisible = false;
    }
}
