using System.Reactive;
using ReactiveUI;

namespace ItomoriLog.UI.ViewModels;

public class AboutDialogViewModel : ViewModelBase
{
    private bool _isOpen;

    public AboutDialogViewModel(string appName, string appVersion, string tagline)
    {
        AppName = appName;
        AppVersion = appVersion;
        Tagline = tagline;

        CloseCommand = ReactiveCommand.Create(Close);
    }

    public string AppName { get; }
    public string AppVersion { get; }
    public string Tagline { get; }

    public string Credits => "Built with Avalonia, DuckDB, and .NET 10";
    public string Copyright => $"© {DateTime.UtcNow.Year} ItomoriLog Contributors";

    public bool IsOpen
    {
        get => _isOpen;
        set => this.RaiseAndSetIfChanged(ref _isOpen, value);
    }

    public ReactiveCommand<Unit, Unit> CloseCommand { get; }

    public void Open()
    {
        IsOpen = true;
    }

    public void Close()
    {
        IsOpen = false;
    }

    public void Toggle()
    {
        if (IsOpen)
            Close();
        else
            Open();
    }
}
