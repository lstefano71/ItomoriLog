using ReactiveUI;

namespace ItomoriLog.UI.ViewModels;

public class MainWindowViewModel : ReactiveObject
{
    private ViewModelBase _currentView;

    public MainWindowViewModel()
    {
        _currentView = new WelcomeViewModel(this);

        ExportDialog = new ExportDialogViewModel();
        AboutDialog = new AboutDialogViewModel("ItomoriLog", "0.1.0", "Braiding time from your logs");

        CommandPalette = new CommandPaletteViewModel(
        [
            new PaletteCommand("New Session", "Create a new log investigation session", "Ctrl+N",
                () => NavigateToWelcome()),
            new PaletteCommand("Open Session", "Open a recent session", "Ctrl+O",
                () => NavigateToWelcome()),
            new PaletteCommand("Export...", "Export current session data", "Ctrl+E",
                () => ExportDialog.Open()),
            new PaletteCommand("Toggle Detail Panel", "Show or hide the log detail panel", "Ctrl+D",
                () => { }),
            new PaletteCommand("Go to Skips", "View skipped records from ingestion", null,
                () => { }),
            new PaletteCommand("About", "About ItomoriLog", null,
                () => AboutDialog.Toggle()),
        ]);
    }

    public ViewModelBase CurrentView
    {
        get => _currentView;
        set => this.RaiseAndSetIfChanged(ref _currentView, value);
    }

    public CommandPaletteViewModel CommandPalette { get; }
    public ExportDialogViewModel ExportDialog { get; }
    public AboutDialogViewModel AboutDialog { get; }

    public void NavigateToSession(string sessionFolder)
    {
        CurrentView = new SessionShellViewModel(this, sessionFolder);
    }

    public void NavigateToWelcome()
    {
        CurrentView = new WelcomeViewModel(this);
    }
}
