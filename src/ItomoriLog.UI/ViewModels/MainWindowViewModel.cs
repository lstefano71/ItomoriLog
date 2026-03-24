using ReactiveUI;
using System.Reactive.Linq;
using System;

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
                () =>
                {
                    if (CurrentView is SessionShellViewModel session)
                        ExportDialog.BindSession(session);
                    ExportDialog.Open();
                }),
            new PaletteCommand("Toggle Detail Panel", "Show or hide the log detail panel", "Ctrl+D",
                () =>
                {
                    if (CurrentView is SessionShellViewModel session && session.LogsPage is { } logs)
                        logs.IsDetailOpen = !logs.IsDetailOpen;
                }),
            new PaletteCommand("Refresh", "Refresh current results", "F5",
                () =>
                {
                    if (CurrentView is SessionShellViewModel session && session.LogsPage is { } logs)
                        logs.RefreshCommand.Execute().Subscribe();
                }),
            new PaletteCommand("Toggle Staging Pane", "Show or hide the staging queue", "Ctrl+Shift+S",
                () =>
                {
                    if (CurrentView is SessionShellViewModel session)
                        session.ToggleStagingPaneCommand.Execute().Subscribe();
                }),
            new PaletteCommand("Add Files...", "Open file picker to ingest more logs", "Ctrl+O",
                () =>
                {
                    if (CurrentView is SessionShellViewModel session)
                    {
                        session.OpenStagingPane();
                        session.RequestOpenFilePicker();
                    }
                }),
            new PaletteCommand("Start Ingestion", "Start ingesting staged files and folders", "Ctrl+I",
                () =>
                {
                    if (CurrentView is SessionShellViewModel session)
                        session.StartIngestionCommand.Execute().Subscribe();
                }),
            new PaletteCommand("Go to Skips", "View skipped records from ingestion", null,
                () => { }),
            new PaletteCommand("About", "About ItomoriLog", null,
                () => AboutDialog.Toggle()),
        ]);

        this.WhenAnyValue(x => x.CurrentView)
            .OfType<SessionShellViewModel>()
            .Subscribe(session => ExportDialog.BindSession(session));
    }

    public string[] StartupArgs { get; private set; } = [];

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
        if (CurrentView is SessionShellViewModel currentSession)
            currentSession.ReleaseSessionLock();
        CurrentView = new SessionShellViewModel(this, sessionFolder);
    }

    public void NavigateToWelcome()
    {
        if (CurrentView is SessionShellViewModel currentSession)
            currentSession.ReleaseSessionLock();
        CurrentView = new WelcomeViewModel(this);
    }

    public void SetStartupArgs(string[]? args)
    {
        StartupArgs = args ?? [];
    }
}
