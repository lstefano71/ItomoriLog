using System.Collections.ObjectModel;
using System.Reactive;
using System.Reactive.Linq;
using ReactiveUI;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.UI.ViewModels;

public class WelcomeViewModel : ViewModelBase
{
    private readonly MainWindowViewModel _main;
    private string _sessionTitle = SessionDefaults.BuildDefaultSessionTitle();
    private string _sessionDescription = "";
    private string _sessionPath;
    private string _defaultTimezone = SessionDefaults.ResolveDefaultTimezone(null);

    public WelcomeViewModel(MainWindowViewModel main)
    {
        _main = main;
        _sessionPath = SessionPaths.DefaultSessionsRoot;
        RecentSessions = new ObservableCollection<RecentSessionEntry>();

        CreateSessionCommand = ReactiveCommand.CreateFromTask(CreateSessionAsync);
        CreateSessionFromDroppedPathsCommand = ReactiveCommand.CreateFromTask<IReadOnlyList<string>>(CreateSessionFromDroppedPathsAsync);
        RefreshRecentCommand = ReactiveCommand.CreateFromTask(RefreshRecentSessionsAsync);
        OpenSessionCommand = ReactiveCommand.Create<RecentSessionEntry>(entry =>
        {
            _main.NavigateToSession(entry.SessionFolder);
        });

        // Load recent sessions on creation
        Observable.StartAsync(RefreshRecentSessionsAsync).Subscribe();
    }

    public string SessionTitle
    {
        get => _sessionTitle;
        set => this.RaiseAndSetIfChanged(ref _sessionTitle, value);
    }

    public string SessionDescription
    {
        get => _sessionDescription;
        set => this.RaiseAndSetIfChanged(ref _sessionDescription, value);
    }

    public string SessionPath
    {
        get => _sessionPath;
        set => this.RaiseAndSetIfChanged(ref _sessionPath, value);
    }

    public string DefaultTimezone
    {
        get => _defaultTimezone;
        set => this.RaiseAndSetIfChanged(ref _defaultTimezone, value);
    }

    public ObservableCollection<RecentSessionEntry> RecentSessions { get; }

    public ReactiveCommand<Unit, Unit> CreateSessionCommand { get; }
    public ReactiveCommand<IReadOnlyList<string>, Unit> CreateSessionFromDroppedPathsCommand { get; }
    public ReactiveCommand<Unit, Unit> RefreshRecentCommand { get; }
    public ReactiveCommand<RecentSessionEntry, Unit> OpenSessionCommand { get; }

    private async Task CreateSessionAsync()
    {
        if (string.IsNullOrWhiteSpace(SessionTitle))
            SessionTitle = SessionDefaults.BuildDefaultSessionTitle();

        DefaultTimezone = SessionDefaults.ResolveDefaultTimezone(DefaultTimezone);
        if (!SessionDefaults.IsValidTimezoneId(DefaultTimezone))
            DefaultTimezone = TimeZoneInfo.Local.Id;

        var sessionFolder = SessionPaths.CreateNew(SessionPath, SessionTitle);
        var dbPath = SessionPaths.GetDbPath(sessionFolder);

        using (var factory = new DuckLakeConnectionFactory(dbPath))
        {
            var store = new SessionStore(factory);
            await store.InitializeAsync(
                SessionTitle,
                string.IsNullOrWhiteSpace(SessionDescription) ? null : SessionDescription,
                string.IsNullOrWhiteSpace(DefaultTimezone) ? null : DefaultTimezone);
        }

        // Add to global store
        using var globalStore = new GlobalStore();
        await globalStore.AddRecentSessionAsync(sessionFolder, SessionTitle, SessionDescription);

        _main.NavigateToSession(sessionFolder);
    }

    private async Task CreateSessionFromDroppedPathsAsync(IReadOnlyList<string> droppedPaths)
    {
        await CreateSessionAsync();
        if (_main.CurrentView is SessionShellViewModel shell && droppedPaths.Count > 0)
            shell.StageFilesCommand.Execute(droppedPaths).Subscribe();
    }

    private async Task RefreshRecentSessionsAsync()
    {
        using var globalStore = new GlobalStore();
        var sessions = await globalStore.GetRecentSessionsAsync();

        RecentSessions.Clear();
        foreach (var s in sessions)
            RecentSessions.Add(s);
    }
}
