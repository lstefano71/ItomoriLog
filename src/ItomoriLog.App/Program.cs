using Avalonia;
using Avalonia.ReactiveUI;
using ItomoriLog.Core.Storage;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Query;
using ItomoriLog.Core.Model;

namespace ItomoriLog.App;

internal static class Program
{
    [STAThread]
    public static void Main(string[] args)
    {
        if (TryRunCli(args))
            return;

        BuildAvaloniaApp().StartWithClassicDesktopLifetime(args);
    }

    public static AppBuilder BuildAvaloniaApp()
        => AppBuilder.Configure<App>()
            .UsePlatformDetect()
            .WithInterFont()
            .UseReactiveUI()
            .LogToTrace();

    private static bool TryRunCli(string[] args)
    {
        if (args.Length == 0)
            return false;

        var command = args[0].Trim().ToLowerInvariant();
        if (command is "ingest")
        {
            RunIngestCli(args.Skip(1).ToArray());
            return true;
        }

        if (command is "browse")
        {
            RunBrowseCli(args.Skip(1).ToArray());
            return true;
        }

        if (command is "--help" or "-h" or "help")
        {
            PrintCliHelp();
            return true;
        }

        return false;
    }

    private static void RunIngestCli(string[] args)
    {
        var options = ParseIngestArgs(args);
        if (options.InputPaths.Count == 0)
            throw new InvalidOperationException("CLI ingest requires at least one input path.");

        var sessionsRoot = string.IsNullOrWhiteSpace(options.SessionOut)
            ? SessionPaths.DefaultSessionsRoot
            : options.SessionOut!;
        Directory.CreateDirectory(sessionsRoot);

        var sessionTitle = string.IsNullOrWhiteSpace(options.SessionTitle)
            ? SessionDefaults.BuildDefaultSessionTitle()
            : options.SessionTitle!.Trim();
        var sessionDescription = string.IsNullOrWhiteSpace(options.SessionDescription)
            ? null
            : options.SessionDescription!.Trim();
        var timezone = SessionDefaults.ResolveDefaultTimezone(options.DefaultTimezone);
        if (!SessionDefaults.IsValidTimezoneId(timezone))
            throw new InvalidOperationException($"Invalid timezone '{timezone}'.");

        var sessionFolder = SessionPaths.CreateNew(sessionsRoot, sessionTitle);
        var dbPath = SessionPaths.GetDbPath(sessionFolder);

        using var factory = new DuckLakeConnectionFactory(dbPath);
        var conn = factory.GetConnectionAsync().GetAwaiter().GetResult();
        SchemaInitializer.EnsureSchemaAsync(conn).GetAwaiter().GetResult();

        var store = new SessionStore(factory);
        store.InitializeAsync(sessionTitle, sessionDescription, timezone).GetAwaiter().GetResult();

        var planner = new FileIngestPlanner(conn);
        var plan = planner.PlanAsync(options.InputPaths, ExistingFileAction.Skip).GetAwaiter().GetResult();

        var orchestrator = new IngestOrchestrator(conn);
        var progress = new Progress<IngestProgressUpdate>(update =>
        {
            Console.WriteLine(
                $"[{update.Phase}] {Path.GetFileName(update.SourcePath)} {update.RecordsProcessed:N0} rec {update.BytesProcessed:N0}/{update.BytesTotal:N0} bytes");
        });

        var tzConfig = BuildCliTimeBasis(timezone);
        var result = orchestrator.IngestFilesAsync(plan.FilesToIngest, tzConfig, progress).GetAwaiter().GetResult();

        using var globalStore = new GlobalStore();
        globalStore.AddRecentSessionAsync(sessionFolder, sessionTitle, sessionDescription).GetAwaiter().GetResult();

        Console.WriteLine($"Session: {sessionFolder}");
        Console.WriteLine($"Ingest complete. Files={result.FilesProcessed}, Rows={result.TotalRows:N0}, Skips={result.Skips.Count}");
    }

    private static void RunBrowseCli(string[] args)
    {
        var sessionPath = ParseBrowseArgs(args);
        if (string.IsNullOrWhiteSpace(sessionPath))
            throw new InvalidOperationException("CLI browse requires --session <path-to-ItomoriLog.duckdb>.");
        if (!File.Exists(sessionPath))
            throw new FileNotFoundException("Session database not found.", sessionPath);

        using var factory = new DuckLakeConnectionFactory(sessionPath);
        var conn = factory.GetConnectionAsync().GetAwaiter().GetResult();
        SchemaInitializer.EnsureSchemaAsync(conn).GetAwaiter().GetResult();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM logs";
        var count = Convert.ToInt64(cmd.ExecuteScalar());
        Console.WriteLine($"Session DB: {sessionPath}");
        Console.WriteLine($"Records: {count:N0}");

        var planner = new QueryPlanner();
        var filter = FilterState.Empty;
        var query = planner.Plan(filter, null, PageDirection.Forward, pageSize: 5, tickContext: null);
        using var q = conn.CreateCommand();
        q.CommandText = query.Sql;
        foreach (var p in query.Parameters)
            q.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = p });
        using var reader = q.ExecuteReader();
        Console.WriteLine("Sample rows:");
        while (reader.Read())
        {
            Console.WriteLine($"{reader.GetDateTime(0):O} [{reader.GetString(4)}] {reader.GetString(11)}");
        }
    }

    private static TimeBasisConfig BuildCliTimeBasis(string timezoneId)
    {
        if (string.Equals(timezoneId, TimeZoneInfo.Local.Id, StringComparison.OrdinalIgnoreCase))
            return new TimeBasisConfig(TimeBasis.Local);

        if (string.Equals(timezoneId, "UTC", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(timezoneId, "Etc/UTC", StringComparison.OrdinalIgnoreCase))
        {
            return new TimeBasisConfig(TimeBasis.Utc);
        }

        return new TimeBasisConfig(TimeBasis.Zone, TimeZoneId: timezoneId);
    }

    private static (List<string> InputPaths, string? SessionOut, string? SessionTitle, string? SessionDescription, string? DefaultTimezone)
        ParseIngestArgs(string[] args)
    {
        var inputs = new List<string>();
        string? sessionOut = null;
        string? sessionTitle = null;
        string? sessionDescription = null;
        string? defaultTimezone = null;

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (string.Equals(arg, "--session-out", StringComparison.OrdinalIgnoreCase))
            {
                sessionOut = RequireValue(args, ref i, "--session-out");
                continue;
            }
            if (string.Equals(arg, "--session-title", StringComparison.OrdinalIgnoreCase))
            {
                sessionTitle = RequireValue(args, ref i, "--session-title");
                continue;
            }
            if (string.Equals(arg, "--session-description", StringComparison.OrdinalIgnoreCase))
            {
                sessionDescription = RequireValue(args, ref i, "--session-description");
                continue;
            }
            if (string.Equals(arg, "--default-timezone", StringComparison.OrdinalIgnoreCase))
            {
                defaultTimezone = RequireValue(args, ref i, "--default-timezone");
                continue;
            }
            if (arg.StartsWith("--", StringComparison.Ordinal))
                throw new InvalidOperationException($"Unknown ingest option '{arg}'.");

            inputs.Add(arg);
        }

        return (inputs, sessionOut, sessionTitle, sessionDescription, defaultTimezone);
    }

    private static string? ParseBrowseArgs(string[] args)
    {
        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (string.Equals(arg, "--session", StringComparison.OrdinalIgnoreCase))
                return RequireValue(args, ref i, "--session");
        }
        return null;
    }

    private static string RequireValue(string[] args, ref int index, string optionName)
    {
        var valueIndex = index + 1;
        if (valueIndex >= args.Length)
            throw new InvalidOperationException($"Missing value for {optionName}.");
        index = valueIndex;
        return args[valueIndex];
    }

    private static void PrintCliHelp()
    {
        Console.WriteLine("ItomoriLog CLI");
        Console.WriteLine("  itomorilog ingest <path> [<path> ...] [--session-out <dir>] [--session-title <text>] [--session-description <text>] [--default-timezone <tz>]");
        Console.WriteLine("  itomorilog browse --session <path-to-ItomoriLog.duckdb>");
    }
}
