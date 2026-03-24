## Session-per-DB model

### What it means

*   **Every new session creates its own DuckLake/DuckDB file** (e.g., `~/ItomoriLog/Sessions/2026-03-24_ItomoriLog_09-47-02/ItomoriLog.duckdb`).
*   All session data (tables `logs`, `skips`, `segments`, `rules`, plus snapshots/catalog) lives **inside that one file/folder**.
*   The **session title & description** live in a tiny **session metadata table** inside the session DB and are editable from the UI.
*   Your “recent sessions” view is just a list of **paths**, with the title/description read from each DB at open time.

> You still can keep a *lightweight* “RecentSessions.json” in your app data to avoid scanning the filesystem, but it’s only a cache/index; **the source of truth is the session DB itself**.

***

## Folder & file layout (suggested)

    ~/ItomoriLog/Sessions/
      2026-03-24_09-47-02_ItomoriLog/     # Session folder
        ItomoriLog.duckdb                 # The DuckDB file with DuckLake tables & metadata
        assets/                           # (optional) any embedded assets or exports
        exports/                          # (optional) user-initiated exports

**Naming defaults**

*   Folder: `${yyyy-MM-dd}_${HH-mm-ss}_ItomoriLog`
*   DB file: `ItomoriLog.duckdb`

Both can be changed after creation; the **title/description** are independent of the folder name.

***

## Tables (add a tiny `session` table)

You already have `logs`, `skips`, `segments`. Add:

```sql
CREATE TABLE IF NOT EXISTS session (
  session_id        VARCHAR PRIMARY KEY,     -- GUID
  created_utc       TIMESTAMP NOT NULL,
  modified_utc      TIMESTAMP NOT NULL,
  title             VARCHAR NOT NULL,        -- editable
  description       VARCHAR,                 -- editable
  created_by        VARCHAR,                 -- optional (OS user)
  default_timezone  VARCHAR,                 -- e.g., "Europe/Rome" or "Local"
  app_version       VARCHAR                  -- "ItomoriLog 0.1.0"
);
```

**On create (new session)**

```sql
INSERT INTO session(session_id, created_utc, modified_utc, title, description, created_by, default_timezone, app_version)
VALUES ($guid, now(), now(),
        $autoTitle, $autoDescription, $osUser, $tz, $ver);
```

*   **Auto title** suggestion: `ItomoriLog Session — {yyyy-MM-dd HH:mm}`
*   **Auto description** suggestion: `Created from {N} files at {path or zip}`

**On edit (from UI)**

```sql
UPDATE session
SET title = $title,
    description = $description,
    modified_utc = now()
WHERE session_id = $id;
```

***

## UI adjustments

### Welcome screen

*   **Left column**: “Create New Session”
    *   Path picker (files/dirs/ZIPs)
    *   Optional “Session title/description” (prefilled; editable)
    *   Default timezone (prefilled from app settings; editable)
    *   **Create** → will create the folder + duckdb file and write `session` row

*   **Right column**: “Recent Sessions”
    *   Virtualized list of entries (read from local cache)
    *   Each row shows **Title**, **modified date**, **path**
    *   On focus/hover, the app lazily **reads the `session` table** from that DB to confirm the title/desc (keeps cache fresh)
    *   Context menu: **Rename title/description**, **Open folder**, **Duplicate session**, **Remove from list** (doesn’t delete files)

### In the main shell (top left)

*   A **Session header** area with:
    *   Title (TextBlock → click to edit inline)
    *   Description (TextBlock → expands; click to edit)
    *   Session path (subtle, copyable)
    *   Default TZ chip (click to change)

### During ingestion

*   The session gets **created immediately** with the provisional title/desc → so the `session` record exists even if ingestion is still running. If the user cancels, the DB still contains whatever was written; your choice whether to prompt to delete/keep.

***

## CLI ergonomics (optional)

    # Create & ingest in one go; auto title/desc
    itomori ingest ./logs/server-*.log --session-out ~/ItomoriLog/Sessions

    # Explicit session title/description
    itomori ingest ./logs \
      --session-title "Release Night — API cluster" \
      --session-description "Prod API logs + nginx access" \
      --session-out ~/ItomoriLog/Sessions

    # Open an existing session
    itomori browse --session ~/ItomoriLog/Sessions/2026-03-24_09-47-02_ItomoriLog/ItomoriLog.duckdb

***

## Current implementation status (issues-001 alignment)

Implemented in this repo:

*   Welcome screen supports recent sessions, default title/timezone, and drag-and-drop paths to create a new session.
*   Session shell supports staged files/folders, drag-and-drop, remove/clear, and explicit **Start Ingestion**.
*   Background sniffing runs as soon as files are staged; low-confidence/ambiguous guesses require confirmation.
*   Per-file/overall ingest progress is shown (bytes, records, throughput, ETA, phase) while browse view stays active.
*   Post-ingest re-ingest is available per staged source and honors selected detection override.
*   CLI MVP is available:
    * `itomorilog ingest <paths...> [--session-out] [--session-title] [--session-description] [--default-timezone]`
    * `itomorilog browse --session <path-to-ItomoriLog.duckdb>`
*   Command palette is implemented and keyboard/search-as-you-type enabled.

Known limits (still intentionally minimal):

*   CLI `browse` currently prints session summary/sample rows to console; it does not open the desktop UI.
*   Drag-and-drop currently uses Avalonia-obsolete `e.Data` API path (functional, with warnings) pending API migration.

***

## Where do rules live? (carry‑over without dogma)

*   **Per‑session rules** (what actually applied) remain inside the **session DB** (e.g., a `rules` table).
*   **Global “Format Knowledge Base”** for suggestions across sessions lives in **app data** (e.g., `~/.itomorilog/fkb.duckdb` or a JSON). It only **suggests** on New Session; does **not** auto‑apply.
*   When the user accepts a suggestion, it becomes **session‑local** (and editable), keeping sessions self‑contained.

***

## Code: create/open session helpers (C#)

```csharp
public sealed class SessionPaths
{
    public static (string folder, string dbPath) CreateNew(string? baseDir, string? explicitName = null)
    {
        baseDir ??= GetDefaultSessionsDir(); // e.g., ~/ItomoriLog/Sessions
        Directory.CreateDirectory(baseDir);

        var name = explicitName ?? $"{DateTime.Now:yyyy-MM-dd_HH-mm-ss}_ItomoriLog";
        var folder = Path.Combine(baseDir, name);
        Directory.CreateDirectory(folder);

        var dbPath = Path.Combine(folder, "ItomoriLog.duckdb");
        return (folder, dbPath);
    }
}

public sealed class SessionStore
{
    private readonly Func<IDbConnection> _connFactory;

    public SessionStore(Func<IDbConnection> connFactory) => _connFactory = connFactory;

    public async Task InitializeAsync(SessionInit init, CancellationToken ct = default)
    {
        await using var conn = _connFactory(); await conn.OpenAsync(ct);
        await Exec(conn, """
            CREATE TABLE IF NOT EXISTS session (
              session_id VARCHAR PRIMARY KEY,
              created_utc TIMESTAMP NOT NULL,
              modified_utc TIMESTAMP NOT NULL,
              title VARCHAR NOT NULL,
              description VARCHAR,
              created_by VARCHAR,
              default_timezone VARCHAR,
              app_version VARCHAR
            );
            """, ct);

        await Exec(conn, """
            INSERT INTO session(session_id, created_utc, modified_utc, title, description, created_by, default_timezone, app_version)
            VALUES (?, now(), now(), ?, ?, ?, ?, ?);
            """, ct, init.SessionId, init.Title, init.Description, init.CreatedBy, init.DefaultTimeZone ?? "Local", init.AppVersion);
    }

    public async Task<(string Title,string? Description,string? DefaultTz)> ReadHeaderAsync(CancellationToken ct = default)
    {
        await using var conn = _connFactory(); await conn.OpenAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT title, description, default_timezone FROM session LIMIT 1;";
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        if (await rdr.ReadAsync(ct))
            return (rdr.GetString(0), rdr.IsDBNull(1) ? null : rdr.GetString(1), rdr.IsDBNull(2) ? null : rdr.GetString(2));
        return ("(untitled session)", null, null);
    }

    public async Task UpdateHeaderAsync(string title, string? description, CancellationToken ct = default)
    {
        await using var conn = _connFactory(); await conn.OpenAsync(ct);
        await Exec(conn, "UPDATE session SET title=?, description=?, modified_utc=now();", ct, title, description ?? (object)DBNull.Value);
    }

    private static async Task Exec(IDbConnection conn, string sql, CancellationToken ct, params object[] args)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var a in args)
        {
            var p = cmd.CreateParameter(); p.Value = a; cmd.Parameters.Add(p);
        }
        await cmd.ExecuteNonQueryAsync(ct);
    }
}

public sealed record SessionInit(string SessionId, string Title, string? Description, string? CreatedBy, string? DefaultTimeZone, string AppVersion);
```

***

## Welcome screen logic (pseudo)

1.  **New Session**
    *   User selects files/dirs/zips → preview shows estimated record count.
    *   Title defaults to `ItomoriLog Session — {now}`; Description defaults from selections.
    *   On **Create**, call `SessionPaths.CreateNew(...)`, then `SessionStore.InitializeAsync(...)`.
    *   Kick off ingestion with the chosen defaults; the session is available immediately.

2.  **Recent Sessions grid**
    *   Backed by a simple model: `ObservableCollection<RecentSessionItem>`.
    *   On first render, show cached title/path; in background, open `ItomoriLog.duckdb` and `SELECT title, modified_utc` to refresh.

***

## Why this works well

*   **Portability**: Each session is a single folder (often a single file) you can zip, ship, or archive.
*   **Isolation**: No chance of one session affecting another; edits to rules/timezone defaults are local.
*   **Predictable paths**: Backups are easy; outside tools can attach to the DB file if needed.
*   **Editable identity**: Session title/description are stored **with the data**; the UI always shows accurate, user‑friendly labels.

***

## What changes from the earlier branding copy

*   Anywhere we mentioned a **“central DB path”**, replace it with:  
    **“Pick or create a session folder. Each session stores its own ItomoriLog.duckdb file (DuckLake/DuckDB).”**
*   The “Recent” area and CLI help now point to **session paths**, not a global catalog.

***

## Optional (later): “Collections” / Workspaces

If one day you want a **Workspace** that holds *multiple* sessions:

*   Keep **one “workspace” DuckDB file** with an index of sessions (title, paths, tags), but **do not store the logs** in it.
*   This gives you cross‑session search/indexing if you decide to build that—without breaking the “session = self‑contained DB” principle.

***

If you want, I can:

*   Patch the **PRD/Spec** we wrote to reflect *session‑per‑DB* explicitly.
*   Update the **Welcome screen XAML** and the **Session header** controls with inline editing.
*   Add the **CLI flags** for `--session-title/--session-description` and wire them to the `session` table.

Do you want the **title/description** to also appear in **exports** (e.g., as Parquet file metadata and export folder README)? I can include that too.
