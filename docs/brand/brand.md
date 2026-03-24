**ItomoriLog** — love it. It’s evocative, memorable, and (for those who know) a subtle nod to timelines crossing and being “tied together.” Perfect fit for a time‑centric log explorer.

Below is a quick **brand kit + rollout checklist** and a few **ready‑to‑use snippets** so you can propagate the name across code, UI, docs, and storage in one pass.

***

## 1) Brand micro‑story & taglines

*   **Micro‑story (internal):** *“ItomoriLog brings scattered events into one braided timeline—clean, fast, and local-first.”*
*   **Tagline options:**
    *   *“Braiding time from your logs.”*
    *   *“Your logs, one timeline.”*
    *   *“Fast local insights across all your logs.”*
    *   *“Search time. Find truth.”*

***

## 2) Brand kit (lightweight)

**Colors (accessible, dark‑theme friendly)**

*   **Primary:** Sakura Pink `#FF6DAE` (accent), **Neon Cyan** `#0EA5E9` (links/active)
*   **Base UI:** Onyx `#111418` (bg), Slate `#1F2430` (panels), Mist `#D6E2F0` (text)
*   **Severity chips:** Info `#38BDF8`, Warn `#F59E0B`, Error `#EF4444`, Debug `#9CA3AF`

**Typography**

*   UI: **Inter** (or **Segoe UI / San Francisco / Roboto** depending on OS)
*   Mono (details/raw): **JetBrains Mono** or **Cascadia Code**

**Icon/wordmark concept (for your designer)**

*   A **braided comet tail** forming a check‑mark shape over a **tick mark** (timeline).
*   24/48/96 px grid, 2px stroke, rounded joins; ensure it reads at 16px.
*   App icon backgrounds: solid Onyx; glyph in Sakura Pink; secondary stroke Neon Cyan.

*(If you want, I can generate an SVG set and a PNG export kit—just say the word.)*

***

## 3) Namespaces, binaries, packages

**Repo**

*   `github.com/<org>/ItomoriLog`

**Assemblies / namespaces**

    ItomoriLog.App            // Avalonia shell
    ItomoriLog.UI             // controls, view models
    ItomoriLog.Ingest         // detectors, readers, timestamp extractors
    ItomoriLog.DuckLake       // storage (DuckDB/DuckLake integration)
    ItomoriLog.Query          // planner, DSL, TICK rewriter
    ItomoriLog.Model          // records, contracts
    ItomoriLog.Tests          // unit + property tests

***

## 5) UI touchpoints

### About dialog (microcopy)

> **ItomoriLog**  
> Braiding time from your logs.  
> v0.1.0 • Local‑first • DuckLake/DuckDB inside

### Splash / loading (Avalonia XAML sample)

```xml
<UserControl xmlns="https://github.com/avaloniaui"
             xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml"
             x:Class="ItomoriLog.UI.Views.SplashView"
             Background="#111418">
  <Grid>
    <StackPanel HorizontalAlignment="Center" VerticalAlignment="Center" Spacing="12">
      <!-- Replace with your vector logo when ready -->
      <TextBlock Text="ItomoriLog" Foreground="#FF6DAE" FontSize="36" FontWeight="Bold" />
      <TextBlock Text="Braiding time from your logs" Foreground="#D6E2F0" Opacity="0.85"
                 HorizontalAlignment="Center"/>
      <ProgressBar IsIndeterminate="True" Width="320"/>
      <TextBlock Text="{Binding StatusText}" Foreground="#9CA3AF" FontSize="12"
                 HorizontalAlignment="Center"/>
    </StackPanel>
  </Grid>
</UserControl>
```

***

## 6) Constants & CLI help (drop‑in)

```csharp
public static class Brand
{
    public const string AppName = "ItomoriLog";
    public const string Tagline = "Braiding time from your logs";
    public const string DefaultDbFile = "ItomoriLog.duckdb";
}

public const string CliHelp = """
ItomoriLog — braiding time from your logs

USAGE
  itomori ingest [path|zip]...     Ingest files/dirs/zips into the local store
  itomori browse                   Launch the UI (Avalonia)
  itomori query "<expr>"           Run a query (supports timestamp IN 'TICK')

EXAMPLES
  itomori ingest ./logs ./archive.zip
  itomori query "level:Error timestamp IN '$now-1h..$now'"

OPTIONS
  --db <path>                      Path to ItomoriLog.duckdb (default: cwd)
  --tz <IANA|Local|UTC|+hh:mm>     Default timezone for bare timestamps
  --help                           Show this help
""";
```

***

## 7) Internal codenames (optional, fun)

*   **Kumihimo** (組紐) for the TICK compiler (the “braid” that ties intervals together).
*   **Mitsuha/Taki** for the UI “pair” of panels (timeline ↔ grid) if you want subtle file/class names.

***

## 8) Docs replacement patch (quick sweep)

*   Replace “project” with **ItomoriLog** in:
    *   **PRD** title: *“ItomoriLog — Personal Portable Log Ingester/Browser”*
    *   **Spec** and **Deep Dives** (module names above)
    *   Examples, screenshots, code samples (binaries, env vars)

**README header template**

```md
# ItomoriLog

> Braiding time from your logs — fast, local-first log ingestion & browsing with a Seq/OpenObserve-class UX.
>
> - AOT-ready Avalonia UI
> - DuckLake/DuckDB storage (ACID, Parquet)
> - TICK-style period expressions: `timestamp IN '...'`
> - Re-ingest only what changed (per source/segment)
```

***

## 9) Release & ops

*   **Versioning:** SemVer (`0.1.0` MVP). Reserve `0.2.x` for TICK day filters + live tail.
*   **Issue labels:** `ui`, `ingest`, `tick`, `ducklake`, `perf`, `reingest`, `docs`, `good‑first‑issue`.
*   **Binary names:** Windows MSI: `ItomoriLog-Setup-<ver>.msi`; macOS `.dmg`; Linux `.AppImage` or `.deb`.

***

## 10) Checklists

**Repo bootstrap**

*   [ ] Rename solution, namespaces, assembly names → `ItomoriLog.*`
*   [ ] Add `Brand` constants and splash
*   [ ] Update PRD/Spec/Deep Dives headings and examples
*   [ ] Add README + icon placeholders
*   [ ] Set default DB to `ItomoriLog.duckdb`

**UX polish (MVP)**

*   [ ] Title bar, About dialog, and tray menu show **ItomoriLog**
*   [ ] Session picker displays name + version
*   [ ] Export filenames prefixed with `itomori-`

