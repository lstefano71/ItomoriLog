using System.Collections.Concurrent;
using DuckDB.NET.Data;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Storage;

namespace ItomoriLog.Core.Query;

/// <summary>
/// Executes keyset-paginated queries against DuckDB and caches pages via LRU.
/// </summary>
public sealed class RowPager
{
    private readonly DuckLakeConnectionFactory _factory;
    private readonly QueryPlanner _planner;
    private readonly int _pageSize;
    private readonly LruPageCache _cache;

    public RowPager(
        DuckLakeConnectionFactory factory,
        QueryPlanner planner,
        int pageSize = 2000,
        int cacheCapacity = 10)
    {
        _factory = factory;
        _planner = planner;
        _pageSize = pageSize;
        _cache = new LruPageCache(cacheCapacity);
    }

    public int PageSize => _pageSize;

    /// <summary>
    /// Fetch a page of log rows starting after the given cursor.
    /// Returns rows in ascending order regardless of direction.
    /// </summary>
    public async Task<PageResult> FetchPageAsync(
        FilterState filter,
        PageCursor? cursor = null,
        PageDirection direction = PageDirection.Forward,
        TickContext? tickContext = null,
        CancellationToken ct = default)
    {
        var cacheKey = BuildCacheKey(filter, cursor, direction);
        if (_cache.TryGet(cacheKey, out var cached))
            return cached;

        var query = _planner.Plan(filter, cursor, direction, _pageSize, tickContext);
        var conn = await _factory.GetConnectionAsync(ct);

        // Execute setup SQL (e.g., temp table for TICK) if present
        if (query.SetupSql is not null)
        {
            using var setupCmd = conn.CreateCommand();
            setupCmd.CommandText = query.SetupSql;
            await setupCmd.ExecuteNonQueryAsync(ct);
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = query.Sql;
        foreach (var p in query.Parameters)
            cmd.Parameters.Add(new DuckDBParameter { Value = p });

        var rows = new List<LogRow>();
        using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            rows.Add(new LogRow(
                TimestampUtc: new DateTimeOffset(reader.GetDateTime(0), TimeSpan.Zero),
                TimestampBasis: Enum.Parse<TimeBasis>(reader.GetString(1)),
                TimestampEffectiveOffsetMinutes: reader.GetInt32(2),
                TimestampOriginal: reader.IsDBNull(3) ? null : reader.GetString(3),
                LogicalSourceId: reader.GetString(4),
                SourcePath: reader.GetString(5),
                PhysicalFileId: reader.GetString(6),
                SegmentId: reader.GetString(7),
                IngestRunId: reader.GetString(8),
                RecordIndex: reader.GetInt64(9),
                Level: reader.IsDBNull(10) ? null : reader.GetString(10),
                Message: reader.GetString(11),
                FieldsJson: reader.IsDBNull(12) ? null : reader.GetString(12)));
        }

        // Backward queries come in DESC order — reverse to natural ASC order
        if (direction == PageDirection.Backward)
            rows.Reverse();

        var result = new PageResult(rows, BuildCursors(rows));
        _cache.Put(cacheKey, result);

        return result;
    }

    /// <summary>Prefetches the next page in the background.</summary>
    public void PrefetchNext(
        FilterState filter,
        PageCursor cursor,
        TickContext? tickContext = null)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                await FetchPageAsync(filter, cursor, PageDirection.Forward, tickContext);
            }
            catch
            {
                // Prefetch is best-effort
            }
        });
    }

    public void ClearCache() => _cache.Clear();

    private static PageCursors BuildCursors(List<LogRow> rows)
    {
        if (rows.Count == 0)
            return new PageCursors(null, null);

        var first = rows[0];
        var last = rows[^1];
        return new PageCursors(
            First: new PageCursor(first.TimestampUtc, first.SegmentId, first.RecordIndex),
            Last: new PageCursor(last.TimestampUtc, last.SegmentId, last.RecordIndex));
    }

    private static string BuildCacheKey(FilterState filter, PageCursor? cursor, PageDirection direction)
    {
        return $"{filter.GetHashCode()}|{cursor?.GetHashCode()}|{direction}";
    }
}

public sealed record PageResult(
    IReadOnlyList<LogRow> Rows,
    PageCursors Cursors);

public sealed record PageCursors(
    PageCursor? First,
    PageCursor? Last);

/// <summary>Simple LRU cache for page results.</summary>
internal sealed class LruPageCache
{
    private readonly int _capacity;
    private readonly ConcurrentDictionary<string, LinkedListNode<(string Key, PageResult Value)>> _map = new();
    private readonly LinkedList<(string Key, PageResult Value)> _list = new();
    private readonly object _lock = new();

    public LruPageCache(int capacity)
    {
        _capacity = capacity;
    }

    public bool TryGet(string key, out PageResult result)
    {
        if (_map.TryGetValue(key, out var node))
        {
            lock (_lock)
            {
                _list.Remove(node);
                _list.AddFirst(node);
            }
            result = node.Value.Value;
            return true;
        }
        result = default!;
        return false;
    }

    public void Put(string key, PageResult value)
    {
        lock (_lock)
        {
            if (_map.TryGetValue(key, out var existing))
            {
                _list.Remove(existing);
                _map.TryRemove(key, out _);
            }

            var node = _list.AddFirst((key, value));
            _map[key] = node;

            while (_list.Count > _capacity)
            {
                var last = _list.Last!;
                _list.RemoveLast();
                _map.TryRemove(last.Value.Key, out _);
            }
        }
    }

    public void Clear()
    {
        lock (_lock)
        {
            _list.Clear();
            _map.Clear();
        }
    }
}
