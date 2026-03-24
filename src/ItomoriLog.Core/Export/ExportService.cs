using System.Globalization;
using System.Text;
using System.Text.Json;
using DuckDB.NET.Data;
using ItomoriLog.Core.Model;
using ItomoriLog.Core.Query;

namespace ItomoriLog.Core.Export;

public enum ExportFormat
{
    Csv,
    JsonLines,
    Parquet
}

public sealed record ExportProgress(long RowsWritten, long TotalEstimate, string Status);

public enum ExportScope
{
    CurrentView,
    FullSession
}

public sealed record ExportOptions(
    ExportFormat Format,
    string OutputPath,
    FilterState? Filter = null,
    ExportScope Scope = ExportScope.CurrentView,
    string? SessionTitle = null,
    string? SessionDescription = null,
    string? SessionFolder = null);

public sealed class ExportService
{
    private readonly DuckDBConnection _connection;
    private readonly QueryPlanner _planner;

    public ExportService(DuckDBConnection connection, QueryPlanner? planner = null)
    {
        _connection = connection;
        _planner = planner ?? new QueryPlanner();
    }

    public async Task<long> ExportAsync(
        ExportOptions options,
        IProgress<ExportProgress>? progress = null,
        CancellationToken ct = default)
    {
        return options.Format switch
        {
            ExportFormat.Csv => await ExportCsvAsync(options, progress, ct),
            ExportFormat.JsonLines => await ExportJsonLinesAsync(options, progress, ct),
            ExportFormat.Parquet => await ExportParquetAsync(options, progress, ct),
            _ => throw new ArgumentOutOfRangeException(nameof(options))
        };
    }

    private async Task<long> ExportCsvAsync(
        ExportOptions options, IProgress<ExportProgress>? progress, CancellationToken ct)
    {
        var effectiveFilter = ResolveExportFilter(options);
        var totalEstimate = await EstimateCountAsync(effectiveFilter, ct);
        progress?.Report(new ExportProgress(0, totalEstimate, "Starting CSV export..."));

        var (whereSql, parameters) = BuildWhereClause(effectiveFilter);
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"""
            SELECT timestamp_utc, timestamp_basis, timestamp_effective_offset_minutes,
                   timestamp_original, logical_source_id, source_path,
                   physical_file_id, segment_id, ingest_run_id,
                   record_index, level, message, fields
            FROM logs{whereSql}
            ORDER BY timestamp_utc ASC, segment_id ASC, record_index ASC
            """;
        foreach (var p in parameters)
            cmd.Parameters.Add(p);

        using var reader = await cmd.ExecuteReaderAsync(ct);
        await using var writer = new StreamWriter(options.OutputPath, false, new UTF8Encoding(false));

        // RFC 4180 header
        await writer.WriteLineAsync("timestamp_utc,timestamp_basis,timestamp_effective_offset_minutes,timestamp_original,logical_source_id,source_path,physical_file_id,segment_id,ingest_run_id,record_index,level,message,fields");

        long rowsWritten = 0;
        while (await reader.ReadAsync(ct))
        {
            ct.ThrowIfCancellationRequested();
            var sb = new StringBuilder();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (i > 0) sb.Append(',');
                if (reader.IsDBNull(i))
                {
                    // empty field
                }
                else
                {
                    var val = reader.GetValue(i);
                    var str = val is DateTime dt
                        ? dt.ToString("O", CultureInfo.InvariantCulture)
                        : Convert.ToString(val, CultureInfo.InvariantCulture) ?? "";
                    sb.Append(CsvEscape(str));
                }
            }
            await writer.WriteLineAsync(sb.ToString());
            rowsWritten++;

            if (rowsWritten % 10_000 == 0)
                progress?.Report(new ExportProgress(rowsWritten, totalEstimate, $"Exported {rowsWritten:N0} rows..."));
        }

        await WriteCsvMetadataAsync(options, rowsWritten, ct);
        progress?.Report(new ExportProgress(rowsWritten, rowsWritten, "CSV export complete"));
        return rowsWritten;
    }

    private async Task<long> ExportJsonLinesAsync(
        ExportOptions options, IProgress<ExportProgress>? progress, CancellationToken ct)
    {
        var effectiveFilter = ResolveExportFilter(options);
        var totalEstimate = await EstimateCountAsync(effectiveFilter, ct);
        progress?.Report(new ExportProgress(0, totalEstimate, "Starting JSON Lines export..."));

        var (whereSql, parameters) = BuildWhereClause(effectiveFilter);
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"""
            SELECT timestamp_utc, timestamp_basis, timestamp_effective_offset_minutes,
                   timestamp_original, logical_source_id, source_path,
                   physical_file_id, segment_id, ingest_run_id,
                   record_index, level, message, fields
            FROM logs{whereSql}
            ORDER BY timestamp_utc ASC, segment_id ASC, record_index ASC
            """;
        foreach (var p in parameters)
            cmd.Parameters.Add(p);

        using var reader = await cmd.ExecuteReaderAsync(ct);
        await using var writer = new StreamWriter(options.OutputPath, false, new UTF8Encoding(false));

        var jsonOptions = new JsonWriterOptions { Indented = false };
        long rowsWritten = 0;

        while (await reader.ReadAsync(ct))
        {
            ct.ThrowIfCancellationRequested();
            using var ms = new MemoryStream();
            using (var jsonWriter = new Utf8JsonWriter(ms, jsonOptions))
            {
                jsonWriter.WriteStartObject();
                WriteJsonField(jsonWriter, reader, 0, "timestamp_utc");
                WriteJsonField(jsonWriter, reader, 1, "timestamp_basis");
                WriteJsonField(jsonWriter, reader, 2, "timestamp_effective_offset_minutes");
                WriteJsonField(jsonWriter, reader, 3, "timestamp_original");
                WriteJsonField(jsonWriter, reader, 4, "logical_source_id");
                WriteJsonField(jsonWriter, reader, 5, "source_path");
                WriteJsonField(jsonWriter, reader, 6, "physical_file_id");
                WriteJsonField(jsonWriter, reader, 7, "segment_id");
                WriteJsonField(jsonWriter, reader, 8, "ingest_run_id");
                WriteJsonField(jsonWriter, reader, 9, "record_index");
                WriteJsonField(jsonWriter, reader, 10, "level");
                WriteJsonField(jsonWriter, reader, 11, "message");
                WriteJsonField(jsonWriter, reader, 12, "fields");
                jsonWriter.WriteEndObject();
            }
            var line = Encoding.UTF8.GetString(ms.ToArray());
            await writer.WriteLineAsync(line);
            rowsWritten++;

            if (rowsWritten % 10_000 == 0)
                progress?.Report(new ExportProgress(rowsWritten, totalEstimate, $"Exported {rowsWritten:N0} rows..."));
        }

        progress?.Report(new ExportProgress(rowsWritten, rowsWritten, "JSON Lines export complete"));
        return rowsWritten;
    }

    private async Task<long> ExportParquetAsync(
        ExportOptions options, IProgress<ExportProgress>? progress, CancellationToken ct)
    {
        var effectiveFilter = ResolveExportFilter(options);
        var totalEstimate = await EstimateCountAsync(effectiveFilter, ct);
        progress?.Report(new ExportProgress(0, totalEstimate, "Starting Parquet export..."));

        var (whereSql, parameters) = BuildWhereClause(effectiveFilter);

        // Escape single quotes in path for SQL
        var escapedPath = options.OutputPath.Replace("'", "''").Replace("\\", "/");

        using var cmd = _connection.CreateCommand();
        var metadataSql = BuildParquetMetadataSql(options);
        cmd.CommandText = $"""
            COPY (
                SELECT timestamp_utc, timestamp_basis, timestamp_effective_offset_minutes,
                       timestamp_original, logical_source_id, source_path,
                       physical_file_id, segment_id, ingest_run_id,
                       record_index, level, message, fields
                FROM logs{whereSql}
                ORDER BY timestamp_utc ASC, segment_id ASC, record_index ASC
            ) TO '{escapedPath}' ({metadataSql})
            """;
        foreach (var p in parameters)
            cmd.Parameters.Add(p);

        await cmd.ExecuteNonQueryAsync(ct);

        // Count the exported rows
        using var countCmd = _connection.CreateCommand();
        countCmd.CommandText = $"SELECT COUNT(*) FROM logs{whereSql}";
        foreach (var p in parameters)
            countCmd.Parameters.Add(new DuckDBParameter { Value = p.Value });

        var count = Convert.ToInt64(await countCmd.ExecuteScalarAsync(ct));
        progress?.Report(new ExportProgress(count, count, "Parquet export complete"));
        return count;
    }

    private static FilterState? ResolveExportFilter(ExportOptions options) =>
        options.Scope == ExportScope.FullSession ? null : options.Filter;

    internal static string BuildCsvMetadataPath(string outputPath) => GetCsvMetadataPath(outputPath);

    private async Task<long> EstimateCountAsync(FilterState? filter, CancellationToken ct)
    {
        var (whereSql, parameters) = BuildWhereClause(filter);
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM logs{whereSql}";
        foreach (var p in parameters)
            cmd.Parameters.Add(p);

        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result);
    }

    internal (string WhereSql, List<DuckDBParameter> Parameters) BuildWhereClause(FilterState? filter)
    {
        if (filter is null || filter == FilterState.Empty)
            return ("", []);

        var clauses = new List<string>();
        var parameters = new List<DuckDBParameter>();

        if (filter.StartUtc.HasValue)
        {
            parameters.Add(new DuckDBParameter { Value = filter.StartUtc.Value.UtcDateTime });
            clauses.Add($"timestamp_utc >= ${parameters.Count}");
        }

        if (filter.EndUtc.HasValue)
        {
            parameters.Add(new DuckDBParameter { Value = filter.EndUtc.Value.UtcDateTime });
            clauses.Add($"timestamp_utc < ${parameters.Count}");
        }

        if (filter.SourceIds is { Count: > 0 })
        {
            var placeholders = new List<string>();
            foreach (var sourceId in filter.SourceIds)
            {
                parameters.Add(new DuckDBParameter { Value = sourceId });
                placeholders.Add($"${parameters.Count}");
            }
            clauses.Add($"logical_source_id IN ({string.Join(", ", placeholders)})");
        }
        if (filter.ExcludedSourceIds is { Count: > 0 })
        {
            var placeholders = new List<string>();
            foreach (var sourceId in filter.ExcludedSourceIds)
            {
                parameters.Add(new DuckDBParameter { Value = sourceId });
                placeholders.Add($"${parameters.Count}");
            }
            clauses.Add($"logical_source_id NOT IN ({string.Join(", ", placeholders)})");
        }

        if (filter.Levels is { Count: > 0 })
        {
            var placeholders = new List<string>();
            foreach (var level in filter.Levels)
            {
                parameters.Add(new DuckDBParameter { Value = level });
                placeholders.Add($"${parameters.Count}");
            }
            clauses.Add($"level IN ({string.Join(", ", placeholders)})");
        }
        if (filter.ExcludedLevels is { Count: > 0 })
        {
            var placeholders = new List<string>();
            foreach (var level in filter.ExcludedLevels)
            {
                parameters.Add(new DuckDBParameter { Value = level });
                placeholders.Add($"${parameters.Count}");
            }
            clauses.Add($"(level IS NULL OR level NOT IN ({string.Join(", ", placeholders)}))");
        }

        if (!string.IsNullOrWhiteSpace(filter.TextSearch))
        {
            parameters.Add(new DuckDBParameter { Value = $"%{filter.TextSearch}%" });
            clauses.Add($"message ILIKE ${parameters.Count}");
        }

        if (clauses.Count == 0)
            return ("", []);

        return ($" WHERE {string.Join(" AND ", clauses)}", parameters);
    }

    internal static string CsvEscape(string value)
    {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
            return $"\"{value.Replace("\"", "\"\"")}\"";
        return value;
    }

    private static void WriteJsonField(Utf8JsonWriter writer, System.Data.Common.DbDataReader reader, int ordinal, string name)
    {
        if (reader.IsDBNull(ordinal))
        {
            writer.WriteNull(name);
            return;
        }

        var val = reader.GetValue(ordinal);
        switch (val)
        {
            case DateTime dt:
                writer.WriteString(name, dt.ToString("O", CultureInfo.InvariantCulture));
                break;
            case long l:
                writer.WriteNumber(name, l);
                break;
            case int i:
                writer.WriteNumber(name, i);
                break;
            case double d:
                writer.WriteNumber(name, d);
                break;
            case string s when name == "fields":
                // Embed as raw JSON if it's valid JSON, otherwise as string
                try
                {
                    using var doc = JsonDocument.Parse(s);
                    writer.WritePropertyName(name);
                    doc.RootElement.WriteTo(writer);
                }
                catch
                {
                    writer.WriteString(name, s);
                }
                break;
            default:
                writer.WriteString(name, Convert.ToString(val, CultureInfo.InvariantCulture));
                break;
        }
    }

    private static string BuildParquetMetadataSql(ExportOptions options)
    {
        var parts = new List<string> { "FORMAT PARQUET" };
        var metadataEntries = new List<string>
        {
            $"export_scope: '{EscapeSqlString(options.Scope.ToString())}'",
            $"export_format: '{EscapeSqlString(options.Format.ToString())}'"
        };

        if (!string.IsNullOrWhiteSpace(options.SessionTitle))
            metadataEntries.Add($"session_title: '{EscapeSqlString(options.SessionTitle)}'");
        if (!string.IsNullOrWhiteSpace(options.SessionDescription))
            metadataEntries.Add($"session_description: '{EscapeSqlString(options.SessionDescription)}'");
        if (!string.IsNullOrWhiteSpace(options.SessionFolder))
            metadataEntries.Add($"session_folder: '{EscapeSqlString(options.SessionFolder)}'");

        metadataEntries.Add($"exported_utc: '{EscapeSqlString(DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture))}'");

        parts.Add($"KV_METADATA {{ {string.Join(", ", metadataEntries)} }}");
        return string.Join(", ", parts);
    }

    private static async Task WriteCsvMetadataAsync(ExportOptions options, long rowsWritten, CancellationToken ct)
    {
        var metadataPath = GetCsvMetadataPath(options.OutputPath);
        var metadata = new
        {
            format = options.Format.ToString(),
            scope = options.Scope.ToString(),
            exported_utc = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture),
            rows_written = rowsWritten,
            session_title = options.SessionTitle,
            session_description = options.SessionDescription,
            session_folder = options.SessionFolder
        };

        await using var fs = File.Create(metadataPath);
        await JsonSerializer.SerializeAsync(fs, metadata, cancellationToken: ct);
    }

    private static string GetCsvMetadataPath(string outputPath)
    {
        var dir = Path.GetDirectoryName(outputPath);
        var stem = Path.GetFileNameWithoutExtension(outputPath);
        return Path.Combine(dir ?? "", $"{stem}_metadata.json");
    }

    private static string EscapeSqlString(string value) => value.Replace("'", "''");
}
