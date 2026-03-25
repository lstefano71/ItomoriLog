using DuckDB.NET.Data;

namespace ItomoriLog.Core.Storage;

public static class DuckLakeSessionMaintenance
{
    public const string PreferredParquetVersion = "2";
    public const string PreferredParquetCompression = "zstd";

    public static async Task ApplyPreferredOptionsAsync(
        DuckDBConnection connection,
        string catalogAlias,
        CancellationToken ct = default)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"""
            CALL {catalogAlias}.set_option('parquet_version', '{PreferredParquetVersion}');
            CALL {catalogAlias}.set_option('parquet_compression', '{PreferredParquetCompression}');
            """;
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
