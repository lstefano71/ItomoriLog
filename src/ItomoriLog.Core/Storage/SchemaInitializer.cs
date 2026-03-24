using DuckDB.NET.Data;

namespace ItomoriLog.Core.Storage;

public static class SchemaInitializer
{
    public static async Task EnsureSchemaAsync(DuckDBConnection connection, CancellationToken ct = default)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = Ddl;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private const string Ddl = """
        CREATE TABLE IF NOT EXISTS session (
            session_id       VARCHAR PRIMARY KEY,
            created_utc      TIMESTAMP NOT NULL,
            modified_utc     TIMESTAMP NOT NULL,
            title            VARCHAR NOT NULL,
            description      VARCHAR,
            created_by       VARCHAR,
            default_timezone VARCHAR,
            app_version      VARCHAR
        );

        CREATE TABLE IF NOT EXISTS logs (
            timestamp_utc                      TIMESTAMP NOT NULL,
            timestamp_basis                    VARCHAR NOT NULL,
            timestamp_effective_offset_minutes INTEGER NOT NULL,
            timestamp_original                 VARCHAR,
            logical_source_id                  VARCHAR NOT NULL,
            source_path                        VARCHAR NOT NULL,
            physical_file_id                   VARCHAR NOT NULL,
            segment_id                         VARCHAR NOT NULL,
            ingest_run_id                      VARCHAR NOT NULL,
            record_index                       BIGINT NOT NULL,
            level                              VARCHAR,
            message                            VARCHAR NOT NULL,
            fields                             JSON
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_logs_keyset
            ON logs (timestamp_utc, segment_id, record_index);

        CREATE INDEX IF NOT EXISTS idx_logs_segment
            ON logs (segment_id);

        CREATE TABLE IF NOT EXISTS segments (
            segment_id         VARCHAR PRIMARY KEY,
            logical_source_id  VARCHAR NOT NULL,
            physical_file_id   VARCHAR NOT NULL,
            min_ts_utc         TIMESTAMP,
            max_ts_utc         TIMESTAMP,
            row_count          BIGINT NOT NULL DEFAULT 0,
            last_ingest_run_id VARCHAR NOT NULL,
            active             BOOLEAN NOT NULL DEFAULT TRUE,
            last_byte_offset   BIGINT,
            source_path        VARCHAR,
            file_size_bytes    BIGINT,
            last_modified_utc  TIMESTAMP,
            file_hash          VARCHAR
        );

        CREATE TABLE IF NOT EXISTS skips (
            session_id          VARCHAR,
            logical_source_id   VARCHAR NOT NULL,
            physical_file_id    VARCHAR NOT NULL,
            segment_id          VARCHAR NOT NULL,
            segment_seq         BIGINT NOT NULL,
            start_line          BIGINT,
            end_line            BIGINT,
            start_offset        BIGINT,
            end_offset          BIGINT,
            reason_code         VARCHAR NOT NULL,
            reason_detail       VARCHAR,
            sample_prefix       BLOB,
            detector_profile_id VARCHAR,
            utc_logged_at       TIMESTAMP NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_skips_segment
            ON skips (segment_id);

        CREATE TABLE IF NOT EXISTS rules (
            rule_id    VARCHAR PRIMARY KEY,
            segment_id VARCHAR,
            rule_type  VARCHAR NOT NULL,
            config     JSON NOT NULL,
            created_utc TIMESTAMP NOT NULL,
            source     VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ingest_runs (
            run_id        VARCHAR PRIMARY KEY,
            started_utc   TIMESTAMP NOT NULL,
            completed_utc TIMESTAMP,
            status        VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS query_history (
            id           INTEGER PRIMARY KEY,
            query_text   VARCHAR NOT NULL,
            executed_utc TIMESTAMP NOT NULL,
            result_count BIGINT
        );
        CREATE SEQUENCE IF NOT EXISTS seq_session_qh START 1;
        """;
}
