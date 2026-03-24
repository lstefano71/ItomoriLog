namespace ItomoriLog.Core.Model;

public sealed record IngestRunRow(
    string RunId,
    DateTimeOffset StartedUtc,
    DateTimeOffset? CompletedUtc,
    string Status);
