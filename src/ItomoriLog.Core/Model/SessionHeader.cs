namespace ItomoriLog.Core.Model;

public sealed record SessionHeader(
    string SessionId,
    DateTimeOffset CreatedUtc,
    DateTimeOffset ModifiedUtc,
    string Title,
    string? Description,
    string? CreatedBy,
    string? DefaultTimezone,
    string? AppVersion);
