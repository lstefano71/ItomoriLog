using System.Text.Json;
using System.Text.RegularExpressions;

namespace ItomoriLog.Core.Ingest;

public sealed class FieldSynthesizer
{
    private readonly Regex? _fieldRegex;

    public static readonly HashSet<string> WellKnownFields = ["level", "subsource", "task_id", "username", "message"];

    public static IReadOnlyList<Regex> CommonPatterns { get; } =
    [
        // Level + Source + TaskId + Message: "INFO [MyApp.Core] [task-123] Starting"
        new Regex(@"^\s*(?<level>\w+)\s+\[(?<subsource>[^\]]+)\]\s*\[(?<task_id>[^\]]+)\]\s*(?<message>.+)$", RegexOptions.Compiled),
        // Level + Source + Message: "INFO [MyApp.Core] Starting"
        new Regex(@"^\s*(?<level>\w+)\s+\[(?<subsource>[^\]]+)\]\s*(?<message>.+)$", RegexOptions.Compiled),
        // Level + Message: "INFO Starting application"
        new Regex(@"^\s*(?<level>\w+)\s+(?<message>.+)$", RegexOptions.Compiled),
    ];

    public FieldSynthesizer(Regex? fieldRegex = null)
    {
        _fieldRegex = fieldRegex;
    }

    public FieldExtractionResult Extract(string postTimestampText)
    {
        var fields = new Dictionary<string, string>();
        string? level = null;
        string? message = null;

        var regex = _fieldRegex;
        if (regex is null) {
            foreach (var pattern in CommonPatterns) {
                var m = pattern.Match(postTimestampText);
                if (m.Success) {
                    regex = pattern;
                    break;
                }
            }
        }

        if (regex is not null) {
            var match = regex.Match(postTimestampText);
            if (match.Success) {
                foreach (var groupName in regex.GetGroupNames()) {
                    if (groupName == "0") continue;
                    var group = match.Groups[groupName];
                    if (!group.Success) continue;

                    var value = group.Value.Trim();
                    if (string.IsNullOrEmpty(value)) continue;

                    switch (groupName) {
                        case "level":
                            level = NormalizeLevel(value);
                            break;
                        case "message":
                            message = value;
                            break;
                        default:
                            fields[groupName] = value;
                            break;
                    }
                }
            }
        }

        message ??= postTimestampText.Trim();

        string? fieldsJson = fields.Count > 0
            ? JsonSerializer.Serialize(fields)
            : null;

        return new FieldExtractionResult(level, message, fieldsJson);
    }

    public static string NormalizeLevel(string raw)
    {
        return raw.ToUpperInvariant() switch {
            "TRACE" or "TRC" or "VERBOSE" or "VRB" => "TRACE",
            "DEBUG" or "DBG" or "FINE" => "DEBUG",
            "INFO" or "INF" or "INFORMATION" => "INFO",
            "WARN" or "WRN" or "WARNING" => "WARN",
            "ERROR" or "ERR" => "ERROR",
            "FATAL" or "FTL" or "CRITICAL" or "CRI" => "FATAL",
            _ => raw.ToUpperInvariant(),
        };
    }
}

public sealed record FieldExtractionResult(
    string? Level,
    string Message,
    string? FieldsJson);
