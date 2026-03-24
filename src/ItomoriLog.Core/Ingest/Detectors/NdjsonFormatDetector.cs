using System.Text.Json;
using ItomoriLog.Core.Ingest.Extractors;

namespace ItomoriLog.Core.Ingest.Detectors;

public sealed class NdjsonFormatDetector : IFormatDetector
{
    private const int MinSniffLines = 5;
    private const int MaxSniffLines = 512;
    private const double MinParseRate = 0.95;

    private static readonly string[] TimestampFieldNames =
    [
        "@timestamp", "timestamp", "@t", "time", "datetime", "date", "ts",
        "Timestamp", "Time", "DateTime", "Date",
        "TIMESTAMP", "TIME", "DATETIME", "DATE",
        "event_time", "EventTime", "log_time", "LogTime",
        "created_at", "CreatedAt",
    ];

    private static readonly string[] LevelFieldNames =
        ["level", "severity", "@l", "Level", "Severity", "loglevel", "LogLevel", "LEVEL"];

    private static readonly string[] MessageFieldNames =
        ["message", "msg", "@m", "Message", "Msg", "MESSAGE", "text", "body"];

    public DetectionResult? Probe(Stream sample, string sourceName)
    {
        sample.Position = 0;
        using var reader = new StreamReader(sample, leaveOpen: true);

        var lines = new List<string>();
        var parsedObjects = new List<Dictionary<string, string>>();
        string? line;

        while ((line = reader.ReadLine()) is not null && lines.Count < MaxSniffLines)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            lines.Add(line);
            if (!LooksLikeJsonObject(line))
                continue;

            try
            {
                using var doc = JsonDocument.Parse(line);
                if (doc.RootElement.ValueKind != JsonValueKind.Object)
                    continue;

                var fields = new Dictionary<string, string>();
                foreach (var prop in doc.RootElement.EnumerateObject())
                {
                    fields[prop.Name] = prop.Value.ValueKind switch
                    {
                        JsonValueKind.String => prop.Value.GetString() ?? "",
                        JsonValueKind.Number => prop.Value.GetRawText(),
                        _ => prop.Value.GetRawText(),
                    };
                }
                parsedObjects.Add(fields);
            }
            catch
            {
                // Not valid JSON — will lower parse rate
            }
        }

        if (lines.Count < MinSniffLines)
            return null;

        double parseRate = (double)parsedObjects.Count / lines.Count;
        if (parseRate < MinParseRate)
            return null;

        // Identify timestamp field
        string? tsField = FindTimestampField(parsedObjects);

        // Build extractor
        var extractor = tsField is not null
            ? new JsonTimestampExtractor(tsField)
            : new JsonTimestampExtractor("timestamp"); // fallback

        // Score timestamp extraction
        int tsExtracted = 0;
        if (tsField is not null)
        {
            foreach (var obj in parsedObjects)
            {
                var fieldDict = obj as IReadOnlyDictionary<string, string>;
                var raw = new RawRecord("", "", 0, 0, fieldDict);
                if (extractor.TryExtract(raw, out _))
                    tsExtracted++;
            }
        }

        double tsRate = parsedObjects.Count > 0 ? (double)tsExtracted / parsedObjects.Count : 0;
        double confidence = parseRate * 0.5 + tsRate * 0.5;

        // Require at least some JSON parsing success
        if (confidence < 0.4)
            confidence = parseRate * 0.9; // Still report as NDJSON if JSON parses well

        return new DetectionResult(
            Confidence: confidence,
            Boundary: new JsonNdBoundary(tsField),
            Extractor: extractor,
            Notes: $"ParseRate: {parseRate:P0}, TsField: {tsField ?? "none"}, TsRate: {tsRate:P0}");
    }

    private static string? FindTimestampField(List<Dictionary<string, string>> objects)
    {
        if (objects.Count == 0) return null;

        // Collect all field names across objects
        var fieldCounts = new Dictionary<string, int>();
        foreach (var obj in objects)
        {
            foreach (var key in obj.Keys)
            {
                fieldCounts[key] = fieldCounts.GetValueOrDefault(key) + 1;
            }
        }

        // Check well-known timestamp field names in priority order
        foreach (var tsName in TimestampFieldNames)
        {
            if (fieldCounts.ContainsKey(tsName))
            {
                // Validate that this field actually contains parseable timestamps
                var extractor = new JsonTimestampExtractor(tsName);
                int success = 0;
                int checks = Math.Min(objects.Count, 10);

                for (int i = 0; i < checks; i++)
                {
                    var raw = new RawRecord("", "", 0, 0, objects[i]);
                    if (extractor.TryExtract(raw, out _))
                        success++;
                }

                if (success > 0 && (double)success / checks >= 0.8)
                    return tsName;
            }
        }

        return null;
    }

    private static bool LooksLikeJsonObject(string line)
    {
        var trimmed = line.Trim();
        return trimmed.Length >= 2
            && trimmed[0] == '{'
            && trimmed[^1] == '}'
            && trimmed.Contains(':');
    }
}
