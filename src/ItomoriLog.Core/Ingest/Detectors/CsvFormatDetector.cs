using System.Globalization;
using ItomoriLog.Core.Ingest.Extractors;

namespace ItomoriLog.Core.Ingest.Detectors;

public sealed class CsvFormatDetector : IFormatDetector
{
    private const int MinSniffLines = 5;
    private const int MaxSniffLines = 2000;
    private const double MinParseRate = 0.95;

    private static readonly char[] CandidateDelimiters = [',', ';', '\t', '|'];

    private static readonly string[] TimestampFieldNames =
    [
        "timestamp", "Timestamp", "TIMESTAMP",
        "datetime", "DateTime", "DATETIME",
        "date_time", "Date_Time",
        "time", "Time", "TIME",
        "date", "Date", "DATE",
        "ts", "TS",
        "@timestamp", "@t",
        "log_time", "LogTime", "logtime",
        "event_time", "EventTime",
        "created_at", "CreatedAt",
        "updated_at", "UpdatedAt",
    ];

    private static readonly string[] DateFieldNames =
        ["date", "Date", "DATE", "log_date", "LogDate"];

    private static readonly string[] TimeFieldNames =
        ["time", "Time", "TIME", "log_time", "LogTime"];

    public DetectionResult? Probe(Stream sample, string sourceName)
    {
        sample.Position = 0;
        using var reader = new StreamReader(sample, leaveOpen: true);

        var lines = new List<string>();
        string? line;
        while ((line = reader.ReadLine()) is not null && lines.Count < MaxSniffLines)
        {
            if (!string.IsNullOrWhiteSpace(line))
                lines.Add(line);
        }

        if (lines.Count < MinSniffLines)
            return null;

        // Step 1: Sniff delimiter
        var (bestDelimiter, bestConsistency, avgColumns) = SniffDelimiter(lines);
        if (bestConsistency < MinParseRate || avgColumns < 2)
            return null;

        // Step 2: Detect header
        bool hasHeader = DetectHeader(lines[0], lines.Count > 1 ? lines[1] : null, bestDelimiter);

        // Step 3: Parse column names
        string[]? columnNames = null;
        int dataStartIndex = 0;
        if (hasHeader)
        {
            columnNames = Readers.CsvRecordReader.ParseCsvLine(lines[0], bestDelimiter);
            dataStartIndex = 1;
        }
        else
        {
            // Generate synthetic column names
            var firstFields = Readers.CsvRecordReader.ParseCsvLine(lines[0], bestDelimiter);
            columnNames = new string[firstFields.Length];
            for (int i = 0; i < firstFields.Length; i++)
                columnNames[i] = $"Column{i}";
        }

        // Step 4: Identify timestamp columns
        var tsFields = IdentifyTimestampFields(lines, dataStartIndex, bestDelimiter, columnNames);
        if (tsFields.Length == 0)
            return null;

        // Step 5: Score — verify timestamp extraction on data rows
        var extractor = new CompositeCsvTsExtractor(tsFields);
        int extracted = 0;
        int dataRows = 0;
        for (int i = dataStartIndex; i < lines.Count; i++)
        {
            var fields = Readers.CsvRecordReader.ParseCsvLine(lines[i], bestDelimiter);
            if (fields.Length != columnNames.Length) continue;

            dataRows++;
            var fieldDict = new Dictionary<string, string>();
            for (int j = 0; j < columnNames.Length && j < fields.Length; j++)
                fieldDict[columnNames[j]] = fields[j];

            var raw = new RawRecord(lines[i], lines[i], i, 0, fieldDict);
            if (extractor.TryExtract(raw, out _))
                extracted++;
        }

        if (dataRows == 0)
            return null;

        double tsRate = (double)extracted / dataRows;
        if (tsRate < MinParseRate)
            return null;

        double confidence = bestConsistency * 0.4 + tsRate * 0.6;

        return new DetectionResult(
            Confidence: confidence,
            Boundary: new CsvBoundary(bestDelimiter, hasHeader, columnNames),
            Extractor: extractor,
            Notes: $"Delimiter: '{(bestDelimiter == '\t' ? "TAB" : bestDelimiter.ToString())}', " +
                   $"Columns: {columnNames.Length}, TsFields: [{string.Join(", ", tsFields)}], " +
                   $"ParseRate: {bestConsistency:P0}, TsRate: {tsRate:P0}");
    }

    private static (char delimiter, double consistency, double avgColumns) SniffDelimiter(List<string> lines)
    {
        char bestDelim = ',';
        double bestConsistency = 0;
        double bestAvgColumns = 0;

        foreach (var delim in CandidateDelimiters)
        {
            var counts = new List<int>();
            foreach (var line in lines)
            {
                var fields = Readers.CsvRecordReader.ParseCsvLine(line, delim);
                counts.Add(fields.Length);
            }

            if (counts.Count == 0 || counts[0] < 2) continue;

            // Use the most common column count (mode)
            int mode = counts.GroupBy(c => c).OrderByDescending(g => g.Count()).First().Key;
            double consistency = (double)counts.Count(c => c == mode) / counts.Count;
            double avg = counts.Average();

            if (consistency > bestConsistency || (Math.Abs(consistency - bestConsistency) < 0.01 && avg > bestAvgColumns))
            {
                bestDelim = delim;
                bestConsistency = consistency;
                bestAvgColumns = avg;
            }
        }

        return (bestDelim, bestConsistency, bestAvgColumns);
    }

    private static bool DetectHeader(string firstLine, string? secondLine, char delimiter)
    {
        if (secondLine is null) return false;

        var headerFields = Readers.CsvRecordReader.ParseCsvLine(firstLine, delimiter);
        var dataFields = Readers.CsvRecordReader.ParseCsvLine(secondLine, delimiter);

        if (headerFields.Length != dataFields.Length) return false;

        // Header: more alphabetic content; data: more numeric/date content
        int headerAlpha = 0, headerNumeric = 0;
        int dataAlpha = 0, dataNumeric = 0;

        foreach (var f in headerFields)
        {
            var trimmed = f.Trim();
            if (trimmed.Length == 0) continue;
            if (trimmed.All(c => char.IsLetter(c) || c == '_' || c == ' ' || c == '@'))
                headerAlpha++;
            else if (double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out _))
                headerNumeric++;
        }

        foreach (var f in dataFields)
        {
            var trimmed = f.Trim();
            if (trimmed.Length == 0) continue;
            if (trimmed.All(c => char.IsLetter(c) || c == '_' || c == ' '))
                dataAlpha++;
            else if (double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out _) ||
                     DateTimeOffset.TryParse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
                dataNumeric++;
        }

        // Header has significantly more alphabetic fields than data row
        return headerAlpha > headerNumeric && (headerAlpha > dataAlpha || dataNumeric > dataAlpha);
    }

    private static string[] IdentifyTimestampFields(List<string> lines, int dataStartIndex, char delimiter, string[] columnNames)
    {
        // Check for composite Date + Time columns first
        string? dateCol = null;
        string? timeCol = null;

        foreach (var name in columnNames)
        {
            if (dateCol is null && DateFieldNames.Any(n => n.Equals(name, StringComparison.OrdinalIgnoreCase)))
                dateCol = name;
            if (timeCol is null && TimeFieldNames.Any(n => n.Equals(name, StringComparison.OrdinalIgnoreCase)))
                timeCol = name;
        }

        // If we have both date and time columns, check if they form a valid composite timestamp
        if (dateCol is not null && timeCol is not null)
        {
            if (ValidateCompositeTimestamp(lines, dataStartIndex, delimiter, columnNames, [dateCol, timeCol]))
                return [dateCol, timeCol];
        }

        // Look for single timestamp column by well-known names
        foreach (var tsName in TimestampFieldNames)
        {
            int colIdx = Array.FindIndex(columnNames, c => c.Equals(tsName, StringComparison.OrdinalIgnoreCase));
            if (colIdx < 0) continue;

            var actualName = columnNames[colIdx];
            if (ValidateCompositeTimestamp(lines, dataStartIndex, delimiter, columnNames, [actualName]))
                return [actualName];
        }

        // Brute force: try each column
        for (int col = 0; col < columnNames.Length; col++)
        {
            if (ValidateCompositeTimestamp(lines, dataStartIndex, delimiter, columnNames, [columnNames[col]]))
                return [columnNames[col]];
        }

        return [];
    }

    private static bool ValidateCompositeTimestamp(List<string> lines, int dataStartIndex, char delimiter, string[] columnNames, string[] tsFields)
    {
        var extractor = new CompositeCsvTsExtractor(tsFields);
        int success = 0;
        int attempts = 0;
        int maxCheck = Math.Min(lines.Count, dataStartIndex + 10);

        for (int i = dataStartIndex; i < maxCheck; i++)
        {
            var fields = Readers.CsvRecordReader.ParseCsvLine(lines[i], delimiter);
            if (fields.Length != columnNames.Length) continue;

            attempts++;
            var fieldDict = new Dictionary<string, string>();
            for (int j = 0; j < columnNames.Length && j < fields.Length; j++)
                fieldDict[columnNames[j]] = fields[j];

            var raw = new RawRecord(lines[i], lines[i], i, 0, fieldDict);
            if (extractor.TryExtract(raw, out _))
                success++;
        }

        return attempts > 0 && (double)success / attempts >= 0.8;
    }
}
