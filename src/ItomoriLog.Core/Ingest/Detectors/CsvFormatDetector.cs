using System.Globalization;
using ItomoriLog.Core.Ingest.Extractors;

namespace ItomoriLog.Core.Ingest.Detectors;

public sealed class CsvFormatDetector : IFormatDetector
{
    private const int MinSniffLines = 5;
    private const int MaxSniffLines = 512;
    private const int MaxDialectSampleRows = 200;
    private const int MaxHeaderSampleRows = 20;
    private const double MinParseRate = 0.95;

    private static readonly char[] CandidateDelimiters = [',', ';', '\t', '|'];
    private static readonly char[] CandidateQuotes = ['"', '\''];

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
        var (bestDelimiter, bestQuote, bestConsistency, modalColumns) = SniffDialect(lines);
        if (bestConsistency < MinParseRate || modalColumns < 2)
            return null;

        // Step 2: Detect header
        bool hasHeader = DetectHeader(lines, bestDelimiter, bestQuote);

        // Step 3: Parse column names
        string[]? columnNames = null;
        int dataStartIndex = 0;
        if (hasHeader)
        {
            columnNames = Readers.CsvRecordReader.ParseCsvLine(lines[0], bestDelimiter, bestQuote);
            dataStartIndex = 1;
        }
        else
        {
            // Generate synthetic column names
            var firstFields = Readers.CsvRecordReader.ParseCsvLine(lines[0], bestDelimiter, bestQuote);
            columnNames = new string[firstFields.Length];
            for (int i = 0; i < firstFields.Length; i++)
                columnNames[i] = $"Column{i}";
        }

        // Step 4: Identify timestamp columns
        var tsFields = IdentifyTimestampFields(lines, dataStartIndex, bestDelimiter, bestQuote, columnNames);
        if (tsFields.Length == 0)
            return null;

        // Step 5: Score — verify timestamp extraction on data rows
        var extractor = new CompositeCsvTsExtractor(tsFields);
        int extracted = 0;
        int dataRows = 0;
        for (int i = dataStartIndex; i < lines.Count; i++)
        {
            var fields = Readers.CsvRecordReader.ParseCsvLine(lines[i], bestDelimiter, bestQuote);
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
            Boundary: new CsvBoundary(bestDelimiter, hasHeader, columnNames, bestQuote),
            Extractor: extractor,
            Notes: $"Delimiter: '{(bestDelimiter == '\t' ? "TAB" : bestDelimiter.ToString())}', " +
                   $"Quote: {DescribeQuote(bestQuote)}, " +
                   $"Columns: {columnNames.Length}, TsFields: [{string.Join(", ", tsFields)}], " +
                   $"ParseRate: {bestConsistency:P0}, TsRate: {tsRate:P0}");
    }

    private static (char delimiter, char quote, double consistency, int modalColumns) SniffDialect(List<string> lines)
    {
        char bestDelim = ',';
        char bestQuote = '"';
        double bestConsistency = 0;
        int bestModalColumns = 0;
        int bestScore = -1;

        foreach (var delim in CandidateDelimiters)
        {
            foreach (var quote in CandidateQuotes)
            {
                var (score, consistency, modalColumns) = ScoreDialect(lines, delim, quote);
                if (score > bestScore)
                {
                    bestScore = score;
                    bestDelim = delim;
                    bestQuote = quote;
                    bestConsistency = consistency;
                    bestModalColumns = modalColumns;
                }
            }
        }

        return (bestDelim, bestQuote, bestConsistency, bestModalColumns);
    }

    private static (int score, double consistency, int modalColumns) ScoreDialect(List<string> lines, char delimiter, char quote)
    {
        var rowCount = Math.Min(lines.Count, MaxDialectSampleRows);
        if (rowCount == 0)
            return (-1, 0, 0);

        var columnCounts = new int[rowCount];
        for (var rowIndex = 0; rowIndex < rowCount; rowIndex++)
            columnCounts[rowIndex] = Readers.CsvRecordReader.ParseCsvLine(lines[rowIndex], delimiter, quote).Length;

        var modalColumns = FindMode(columnCounts);
        if (modalColumns <= 1)
            return (-1, 0, modalColumns);

        var consistentRows = columnCounts.Count(count => count == modalColumns);
        var consistency = (double)consistentRows / rowCount;
        var consistencyScore = (int)Math.Round(consistency * 1000, MidpointRounding.AwayFromZero);
        var structuredFieldScore = 0;
        var maxTypedRows = Math.Min(rowCount, MaxHeaderSampleRows + 1);
        for (var rowIndex = 0; rowIndex < maxTypedRows; rowIndex++)
        {
            var fields = Readers.CsvRecordReader.ParseCsvLine(lines[rowIndex], delimiter, quote);
            if (fields.Length != modalColumns)
                continue;

            structuredFieldScore += fields.Count(field => IsStructuredType(ClassifyField(field)));
        }

        var score = consistencyScore * 1_000_000 + modalColumns * 10_000 + structuredFieldScore * 10 + rowCount;
        return (score, consistency, modalColumns);
    }

    private static int FindMode(IReadOnlyList<int> values)
    {
        if (values.Count == 0)
            return 0;

        var modeGroups = values.GroupBy(value => value)
            .OrderByDescending(group => group.Count())
            .ThenByDescending(group => group.Key)
            .First();
        return modeGroups.Key;
    }

    private static bool DetectHeader(IReadOnlyList<string> lines, char delimiter, char quote)
    {
        if (lines.Count < 2)
            return false;

        var headerFields = Readers.CsvRecordReader.ParseCsvLine(lines[0], delimiter, quote);
        if (headerFields.Length == 0)
            return false;

        var headerTypes = headerFields.Select(ClassifyField).ToArray();
        if (headerTypes.All(type => type == FieldType.Empty) || headerTypes.Any(IsStructuredType))
            return false;

        var structuredHits = new int[headerFields.Length];
        var dataRowCount = 0;
        var maxRowIndex = Math.Min(lines.Count, MaxHeaderSampleRows + 1);
        for (var rowIndex = 1; rowIndex < maxRowIndex; rowIndex++)
        {
            var rowFields = Readers.CsvRecordReader.ParseCsvLine(lines[rowIndex], delimiter, quote);
            if (rowFields.Length != headerFields.Length)
                continue;

            dataRowCount++;
            for (var columnIndex = 0; columnIndex < rowFields.Length; columnIndex++)
            {
                if (IsStructuredType(ClassifyField(rowFields[columnIndex])))
                    structuredHits[columnIndex]++;
            }
        }

        if (dataRowCount == 0)
            return false;

        var threshold = Math.Max(1, (dataRowCount + 1) / 2);
        return structuredHits.Any(hitCount => hitCount >= threshold);
    }

    private static FieldType ClassifyField(string value)
    {
        var trimmed = value.Trim();
        if (trimmed.Length == 0)
            return FieldType.Empty;

        if (bool.TryParse(trimmed, out _))
            return FieldType.Boolean;

        if (long.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out _) ||
            double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out _))
            return FieldType.Number;

        if (DateTimeOffset.TryParse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out _) ||
            DateTime.TryParse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out _))
            return FieldType.Date;

        return FieldType.Text;
    }

    private static bool IsStructuredType(FieldType type) =>
        type is FieldType.Number or FieldType.Boolean or FieldType.Date;

    private static string DescribeQuote(char quote) =>
        quote switch
        {
            '"' => "double-quote",
            '\'' => "single-quote",
            _ => quote.ToString()
        };

    private static string[] IdentifyTimestampFields(List<string> lines, int dataStartIndex, char delimiter, char quote, string[] columnNames)
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
            if (ValidateCompositeTimestamp(lines, dataStartIndex, delimiter, quote, columnNames, [dateCol, timeCol]))
                return [dateCol, timeCol];
        }

        // For headerless or unfamiliar CSVs, try composite timestamp pairs before
        // falling back to a single parseable column.
        for (var first = 0; first < columnNames.Length; first++)
        {
            for (var second = first + 1; second < columnNames.Length; second++)
            {
                var candidateFields = new[] { columnNames[first], columnNames[second] };
                if (ValidateCompositeTimestamp(lines, dataStartIndex, delimiter, quote, columnNames, candidateFields))
                    return candidateFields;
            }
        }

        // Look for single timestamp column by well-known names
        foreach (var tsName in TimestampFieldNames)
        {
            int colIdx = Array.FindIndex(columnNames, c => c.Equals(tsName, StringComparison.OrdinalIgnoreCase));
            if (colIdx < 0) continue;

            var actualName = columnNames[colIdx];
            if (ValidateCompositeTimestamp(lines, dataStartIndex, delimiter, quote, columnNames, [actualName]))
                return [actualName];
        }

        // Brute force: try each column
        for (int col = 0; col < columnNames.Length; col++)
        {
            if (ValidateCompositeTimestamp(lines, dataStartIndex, delimiter, quote, columnNames, [columnNames[col]]))
                return [columnNames[col]];
        }

        return [];
    }

    private static bool ValidateCompositeTimestamp(List<string> lines, int dataStartIndex, char delimiter, char quote, string[] columnNames, string[] tsFields)
    {
        var extractor = new CompositeCsvTsExtractor(tsFields);
        int success = 0;
        int attempts = 0;
        int maxCheck = Math.Min(lines.Count, dataStartIndex + 10);

        for (int i = dataStartIndex; i < maxCheck; i++)
        {
            var fields = Readers.CsvRecordReader.ParseCsvLine(lines[i], delimiter, quote);
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

    private enum FieldType : byte
    {
        Empty,
        Text,
        Number,
        Boolean,
        Date
    }
}
