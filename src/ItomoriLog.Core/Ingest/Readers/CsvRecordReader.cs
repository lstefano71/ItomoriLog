using System.Text;

namespace ItomoriLog.Core.Ingest.Readers;

public sealed class CsvRecordReader : IRecordReader
{
    private readonly TextReader _reader;
    private readonly CsvBoundary _boundary;
    private readonly SkipLogger? _skipLogger;
    private readonly string[] _columnNames;
    private readonly int _expectedColumnCount;
    private readonly int _resyncThreshold;
    private const int MaxConsecutiveFailures = 10_000;

    private long _lineNumber;
    private long _byteOffset;
    private int _consecutiveBadRows;
    private int _consecutiveGoodRows;
    private SkipLogger.SkipSegment? _activeSkip;
    private readonly Func<string, long> _byteCounter;

    public CsvRecordReader(
        TextReader reader,
        CsvBoundary boundary,
        SkipLogger? skipLogger = null,
        int resyncThreshold = 5,
        Func<string, long>? byteCounter = null)
    {
        _reader = reader;
        _boundary = boundary;
        _skipLogger = skipLogger;
        _resyncThreshold = resyncThreshold;
        _byteCounter = byteCounter ?? DefaultByteCount;

        _columnNames = boundary.ColumnNames ?? [];
        _expectedColumnCount = _columnNames.Length;

        // Skip header row if present
        if (boundary.HasHeader) {
            var headerLine = _reader.ReadLine();
            if (headerLine is not null) {
                _lineNumber++;
                _byteOffset += _byteCounter(headerLine) + 1;

                // If no column names were provided, parse them from the header
                if (_expectedColumnCount == 0) {
                    _columnNames = ParseCsvLine(headerLine, boundary.Delimiter, boundary.Quote);
                    _expectedColumnCount = _columnNames.Length;
                }
            }
        }
    }

    public bool TryReadNext(out RawRecord record)
    {
        while (true) {
            var line = _reader.ReadLine();
            if (line is null) {
                CloseActiveSkip();
                record = default!;
                return false;
            }

            _lineNumber++;
            _byteOffset += _byteCounter(line) + 1;

            if (string.IsNullOrWhiteSpace(line))
                continue;

            var fields = ParseCsvLine(line, _boundary.Delimiter, _boundary.Quote);

            if (_expectedColumnCount > 0 && fields.Length != _expectedColumnCount) {
                // Bad row
                _consecutiveBadRows++;
                _consecutiveGoodRows = 0;

                if (_consecutiveBadRows >= MaxConsecutiveFailures) {
                    CloseActiveSkip();
                    _skipLogger?.BeginSkip(
                        Model.SkipReasonCode.Abandoned,
                        $"Abandoned after {MaxConsecutiveFailures} consecutive bad rows",
                        startLine: _lineNumber).Close(endLine: _lineNumber);
                    record = default!;
                    return false;
                }

                if (_activeSkip is null && _skipLogger is not null) {
                    _activeSkip = _skipLogger.BeginSkip(
                        Model.SkipReasonCode.CsvColumnMismatch,
                        $"Expected {_expectedColumnCount} columns, got {fields.Length}",
                        startLine: _lineNumber,
                        startOffset: _byteOffset,
                        samplePrefix: Encoding.UTF8.GetBytes(line[..Math.Min(line.Length, 256)]));
                }

                continue;
            }

            // Good row
            _consecutiveBadRows = 0;
            _consecutiveGoodRows++;

            if (_activeSkip is not null && _consecutiveGoodRows >= _resyncThreshold) {
                CloseActiveSkip();
            }

            var fieldDict = new Dictionary<string, string>(_columnNames.Length);
            for (int i = 0; i < _columnNames.Length && i < fields.Length; i++) {
                fieldDict[_columnNames[i]] = fields[i];
            }

            record = new RawRecord(
                FirstLine: line,
                FullText: line,
                LineNumber: _lineNumber,
                ByteOffset: _byteOffset,
                Fields: fieldDict,
                EndByteOffset: _byteOffset);
            return true;
        }
    }

    private void CloseActiveSkip()
    {
        if (_activeSkip is not null) {
            var skip = _activeSkip.Value;
            skip.Close(endLine: _lineNumber, endOffset: _byteOffset);
            _activeSkip = null;
        }
    }

    internal static string[] ParseCsvLine(string line, char delimiter, char quote = '"')
    {
        var fields = new List<string>();
        var sb = new StringBuilder();
        bool inQuotes = false;
        int i = 0;

        while (i < line.Length) {
            char c = line[i];

            if (inQuotes) {
                if (c == quote) {
                    // Check for escaped quote (RFC 4180)
                    if (i + 1 < line.Length && line[i + 1] == quote) {
                        sb.Append(quote);
                        i += 2;
                        continue;
                    }
                    inQuotes = false;
                    i++;
                    continue;
                }
                sb.Append(c);
                i++;
            } else {
                if (c == quote && sb.Length == 0) {
                    inQuotes = true;
                    i++;
                } else if (c == delimiter) {
                    fields.Add(sb.ToString());
                    sb.Clear();
                    i++;
                } else {
                    sb.Append(c);
                    i++;
                }
            }
        }

        fields.Add(sb.ToString());
        return [.. fields];
    }

    public void Dispose() { }

    private static long DefaultByteCount(string line) => Encoding.UTF8.GetByteCount(line);
}
