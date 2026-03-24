using System.Text;
using System.Text.Json;

namespace ItomoriLog.Core.Ingest.Readers;

public sealed class NdjsonRecordReader : IRecordReader
{
    private readonly TextReader _reader;
    private readonly JsonNdBoundary _boundary;
    private readonly SkipLogger? _skipLogger;
    private const int ResyncThreshold = 3;
    private const int MaxConsecutiveFailures = 10_000;

    private long _lineNumber;
    private long _byteOffset;
    private int _consecutiveBadRows;
    private int _consecutiveGoodRows;
    private SkipLogger.SkipSegment? _activeSkip;
    private readonly Func<string, long> _byteCounter;

    public NdjsonRecordReader(
        TextReader reader,
        JsonNdBoundary boundary,
        SkipLogger? skipLogger = null,
        Func<string, long>? byteCounter = null)
    {
        _reader = reader;
        _boundary = boundary;
        _skipLogger = skipLogger;
        _byteCounter = byteCounter ?? DefaultByteCount;
    }

    public bool TryReadNext(out RawRecord record)
    {
        while (true)
        {
            var line = _reader.ReadLine();
            if (line is null)
            {
                CloseActiveSkip();
                record = default!;
                return false;
            }

            _lineNumber++;
            _byteOffset += _byteCounter(line) + 1;

            if (string.IsNullOrWhiteSpace(line))
                continue;

            Dictionary<string, string>? fields;
            try
            {
                fields = ParseJsonLine(line);
            }
            catch
            {
                // Malformed JSON
                _consecutiveBadRows++;
                _consecutiveGoodRows = 0;

                if (_consecutiveBadRows >= MaxConsecutiveFailures)
                {
                    CloseActiveSkip();
                    _skipLogger?.BeginSkip(
                        Model.SkipReasonCode.Abandoned,
                        $"Abandoned after {MaxConsecutiveFailures} consecutive malformed lines",
                        startLine: _lineNumber).Close(endLine: _lineNumber);
                    record = default!;
                    return false;
                }

                if (_activeSkip is null && _skipLogger is not null)
                {
                    _activeSkip = _skipLogger.BeginSkip(
                        Model.SkipReasonCode.JsonMalformed,
                        "Failed to parse JSON line",
                        startLine: _lineNumber,
                        startOffset: _byteOffset,
                        samplePrefix: Encoding.UTF8.GetBytes(line[..Math.Min(line.Length, 256)]));
                }

                continue;
            }

            // Good row
            _consecutiveBadRows = 0;
            _consecutiveGoodRows++;

            if (_activeSkip is not null && _consecutiveGoodRows >= ResyncThreshold)
            {
                CloseActiveSkip();
            }

            record = new RawRecord(
                FirstLine: line,
                FullText: line,
                LineNumber: _lineNumber,
                ByteOffset: _byteOffset,
                Fields: fields,
                EndByteOffset: _byteOffset);
            return true;
        }
    }

    private static Dictionary<string, string> ParseJsonLine(string line)
    {
        using var doc = JsonDocument.Parse(line);
        var fields = new Dictionary<string, string>();

        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            throw new JsonException("Expected JSON object");

        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            fields[prop.Name] = prop.Value.ValueKind switch
            {
                JsonValueKind.String => prop.Value.GetString() ?? "",
                JsonValueKind.Number => prop.Value.GetRawText(),
                JsonValueKind.True => "true",
                JsonValueKind.False => "false",
                JsonValueKind.Null => "",
                _ => prop.Value.GetRawText(),
            };
        }

        return fields;
    }

    private void CloseActiveSkip()
    {
        if (_activeSkip is not null)
        {
            var skip = _activeSkip.Value;
            skip.Close(endLine: _lineNumber, endOffset: _byteOffset);
            _activeSkip = null;
        }
    }

    public void Dispose() { }

    private static long DefaultByteCount(string line) => Encoding.UTF8.GetByteCount(line);
}
