using System.Text.RegularExpressions;

namespace ItomoriLog.Core.Ingest.Readers;

public sealed class TextRecordReader : IRecordReader
{
    private readonly TextReader _reader;
    private readonly Regex _startRegex;
    private readonly int _maxLookahead;
    private BufferedLine? _pushback;
    private long _lineNumber;
    private long _byteOffset;
    private readonly Func<string, long> _byteCounter;

    public TextRecordReader(
        TextReader reader,
        Regex startRegex,
        int maxLookahead = 4096,
        Func<string, long>? byteCounter = null)
    {
        _reader = reader;
        _startRegex = startRegex;
        _maxLookahead = maxLookahead;
        _byteCounter = byteCounter ?? DefaultByteCount;
    }

    public bool TryReadNext(out RawRecord record)
    {
        var lines = new List<string>(8);
        BufferedLine? bufferedLine;
        long recordStartLine = 0;
        long recordStartOffset = 0;
        long recordEndOffset = 0;

        // Seek to next start-of-record
        if (_pushback is not null) {
            bufferedLine = _pushback;
            _pushback = null;
            if (_startRegex.IsMatch(bufferedLine.Value.Text)) {
                lines.Add(bufferedLine.Value.Text);
                recordStartLine = bufferedLine.Value.LineNumber;
                recordStartOffset = bufferedLine.Value.StartByteOffset;
                recordEndOffset = bufferedLine.Value.EndByteOffset;
            }
        }

        if (lines.Count == 0) {
            while ((bufferedLine = ReadLine()) is not null) {
                if (_startRegex.IsMatch(bufferedLine.Value.Text)) {
                    lines.Add(bufferedLine.Value.Text);
                    recordStartLine = bufferedLine.Value.LineNumber;
                    recordStartOffset = bufferedLine.Value.StartByteOffset;
                    recordEndOffset = bufferedLine.Value.EndByteOffset;
                    break;
                }
            }
        }

        if (lines.Count == 0) {
            record = default!;
            return false;
        }

        // Accumulate continuation lines
        int continuationCount = 0;
        while ((bufferedLine = ReadLine()) is not null) {
            if (_startRegex.IsMatch(bufferedLine.Value.Text)) {
                _pushback = bufferedLine;
                break;
            }

            lines.Add(bufferedLine.Value.Text);
            recordEndOffset = bufferedLine.Value.EndByteOffset;
            if (++continuationCount > _maxLookahead) break;
        }

        var fullText = string.Join('\n', lines);
        record = new RawRecord(
            FirstLine: lines[0],
            FullText: fullText,
            LineNumber: recordStartLine,
            ByteOffset: recordStartOffset,
            Fields: null,
            EndByteOffset: recordEndOffset);
        return true;
    }

    private BufferedLine? ReadLine()
    {
        var previousOffset = _byteOffset;
        var line = _reader.ReadLine();
        if (line is not null) {
            _lineNumber++;
            var lineBytes = _byteCounter(line);
            if (lineBytes < 0)
                lineBytes = 0;
            _byteOffset += lineBytes + 1; // +1 for newline
            return new BufferedLine(line, _lineNumber, previousOffset, _byteOffset);
        }
        return null;
    }

    private static long DefaultByteCount(string line) =>
        System.Text.Encoding.UTF8.GetByteCount(line);

    private readonly record struct BufferedLine(
        string Text,
        long LineNumber,
        long StartByteOffset,
        long EndByteOffset);

    public void Dispose() { }
}
