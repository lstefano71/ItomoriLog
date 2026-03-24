using System.Text.RegularExpressions;

namespace ItomoriLog.Core.Ingest.Readers;

public sealed class TextRecordReader : IRecordReader
{
    private readonly TextReader _reader;
    private readonly Regex _startRegex;
    private readonly int _maxLookahead;
    private string? _pushback;
    private long _lineNumber;
    private long _byteOffset;

    public TextRecordReader(TextReader reader, Regex startRegex, int maxLookahead = 4096)
    {
        _reader = reader;
        _startRegex = startRegex;
        _maxLookahead = maxLookahead;
    }

    public bool TryReadNext(out RawRecord record)
    {
        var lines = new List<string>(8);
        string? line;
        long recordStartLine = 0;
        long recordStartOffset = 0;

        // Seek to next start-of-record
        if (_pushback is not null)
        {
            line = _pushback;
            _pushback = null;
            if (_startRegex.IsMatch(line))
            {
                lines.Add(line);
                recordStartLine = _lineNumber;
                recordStartOffset = _byteOffset;
            }
        }

        if (lines.Count == 0)
        {
            while ((line = ReadLine()) is not null)
            {
                if (_startRegex.IsMatch(line))
                {
                    lines.Add(line);
                    recordStartLine = _lineNumber;
                    recordStartOffset = _byteOffset;
                    break;
                }
            }
        }

        if (lines.Count == 0)
        {
            record = default!;
            return false;
        }

        // Accumulate continuation lines
        int continuationCount = 0;
        while ((line = ReadLine()) is not null)
        {
            if (_startRegex.IsMatch(line))
            {
                _pushback = line;
                break;
            }

            lines.Add(line);
            if (++continuationCount > _maxLookahead) break;
        }

        var fullText = string.Join('\n', lines);
        record = new RawRecord(lines[0], fullText, recordStartLine, recordStartOffset);
        return true;
    }

    private string? ReadLine()
    {
        var line = _reader.ReadLine();
        if (line is not null)
        {
            _lineNumber++;
            _byteOffset += System.Text.Encoding.UTF8.GetByteCount(line) + 1; // +1 for newline
        }
        return line;
    }

    public void Dispose() { }
}
