using System.Runtime.CompilerServices;

namespace Leviathan.Core.Csv;

/// <summary>
/// Detects the CSV dialect (delimiter, quote, escape) by sampling the beginning
/// of a file. Inspired by DuckDB's CSV sniffer: for each candidate delimiter the
/// sample is parsed with a full RFC 4180 state machine, and the delimiter that
/// yields the most consistent (and highest) column count wins.
/// </summary>
public static class CsvDialectDetector
{
    /// <summary>Candidate delimiters tried in order of priority.</summary>
    private static ReadOnlySpan<byte> CandidateDelimiters => [(byte)',', (byte)'\t', (byte)'|', (byte)';'];

    /// <summary>Candidate quote characters.</summary>
    private static ReadOnlySpan<byte> CandidateQuotes => [(byte)'"', (byte)'\''];

    /// <summary>Maximum number of sample rows to evaluate per candidate.</summary>
    private const int MaxSampleRows = 200;

    /// <summary>
    /// Detects the CSV dialect from a sample of the file content.
    /// </summary>
    /// <param name="sample">
    /// The first chunk of the file (typically 32–64 KB). Larger samples improve
    /// accuracy but the detector caps analysis at <see cref="MaxSampleRows"/> rows.
    /// </param>
    /// <returns>
    /// A <see cref="CsvDialect"/> with detected separator, quote, and escape values.
    /// <see cref="CsvDialect.HasHeader"/> is always <c>true</c> here — use
    /// <see cref="CsvHeaderDetector"/> for header detection.
    /// </returns>
    public static CsvDialect Detect(ReadOnlySpan<byte> sample)
    {
        if (sample.IsEmpty)
            return CsvDialect.Csv();

        byte bestDelimiter = (byte)',';
        byte bestQuote = (byte)'"';
        int bestScore = -1;

        foreach (byte delim in CandidateDelimiters) {
            foreach (byte quote in CandidateQuotes) {
                int score = ScoreDialect(sample, delim, quote);
                if (score > bestScore) {
                    bestScore = score;
                    bestDelimiter = delim;
                    bestQuote = quote;
                }
            }
        }

        return new CsvDialect(bestDelimiter, bestQuote, bestQuote, HasHeader: true);
    }

    /// <summary>
    /// Scores a (delimiter, quote) pair. Higher is better.
    /// The score favours: (1) more consistent column counts across rows,
    /// (2) higher column counts, (3) more rows parsed successfully.
    /// Returns -1 if the dialect is clearly wrong (e.g., every row has 1 column).
    /// </summary>
    private static int ScoreDialect(ReadOnlySpan<byte> sample, byte delimiter, byte quote)
    {
        // Parse rows, tracking column count per row
        Span<int> columnCounts = stackalloc int[MaxSampleRows];
        int rowCount = 0;

        int pos = 0;
        while (pos < sample.Length && rowCount < MaxSampleRows) {
            int rowStart = pos;
            int cols = CountFieldsInRow(sample, ref pos, delimiter, quote);
            if (cols < 0) break; // malformed beyond recovery
            columnCounts[rowCount++] = cols;
        }

        if (rowCount == 0)
            return -1;

        // Find the modal (most frequent) column count
        int modalCount = FindMode(columnCounts[..rowCount]);

        if (modalCount <= 1)
            return -1; // single column → delimiter not present

        // Count how many rows match the modal count
        int consistent = 0;
        for (int i = 0; i < rowCount; i++) {
            if (columnCounts[i] == modalCount)
                consistent++;
        }

        // Score = consistency_ratio * 1000 + modalCount * 10 + rowCount
        // This heavily weights consistency, then column count, then sample size.
        int consistencyScore = (consistent * 1000) / rowCount;
        return consistencyScore * 1000 + modalCount * 10 + rowCount;
    }

    /// <summary>
    /// Counts the number of fields in one row starting at <paramref name="pos"/>,
    /// advancing <paramref name="pos"/> past the row terminator.
    /// Uses a simplified RFC 4180 state machine that tracks quoted regions.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CountFieldsInRow(ReadOnlySpan<byte> data, ref int pos, byte delimiter, byte quote)
    {
        int fields = 1;
        bool inQuoted = false;

        while (pos < data.Length) {
            byte b = data[pos];

            if (inQuoted) {
                if (b == quote) {
                    // Check for escaped quote (doubled)
                    if (pos + 1 < data.Length && data[pos + 1] == quote) {
                        pos += 2;
                        continue;
                    }
                    inQuoted = false;
                }
                pos++;
                continue;
            }

            // Not in quoted field
            if (b == quote) {
                inQuoted = true;
                pos++;
                continue;
            }

            if (b == delimiter) {
                fields++;
                pos++;
                continue;
            }

            if (b == (byte)'\n') {
                pos++;
                return fields;
            }

            if (b == (byte)'\r') {
                pos++;
                if (pos < data.Length && data[pos] == (byte)'\n')
                    pos++;
                return fields;
            }

            pos++;
        }

        // End of data — last row without trailing newline
        return fields;
    }

    /// <summary>
    /// Finds the most frequent value in the span (mode). For ties, returns the larger value
    /// (favour more columns).
    /// </summary>
    private static int FindMode(ReadOnlySpan<int> values)
    {
        if (values.Length == 0) return 0;

        // Sort a copy to find mode efficiently
        Span<int> sorted = stackalloc int[values.Length];
        values.CopyTo(sorted);
        sorted.Sort();

        int bestVal = sorted[0];
        int bestFreq = 1;
        int currentVal = sorted[0];
        int currentFreq = 1;

        for (int i = 1; i < sorted.Length; i++) {
            if (sorted[i] == currentVal) {
                currentFreq++;
            } else {
                if (currentFreq > bestFreq || (currentFreq == bestFreq && currentVal > bestVal)) {
                    bestVal = currentVal;
                    bestFreq = currentFreq;
                }
                currentVal = sorted[i];
                currentFreq = 1;
            }
        }

        if (currentFreq > bestFreq || (currentFreq == bestFreq && currentVal > bestVal)) {
            bestVal = currentVal;
        }

        return bestVal;
    }
}
