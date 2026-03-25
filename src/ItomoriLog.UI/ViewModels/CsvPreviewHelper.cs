using System.Text;

namespace ItomoriLog.UI.ViewModels;

internal static class CsvPreviewHelper
{
    public static IReadOnlyList<string> ReadNonEmptyLines(byte[] sniffBuffer, int bytesRead, Encoding encoding, int maxLines = 32)
    {
        using var stream = new MemoryStream(sniffBuffer, 0, bytesRead, writable: false);
        using var reader = new StreamReader(stream, encoding, detectEncodingFromByteOrderMarks: true, leaveOpen: false);

        var lines = new List<string>();
        while (reader.ReadLine() is { } line && lines.Count < maxLines) {
            if (!string.IsNullOrWhiteSpace(line))
                lines.Add(line);
        }

        return lines;
    }

    public static string[] SplitLine(string line, char delimiter, char quote)
    {
        var fields = new List<string>();
        var sb = new StringBuilder();
        var inQuotes = false;
        var i = 0;

        while (i < line.Length) {
            var c = line[i];
            if (inQuotes) {
                if (c == quote) {
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
                continue;
            }

            if (c == quote) {
                inQuotes = true;
                i++;
                continue;
            }

            if (c == delimiter) {
                fields.Add(sb.ToString());
                sb.Clear();
                i++;
                continue;
            }

            sb.Append(c);
            i++;
        }

        fields.Add(sb.ToString());
        return [.. fields];
    }

    public static string[] BuildColumnNames(
        IReadOnlyList<string> sampleLines,
        char delimiter,
        char quote,
        bool hasHeader,
        IReadOnlyList<string>? fallbackColumnNames = null)
    {
        if (sampleLines.Count > 0) {
            var firstFields = SplitLine(sampleLines[0], delimiter, quote);
            if (firstFields.Length > 0) {
                if (hasHeader)
                    return firstFields;

                return Enumerable.Range(0, firstFields.Length)
                    .Select(index => $"Column{index}")
                    .ToArray();
            }
        }

        var fallback = fallbackColumnNames?.ToArray() ?? [];
        if (fallback.Length == 0)
            return [];

        if (hasHeader)
            return fallback;

        return Enumerable.Range(0, fallback.Length)
            .Select(index => $"Column{index}")
            .ToArray();
    }
}
