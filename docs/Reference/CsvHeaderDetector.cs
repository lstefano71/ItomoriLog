namespace Leviathan.Core.Csv;

/// <summary>
/// Detects whether a CSV file's first record is a header row by comparing
/// the data types of the first row against subsequent data rows.
/// Inspired by DuckDB's header detection heuristic: if the first row contains
/// all text values while later rows contain numeric or date-like values in
/// at least one column, the first row is considered a header.
/// </summary>
public static class CsvHeaderDetector
{
    /// <summary>Maximum number of data rows to sample for type analysis.</summary>
    private const int MaxSampleRows = 20;

    /// <summary>Maximum number of columns to analyse.</summary>
    private const int MaxColumns = 256;

    /// <summary>
    /// Analyses the first few rows of a CSV to determine whether the first row
    /// is a header.
    /// </summary>
    /// <param name="sample">Raw bytes from the start of the file.</param>
    /// <param name="dialect">
    /// The detected dialect (<see cref="CsvDialect.HasHeader"/> is ignored;
    /// this method determines it).
    /// </param>
    /// <returns><c>true</c> if the first row appears to be a header.</returns>
    public static bool Detect(ReadOnlySpan<byte> sample, CsvDialect dialect)
    {
        if (sample.IsEmpty)
            return false;

        // Parse up to MaxSampleRows + 1 records (first row + data rows)
        Span<CsvField> fieldBuffer = stackalloc CsvField[MaxColumns];

        // Find first record
        int pos = 0;
        int firstRowEnd = FindRecordEnd(sample, ref pos, dialect);
        if (firstRowEnd < 0)
            return false;

        ReadOnlySpan<byte> firstRowBytes = sample[..firstRowEnd];
        int headerFieldCount = CsvFieldParser.ParseRecord(firstRowBytes, dialect, fieldBuffer);
        if (headerFieldCount == 0)
            return false;

        int colCount = Math.Min(headerFieldCount, MaxColumns);

        // Classify the header row fields
        Span<FieldType> headerTypes = stackalloc FieldType[colCount];
        ClassifyFields(firstRowBytes, fieldBuffer[..colCount], dialect, headerTypes);

        // Check if header row is all text
        bool headerAllText = true;
        for (int i = 0; i < colCount; i++) {
            if (headerTypes[i] != FieldType.Text) {
                headerAllText = false;
                break;
            }
        }

        if (!headerAllText)
            return false; // header must be all text for the heuristic to fire

        // Sample data rows and check if at least one column is consistently non-text
        Span<int> numericHits = stackalloc int[colCount];
        numericHits.Clear();
        int dataRowCount = 0;
        Span<CsvField> rowFields = stackalloc CsvField[MaxColumns];
        Span<FieldType> rowTypes = stackalloc FieldType[MaxColumns];

        for (int row = 0; row < MaxSampleRows && pos < sample.Length; row++) {
            int rowStart = pos;
            int rowEnd = FindRecordEnd(sample, ref pos, dialect);
            if (rowEnd < 0) break;

            ReadOnlySpan<byte> rowBytes = sample[rowStart..rowEnd];
            int fieldCount = CsvFieldParser.ParseRecord(rowBytes, dialect, rowFields);

            int typesToClassify = Math.Min(fieldCount, colCount);
            ClassifyFields(rowBytes, rowFields[..typesToClassify], dialect, rowTypes[..typesToClassify]);

            for (int c = 0; c < typesToClassify; c++) {
                if (rowTypes[c] != FieldType.Text)
                    numericHits[c]++;
            }

            dataRowCount++;
        }

        if (dataRowCount == 0)
            return false; // not enough data to decide

        // If at least one column has > 50% non-text values in data rows → header detected
        int threshold = Math.Max(1, dataRowCount / 2);
        for (int c = 0; c < colCount; c++) {
            if (numericHits[c] >= threshold)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Finds the end offset of the current record (before the newline) and
    /// advances <paramref name="pos"/> past the newline. Respects quoted fields
    /// (newlines inside quotes are not record boundaries).
    /// </summary>
    private static int FindRecordEnd(ReadOnlySpan<byte> data, ref int pos, CsvDialect dialect)
    {
        byte quote = dialect.Quote;
        bool inQuoted = false;
        int start = pos;

        while (pos < data.Length) {
            byte b = data[pos];

            if (inQuoted) {
                if (b == quote) {
                    if (pos + 1 < data.Length && data[pos + 1] == quote) {
                        pos += 2;
                        continue;
                    }
                    inQuoted = false;
                }
                pos++;
                continue;
            }

            if (b == quote) {
                inQuoted = true;
                pos++;
                continue;
            }

            if (b == (byte)'\n') {
                int end = pos;
                pos++;
                return end;
            }

            if (b == (byte)'\r') {
                int end = pos;
                pos++;
                if (pos < data.Length && data[pos] == (byte)'\n')
                    pos++;
                return end;
            }

            pos++;
        }

        // End of data without trailing newline
        return pos > start ? pos : -1;
    }

    /// <summary>
    /// Classifies each field into a rough type category.
    /// </summary>
    private static void ClassifyFields(
        ReadOnlySpan<byte> record,
        ReadOnlySpan<CsvField> fields,
        CsvDialect dialect,
        Span<FieldType> types)
    {
        Span<byte> unescaped = stackalloc byte[1024];

        for (int i = 0; i < fields.Length; i++) {
            CsvField field = fields[i];

            if (field.IsQuoted) {
                int written = CsvFieldParser.UnescapeField(record, field, dialect, unescaped);
                types[i] = ClassifySingle(unescaped[..written]);
            } else {
                int len = Math.Min(field.Length, 1024);
                types[i] = ClassifySingle(record.Slice(field.Offset, len));
            }
        }
    }

    /// <summary>
    /// Classifies a single field value.
    /// </summary>
    private static FieldType ClassifySingle(ReadOnlySpan<byte> value)
    {
        if (value.IsEmpty)
            return FieldType.Empty;

        // Check for boolean
        if (EqualsIgnoreCase(value, "true"u8) || EqualsIgnoreCase(value, "false"u8))
            return FieldType.Boolean;

        // Check for integer (optional leading minus, digits only)
        if (IsInteger(value))
            return FieldType.Number;

        // Check for floating point (digits, optional decimal point, optional exponent)
        if (IsFloat(value))
            return FieldType.Number;

        // Check for date-like patterns (YYYY-MM-DD, DD/MM/YYYY, etc.)
        if (IsDateLike(value))
            return FieldType.Date;

        return FieldType.Text;
    }

    private static bool IsInteger(ReadOnlySpan<byte> value)
    {
        int start = 0;
        if (value.Length > 0 && (value[0] == (byte)'-' || value[0] == (byte)'+'))
            start = 1;
        if (start >= value.Length)
            return false;
        for (int i = start; i < value.Length; i++) {
            if (value[i] < (byte)'0' || value[i] > (byte)'9')
                return false;
        }
        return true;
    }

    private static bool IsFloat(ReadOnlySpan<byte> value)
    {
        bool hasDot = false;
        bool hasE = false;
        bool hasDigit = false;
        int start = 0;

        if (value.Length > 0 && (value[0] == (byte)'-' || value[0] == (byte)'+'))
            start = 1;

        for (int i = start; i < value.Length; i++) {
            byte b = value[i];
            if (b >= (byte)'0' && b <= (byte)'9') {
                hasDigit = true;
                continue;
            }
            if (b == (byte)'.' && !hasDot && !hasE) {
                hasDot = true;
                continue;
            }
            if ((b == (byte)'e' || b == (byte)'E') && !hasE && hasDigit) {
                hasE = true;
                if (i + 1 < value.Length && (value[i + 1] == (byte)'+' || value[i + 1] == (byte)'-'))
                    i++;
                continue;
            }
            return false;
        }
        return hasDigit && hasDot; // require decimal point to distinguish from integer text
    }

    private static bool IsDateLike(ReadOnlySpan<byte> value)
    {
        // Must have at least 8 chars (YYYYMMDD) and contain separators
        if (value.Length < 8 || value.Length > 30)
            return false;

        int separators = 0;
        int digits = 0;
        for (int i = 0; i < value.Length; i++) {
            byte b = value[i];
            if (b >= (byte)'0' && b <= (byte)'9')
                digits++;
            else if (b == (byte)'-' || b == (byte)'/' || b == (byte)'.' || b == (byte)':' || b == (byte)' ' || b == (byte)'T')
                separators++;
            else
                return false; // non-date character
        }

        // A date should have some digits and some separators
        return digits >= 4 && separators >= 2;
    }

    private static bool EqualsIgnoreCase(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++) {
            byte av = a[i];
            byte bv = b[i];
            if (av >= (byte)'A' && av <= (byte)'Z') av += 32;
            if (bv >= (byte)'A' && bv <= (byte)'Z') bv += 32;
            if (av != bv) return false;
        }
        return true;
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
