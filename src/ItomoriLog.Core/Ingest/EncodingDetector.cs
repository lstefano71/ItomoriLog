using System.Text;

namespace ItomoriLog.Core.Ingest;

public static class EncodingDetector
{
    public static Encoding Detect(Stream stream)
    {
        stream.Position = 0;
        Span<byte> bom = stackalloc byte[4];
        int read = stream.Read(bom);
        stream.Position = 0;

        if (read >= 3 && bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF)
            return Encoding.UTF8;
        if (read >= 2 && bom[0] == 0xFF && bom[1] == 0xFE)
            return Encoding.Unicode; // UTF-16 LE
        if (read >= 2 && bom[0] == 0xFE && bom[1] == 0xFF)
            return Encoding.BigEndianUnicode; // UTF-16 BE

        // Try UTF-8 validation
        stream.Position = 0;
        var buffer = new byte[Math.Min(stream.Length, 8192)];
        stream.Read(buffer);
        stream.Position = 0;

        if (IsValidUtf8(buffer))
            return Encoding.UTF8;

        // Fallback to Windows-1252
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        return Encoding.GetEncoding(1252);
    }

    private static bool IsValidUtf8(ReadOnlySpan<byte> data)
    {
        for (int i = 0; i < data.Length;)
        {
            byte b = data[i];
            int seqLen;
            if (b <= 0x7F) { seqLen = 1; }
            else if (b >= 0xC2 && b <= 0xDF) { seqLen = 2; }
            else if (b >= 0xE0 && b <= 0xEF) { seqLen = 3; }
            else if (b >= 0xF0 && b <= 0xF4) { seqLen = 4; }
            else return false;

            if (i + seqLen > data.Length) break; // allow truncation at end
            for (int j = 1; j < seqLen; j++)
                if ((data[i + j] & 0xC0) != 0x80) return false;
            i += seqLen;
        }
        return true;
    }
}
