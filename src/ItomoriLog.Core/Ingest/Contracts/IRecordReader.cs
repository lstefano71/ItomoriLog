namespace ItomoriLog.Core.Ingest;

public interface IRecordReader : IDisposable
{
    bool TryReadNext(out RawRecord record);
}

public sealed record RawRecord(
    string FirstLine,
    string FullText,
    long LineNumber,
    long ByteOffset,
    IReadOnlyDictionary<string, string>? Fields = null);
