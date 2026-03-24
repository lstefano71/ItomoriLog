namespace ItomoriLog.Core.Ingest;

public interface ITimestampExtractor
{
    bool TryExtract(RawRecord raw, out DateTimeOffset timestamp);
    string Description { get; }
}
