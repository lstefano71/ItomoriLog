namespace ItomoriLog.Core.Model;

public enum SkipReasonCode
{
    DecodeError,
    CsvColumnMismatch,
    JsonMalformed,
    RegexDrift,
    Oversize,
    TimeParse,
    ZipEntryCorrupt,
    UserSkip,
    NestedArchive,
    NotRecognized,
    Abandoned,
    IOError
}
