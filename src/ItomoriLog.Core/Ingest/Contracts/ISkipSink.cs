using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Ingest;

public interface ISkipSink
{
    void Write(SkipRow skip);
}
