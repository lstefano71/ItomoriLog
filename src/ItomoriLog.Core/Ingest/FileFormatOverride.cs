using System.Text;
using ItomoriLog.Core.Model;

namespace ItomoriLog.Core.Ingest;

public sealed record FileFormatOverride(
    string SourcePath,
    DetectionResult Detection,
    Encoding? EncodingOverride = null,
    TimeBasisConfig? TimeBasisOverride = null);
