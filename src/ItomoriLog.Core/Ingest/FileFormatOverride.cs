using ItomoriLog.Core.Model;

using System.Text;

namespace ItomoriLog.Core.Ingest;

public sealed record FileFormatOverride(
    string SourcePath,
    DetectionResult Detection,
    Encoding? EncodingOverride = null,
    TimeBasisConfig? TimeBasisOverride = null);
