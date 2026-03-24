namespace ItomoriLog.Core.Ingest;

public interface IFormatDetector
{
    DetectionResult? Probe(Stream sample, string sourceName);
}
