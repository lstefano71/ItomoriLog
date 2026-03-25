using BenchmarkDotNet.Attributes;
using ItomoriLog.Core.Ingest;
using System.Text.RegularExpressions;
using Microsoft.VSDiagnostics;

namespace ItomoriLog.Benchmarks;
[CPUUsageDiagnoser]
public class FieldSynthesizerBenchmarks
{
    private FieldSynthesizer _synthesizerNoPattern = null!;
    private FieldSynthesizer _synthesizerWithPattern = null!;
    private const string LevelSourceTaskMsg = "INFO [MyApp.Core] [task-123] Starting the ingest pipeline for segment abc123def";
    private const string LevelSourceMsg = "WARN [MyApp.Http] Request timed out after 30 seconds waiting for upstream";
    private const string LevelMsg = "ERROR Something went wrong during processing of the batch";
    private const string NoPatternMsg = "raw unstructured line with no recognisable severity prefix";
    [GlobalSetup]
    public void Setup()
    {
        _synthesizerNoPattern = new FieldSynthesizer();
        _synthesizerWithPattern = new FieldSynthesizer(FieldSynthesizer.CommonPatterns[0]);
    }

    [Benchmark(Baseline = true, Description = "AutoDetect – LevelSourceTaskMsg")]
    public FieldExtractionResult AutoDetect_LevelSourceTaskMessage() => _synthesizerNoPattern.Extract(LevelSourceTaskMsg);
    [Benchmark(Description = "AutoDetect – LevelSourceMsg")]
    public FieldExtractionResult AutoDetect_LevelSourceMessage() => _synthesizerNoPattern.Extract(LevelSourceMsg);
    [Benchmark(Description = "AutoDetect – LevelMsg")]
    public FieldExtractionResult AutoDetect_LevelMessage() => _synthesizerNoPattern.Extract(LevelMsg);
    [Benchmark(Description = "AutoDetect – NoPattern")]
    public FieldExtractionResult AutoDetect_NoPattern() => _synthesizerNoPattern.Extract(NoPatternMsg);
    [Benchmark(Description = "ExplicitPattern – LevelSourceTaskMsg")]
    public FieldExtractionResult ExplicitPattern_LevelSourceTaskMessage() => _synthesizerWithPattern.Extract(LevelSourceTaskMsg);
}