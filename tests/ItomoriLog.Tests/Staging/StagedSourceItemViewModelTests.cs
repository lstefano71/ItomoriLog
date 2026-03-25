using FluentAssertions;
using ItomoriLog.Core.Ingest;
using ItomoriLog.Core.Ingest.Extractors;
using ItomoriLog.Core.Model;
using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.Tests.Staging;

public class StagedSourceItemViewModelTests
{
    [Fact]
    public void TryBuildSelectedOverride_CsvRecipeCompilesEditedFieldsEncodingAndTimezone()
    {
        var item = new StagedSourceItemViewModel(@"C:\logs\sample.csv", isDirectory: false);
        item.ApplyDetectionChoices([BuildCsvChoice()], needsReview: false);

        item.TimestampOverrideText = "Column0 + Column1";
        item.TimezoneOverrideText = "UTC+02:00";
        item.SelectedEncodingChoice = item.EncodingChoices.Single(choice => choice.CodePage == 1252);

        var built = item.TryBuildSelectedOverride(out var formatOverride);

        built.Should().BeTrue();
        formatOverride.Detection.Boundary.Should().BeOfType<CsvBoundary>();
        formatOverride.Detection.Extractor.Should().BeOfType<CompositeCsvTsExtractor>();
        formatOverride.Detection.Extractor.Description.Should().Be("CsvComposite(Column0+Column1)");
        formatOverride.EncodingOverride.Should().NotBeNull();
        formatOverride.EncodingOverride!.CodePage.Should().Be(1252);
        formatOverride.TimeBasisOverride.Should().NotBeNull();
        formatOverride.TimeBasisOverride!.Basis.Should().Be(TimeBasis.FixedOffset);
        formatOverride.TimeBasisOverride.OffsetMinutes.Should().Be(120);
    }

    [Fact]
    public void InvalidTimezoneOverride_IsReportedAndBlocksOverrideBuild()
    {
        var item = new StagedSourceItemViewModel(@"C:\logs\sample.csv", isDirectory: false);
        item.ApplyDetectionChoices([BuildCsvChoice()], needsReview: false);

        item.TimezoneOverrideText = "Not/A_Real_Zone";

        item.HasOverrideValidationError.Should().BeTrue();
        item.OverrideValidationMessage.Should().Contain("Timezone override");
        item.TryBuildSelectedOverride(out _).Should().BeFalse();
    }

    [Fact]
    public void TryBuildSelectedOverride_CsvStructureOverride_RebuildsDelimiterHeaderAndColumnsFromSample()
    {
        var item = new StagedSourceItemViewModel(@"C:\logs\sample.csv", isDirectory: false);
        item.ApplyDetectionChoices([BuildCsvChoice(
            boundary: new CsvBoundary(',', HasHeader: false, ColumnNames: ["Column0", "Column1", "Column2"]),
            extractor: new CompositeCsvTsExtractor(["Column0"]),
            sampleLines:
            [
                "date;time;message",
                "2023-02-28;13:43:56.961Z;INIT"
            ])], needsReview: false);

        item.SelectedDelimiterChoice = item.DelimiterChoices.Single(choice => choice.Value == ';');
        item.CsvHasHeader = true;
        item.TimestampOverrideText = "date + time";

        var built = item.TryBuildSelectedOverride(out var formatOverride);

        built.Should().BeTrue();
        formatOverride.Detection.Boundary.Should().BeEquivalentTo(new CsvBoundary(';', true, ["date", "time", "message"]));
        formatOverride.Detection.Extractor.Description.Should().Be("CsvComposite(date+time)");
    }

    [Fact]
    public void HeaderToggle_RevalidatesTimestampFieldsAgainstRegeneratedColumns()
    {
        var item = new StagedSourceItemViewModel(@"C:\logs\sample.csv", isDirectory: false);
        item.ApplyDetectionChoices([BuildCsvChoice(
            boundary: new CsvBoundary(',', HasHeader: true, ColumnNames: ["date", "time", "message"]),
            extractor: new CompositeCsvTsExtractor(["date"]),
            sampleLines:
            [
                "date,time,message",
                "2023-02-28,13:43:56.961Z,INIT"
            ])], needsReview: false);

        item.CsvHasHeader = false;

        item.HasOverrideValidationError.Should().BeTrue();
        item.OverrideValidationMessage.Should().Contain("Column0");
        item.TryBuildSelectedOverride(out _).Should().BeFalse();
    }

    [Fact]
    public void ApplyFeedbackSuggestions_SurfacesTemplateAndQueueSummary()
    {
        var item = new StagedSourceItemViewModel(@"C:\logs\sample.csv", isDirectory: false);
        item.ApplyDetectionChoices([BuildCsvChoice()], needsReview: false);

        item.ApplyFeedbackSuggestions(
            "server-prod|.log",
            [
                new FeedbackSuggestionViewModel("CSV · Timestamp · UTF-8 · UTC", "template server-prod|.log", 1.0, 2)
            ]);

        item.HasFilenameTemplateKey.Should().BeTrue();
        item.FilenameTemplateKey.Should().Be("server-prod|.log");
        item.HasFeedbackSuggestions.Should().BeTrue();
        item.QueueSummary.Should().Contain("learned suggestion");
    }

    private static DetectionChoiceViewModel BuildCsvChoice(
        CsvBoundary? boundary = null,
        ITimestampExtractor? extractor = null,
        IReadOnlyList<string>? sampleLines = null)
    {
        var detection = new DetectionResult(
            Confidence: 0.97,
            Boundary: boundary ?? new CsvBoundary(',', HasHeader: false, ColumnNames: ["Column0", "Column1", "Column2"]),
            Extractor: extractor ?? new CompositeCsvTsExtractor(["Column0"]),
            Notes: "Detected during sniff");

        return new DetectionChoiceViewModel(
            FormatName: "CSV",
            ShortLabel: "Column0",
            Summary: "CSV · Column0 · UTF-8 · session timezone",
            Confidence: detection.Confidence,
            Detection: detection,
            Details: [],
            Notes: detection.Notes,
            EncodingCodePage: 65001,
            EncodingDisplay: "UTF-8",
            SampleLines: sampleLines ?? ["2023-02-28,13:43:56.961Z,INIT"]);
    }
}
