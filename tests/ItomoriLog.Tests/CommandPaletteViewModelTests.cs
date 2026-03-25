using FluentAssertions;

using ItomoriLog.UI.ViewModels;

namespace ItomoriLog.Tests;

public class CommandPaletteViewModelTests
{
    private static List<PaletteCommand> SampleCommands() =>
    [
        new("New Session", "Create a new session", "Ctrl+N", () => { }),
        new("Open Session", "Open an existing session", "Ctrl+O", () => { }),
        new("Export...", "Export data", null, () => { }),
        new("Toggle Detail Panel", "Show/hide detail", "Ctrl+D", () => { }),
        new("Start Ingestion", "Start staged ingestion", "Ctrl+I", () => { }),
        new("Go to Skips", "View skipped records", null, () => { }),
        new("About", "About ItomoriLog", null, () => { }),
    ];

    [Fact]
    public void InitialState_ShowsAllCommands()
    {
        var vm = new CommandPaletteViewModel(SampleCommands());

        vm.FilteredCommands.Should().HaveCount(7);
        vm.IsOpen.Should().BeFalse();
    }

    [Theory]
    [InlineData("ns", "New Session")]
    [InlineData("os", "Open Session")]
    [InlineData("ex", "Export...")]
    [InlineData("tdp", "Toggle Detail Panel")]
    [InlineData("ab", "About")]
    public void FuzzyFilter_MatchesExpectedCommand(string query, string expectedName)
    {
        var vm = new CommandPaletteViewModel(SampleCommands());

        vm.SearchText = query;

        vm.FilteredCommands.Should().Contain(c => c.Name == expectedName);
    }

    [Theory]
    [InlineData("ns", "New Session", true)]
    [InlineData("NS", "New Session", true)]
    [InlineData("zz", "New Session", false)]
    [InlineData("", "New Session", true)]
    [InlineData("newsess", "New Session", true)]
    [InlineData("nz", "New Session", false)]
    public void FuzzyMatch_ReturnsExpected(string query, string text, bool expected)
    {
        CommandPaletteViewModel.FuzzyMatch(text, query).Should().Be(expected);
    }

    [Fact]
    public void SearchText_FiltersCommands()
    {
        var vm = new CommandPaletteViewModel(SampleCommands());

        vm.SearchText = "about";

        vm.FilteredCommands.Should().ContainSingle()
            .Which.Name.Should().Be("About");
    }

    [Fact]
    public void EmptySearch_ShowsAllCommands()
    {
        var vm = new CommandPaletteViewModel(SampleCommands());

        vm.SearchText = "ns";
        vm.SearchText = "";

        vm.FilteredCommands.Should().HaveCount(7);
    }

    [Fact]
    public void Toggle_OpensAndCloses()
    {
        var vm = new CommandPaletteViewModel(SampleCommands());

        vm.Toggle();
        vm.IsOpen.Should().BeTrue();
        vm.SelectedCommand.Should().NotBeNull();
        vm.SelectedCommand!.Name.Should().Be("New Session");

        vm.Toggle();
        vm.IsOpen.Should().BeFalse();
    }

    [Fact]
    public void Toggle_ClearsSearchText()
    {
        var vm = new CommandPaletteViewModel(SampleCommands());
        vm.SearchText = "test";

        vm.Toggle(); // opens

        vm.SearchText.Should().BeEmpty();
    }

    [Fact]
    public void ExecuteSelectedCommand_InvokesAction()
    {
        var invoked = false;
        var commands = new List<PaletteCommand>
        {
            new("Test", "Test command", null, () => invoked = true),
        };
        var vm = new CommandPaletteViewModel(commands);
        vm.IsOpen = true;
        vm.SelectedCommand = commands[0];

        vm.ExecuteSelectedCommand.Execute().Subscribe();

        invoked.Should().BeTrue();
        vm.IsOpen.Should().BeFalse();
    }

    [Fact]
    public void SelectedCommand_SetToFirstOnFilter()
    {
        var vm = new CommandPaletteViewModel(SampleCommands());

        vm.SearchText = "ab";

        vm.SelectedCommand.Should().NotBeNull();
        vm.SelectedCommand!.Name.Should().Be("About");
    }

    [Fact]
    public void NoMatch_EmptiesFilteredCommands()
    {
        var vm = new CommandPaletteViewModel(SampleCommands());

        vm.SearchText = "zzzzzzz";

        vm.FilteredCommands.Should().BeEmpty();
        vm.SelectedCommand.Should().BeNull();
    }

    [Fact]
    public void MoveSelectionBy_ClampsWithinFilteredCommands()
    {
        var vm = new CommandPaletteViewModel(SampleCommands());

        vm.Open();
        vm.MoveSelectionBy(3);
        vm.SelectedCommand!.Name.Should().Be("Toggle Detail Panel");

        vm.MoveSelectionBy(99);
        vm.SelectedCommand!.Name.Should().Be("About");

        vm.MoveSelectionBy(-99);
        vm.SelectedCommand!.Name.Should().Be("New Session");
    }
}
