using FluentAssertions;

using ItomoriLog.Core.Query;

namespace ItomoriLog.Tests.Query;

public class SearchQueryParserTests
{
    private readonly SearchQueryParser _parser = new();

    [Fact]
    public void Parse_Empty_ReturnsNoFilters()
    {
        var result = _parser.Parse("   ");
        result.Error.Should().BeNull();
        result.MessageQuery.Should().BeNull();
        result.TickExpression.Should().BeNull();
    }

    [Fact]
    public void Parse_ImplicitAnd_BuildsAndTree()
    {
        var result = _parser.Parse("error timeout");
        result.Error.Should().BeNull();
        result.TickExpression.Should().BeNull();
        result.MessageQuery.Should().BeOfType<MessageAndNode>();
    }

    [Fact]
    public void Parse_ExplicitOr_HasLowerPrecedenceThanAnd()
    {
        var result = _parser.Parse("alpha OR beta gamma");
        result.Error.Should().BeNull();

        result.MessageQuery.Should().BeOfType<MessageOrNode>();
        var orNode = (MessageOrNode)result.MessageQuery!;
        orNode.Left.Should().BeOfType<MessageTermNode>();
        orNode.Right.Should().BeOfType<MessageAndNode>();
    }

    [Fact]
    public void Parse_Parentheses_OverridesPrecedence()
    {
        var result = _parser.Parse("(alpha OR beta) gamma");
        result.Error.Should().BeNull();

        result.MessageQuery.Should().BeOfType<MessageAndNode>();
        var andNode = (MessageAndNode)result.MessageQuery!;
        andNode.Left.Should().BeOfType<MessageOrNode>();
        andNode.Right.Should().BeOfType<MessageTermNode>();
    }

    [Fact]
    public void Parse_TimestampInExtracted_WithResidualMessageQuery()
    {
        var result = _parser.Parse("""error OR timeout timestamp IN '$now-1h..$now'""");
        result.Error.Should().BeNull();
        result.TickExpression.Should().Be("$now-1h..$now");
        result.MessageQuery.Should().NotBeNull();
    }

    [Fact]
    public void Parse_OnlyTimestampIn_LeavesNoMessageQuery()
    {
        var result = _parser.Parse("""timestamp in '$today'""");
        result.Error.Should().BeNull();
        result.TickExpression.Should().Be("$today");
        result.MessageQuery.Should().BeNull();
    }

    [Fact]
    public void Parse_MultipleTickClauses_ReturnsError()
    {
        var result = _parser.Parse("""timestamp in '$today' timestamp in '$now-1h..$now'""");
        result.Error.Should().NotBeNull();
    }

    [Fact]
    public void Parse_UnterminatedQuote_ReturnsError()
    {
        var result = _parser.Parse("alpha \"broken");
        result.Error.Should().NotBeNull();
    }

    [Fact]
    public void SqlEmitter_EmitsEscapedParameterizedSql()
    {
        var query = new MessageAndNode(
            new MessageTermNode(@"50%_error"),
            new MessageOrNode(
                new MessageTermNode("alpha"),
                new MessageTermNode("beta")));

        var emitter = new SearchQuerySqlEmitter();
        var sql = emitter.Emit(query);

        sql.WhereSql.Should().Contain("ILIKE");
        sql.Parameters.Should().HaveCount(3);
        sql.Parameters[0].Should().Be(@"%50\%\_error%");
    }
}
