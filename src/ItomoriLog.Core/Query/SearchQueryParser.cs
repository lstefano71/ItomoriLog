using System.Text.RegularExpressions;

namespace ItomoriLog.Core.Query;

public abstract record MessageQueryNode;
public sealed record MessageTermNode(string Term) : MessageQueryNode;
public sealed record MessageAndNode(MessageQueryNode Left, MessageQueryNode Right) : MessageQueryNode;
public sealed record MessageOrNode(MessageQueryNode Left, MessageQueryNode Right) : MessageQueryNode;

public sealed record SearchQueryParseResult(
    MessageQueryNode? MessageQuery,
    string? TickExpression,
    string? Error);

public sealed partial class SearchQueryParser
{
    private static readonly Regex TickClauseRegex = TickClauseRegexImpl();

    public SearchQueryParseResult Parse(string? queryText)
    {
        if (string.IsNullOrWhiteSpace(queryText))
            return new SearchQueryParseResult(null, null, null);

        var working = queryText.Trim();
        string? tickExpression = null;

        var tickMatches = TickClauseRegex.Matches(working);
        if (tickMatches.Count > 1)
            return new SearchQueryParseResult(null, null, "Only one timestamp IN 'TICK' clause is supported.");

        if (tickMatches.Count == 1) {
            var match = tickMatches[0];
            var quote = match.Groups["quote"].Value[0];
            tickExpression = UnescapeQuoted(match.Groups["tick"].Value, quote);
            working = working.Remove(match.Index, match.Length);
            working = NormalizeResidualQuery(working);
        }

        if (string.IsNullOrWhiteSpace(working))
            return new SearchQueryParseResult(null, tickExpression, null);

        var tokenized = Tokenizer.Tokenize(working);
        if (tokenized.Error is not null)
            return new SearchQueryParseResult(null, tickExpression, tokenized.Error);

        var parser = new Parser(tokenized.Tokens);
        var messageQuery = parser.ParseExpression();
        if (parser.Error is not null)
            return new SearchQueryParseResult(null, tickExpression, parser.Error);

        return new SearchQueryParseResult(messageQuery, tickExpression, null);
    }

    private static string NormalizeResidualQuery(string text)
    {
        var normalized = WhitespaceRegex().Replace(text, " ").Trim();
        while (true) {
            var previous = normalized;
            normalized = LeadingBooleanRegex().Replace(normalized, "").Trim();
            normalized = TrailingBooleanRegex().Replace(normalized, "").Trim();
            if (normalized == previous)
                break;
        }
        return normalized;
    }

    private static string UnescapeQuoted(string value, char quote)
    {
        var quoteEscaped = $@"\{quote}";
        return value
            .Replace(quoteEscaped, quote.ToString(), StringComparison.Ordinal)
            .Replace(@"\\", @"\", StringComparison.Ordinal);
    }

    private enum TokenKind
    {
        Term,
        And,
        Or,
        LParen,
        RParen
    }

    [GeneratedRegex("""\btimestamp\s+in\s*(?<quote>['"])(?<tick>(?:\\.|(?!\k<quote>).)*)\k<quote>""", RegexOptions.IgnoreCase)]
    private static partial Regex TickClauseRegexImpl();

    [GeneratedRegex(@"\s+")]
    private static partial Regex WhitespaceRegex();

    [GeneratedRegex(@"^(AND|OR)\s+", RegexOptions.IgnoreCase)]
    private static partial Regex LeadingBooleanRegex();

    [GeneratedRegex(@"\s+(AND|OR)$", RegexOptions.IgnoreCase)]
    private static partial Regex TrailingBooleanRegex();

    private readonly record struct Token(TokenKind Kind, string Text);

    private sealed record TokenizeResult(IReadOnlyList<Token> Tokens, string? Error);

    private static class Tokenizer
    {
        public static TokenizeResult Tokenize(string input)
        {
            var tokens = new List<Token>();
            var i = 0;

            while (i < input.Length) {
                var c = input[i];
                if (char.IsWhiteSpace(c)) {
                    i++;
                    continue;
                }

                if (c == '(') {
                    tokens.Add(new Token(TokenKind.LParen, "("));
                    i++;
                    continue;
                }

                if (c == ')') {
                    tokens.Add(new Token(TokenKind.RParen, ")"));
                    i++;
                    continue;
                }

                if (c == '"' || c == '\'') {
                    var quote = c;
                    i++;
                    var start = i;
                    var escaped = false;
                    var sb = new System.Text.StringBuilder();
                    while (i < input.Length) {
                        var ch = input[i];
                        if (!escaped && ch == '\\') {
                            escaped = true;
                            i++;
                            continue;
                        }

                        if (!escaped && ch == quote)
                            break;

                        sb.Append(ch);
                        escaped = false;
                        i++;
                    }

                    if (i >= input.Length || input[i] != quote)
                        return new TokenizeResult([], $"Unterminated quoted term starting at position {start}.");

                    var quotedTerm = sb.ToString().Trim();
                    if (quotedTerm.Length > 0)
                        tokens.Add(new Token(TokenKind.Term, quotedTerm));

                    i++; // consume closing quote
                    continue;
                }

                var termStart = i;
                while (i < input.Length && !char.IsWhiteSpace(input[i]) && input[i] != '(' && input[i] != ')')
                    i++;

                var term = input[termStart..i];
                if (term.Length == 0)
                    continue;

                if (term.Equals("OR", StringComparison.OrdinalIgnoreCase))
                    tokens.Add(new Token(TokenKind.Or, term));
                else if (term.Equals("AND", StringComparison.OrdinalIgnoreCase))
                    tokens.Add(new Token(TokenKind.And, term));
                else
                    tokens.Add(new Token(TokenKind.Term, term));
            }

            return new TokenizeResult(tokens, null);
        }
    }

    private sealed class Parser
    {
        private readonly IReadOnlyList<Token> _tokens;
        private int _pos;

        public string? Error { get; private set; }

        public Parser(IReadOnlyList<Token> tokens)
        {
            _tokens = tokens;
        }

        public MessageQueryNode? ParseExpression()
        {
            var node = ParseOr();
            if (node is null)
                return null;

            if (_pos < _tokens.Count) {
                Error = $"Unexpected token '{_tokens[_pos].Text}'.";
                return null;
            }

            return node;
        }

        private MessageQueryNode? ParseOr()
        {
            var left = ParseAnd();
            if (left is null)
                return null;

            while (PeekKind() == TokenKind.Or) {
                _pos++; // consume OR
                var right = ParseAnd();
                if (right is null) {
                    Error ??= "Expected term or '(' after OR.";
                    return null;
                }
                left = new MessageOrNode(left, right);
            }

            return left;
        }

        private MessageQueryNode? ParseAnd()
        {
            var left = ParsePrimary();
            if (left is null)
                return null;

            while (true) {
                var explicitAnd = false;
                if (PeekKind() == TokenKind.And) {
                    explicitAnd = true;
                    _pos++; // consume AND
                } else if (!CanStartPrimary(PeekKind())) {
                    break;
                }

                var right = ParsePrimary();
                if (right is null) {
                    Error ??= explicitAnd
                        ? "Expected term or '(' after AND."
                        : "Expected term or '(' after implicit AND.";
                    return null;
                }
                left = new MessageAndNode(left, right);
            }

            return left;
        }

        private MessageQueryNode? ParsePrimary()
        {
            var kind = PeekKind();
            if (kind is null) {
                Error ??= "Unexpected end of query.";
                return null;
            }

            if (kind == TokenKind.LParen) {
                _pos++; // consume '('
                var inner = ParseOr();
                if (inner is null)
                    return null;

                if (PeekKind() != TokenKind.RParen) {
                    Error ??= "Missing closing ')'.";
                    return null;
                }

                _pos++; // consume ')'
                return inner;
            }

            if (kind == TokenKind.Term) {
                var token = _tokens[_pos++];
                return new MessageTermNode(token.Text);
            }

            Error ??= $"Expected term or '(', got '{_tokens[_pos].Text}'.";
            return null;
        }

        private TokenKind? PeekKind() => _pos < _tokens.Count ? _tokens[_pos].Kind : null;

        private static bool CanStartPrimary(TokenKind? kind) =>
            kind is TokenKind.Term or TokenKind.LParen;
    }
}

public sealed class SearchQuerySqlEmitter
{
    public SqlEmission Emit(MessageQueryNode query, string messageColumn = "message")
    {
        var parameters = new List<object>();
        var whereSql = EmitNode(query, parameters, messageColumn);
        return new SqlEmission(whereSql, parameters);
    }

    private static string EmitNode(MessageQueryNode query, List<object> parameters, string messageColumn)
    {
        return query switch {
            MessageTermNode term => EmitTerm(term.Term, parameters, messageColumn),
            MessageAndNode and => $"({EmitNode(and.Left, parameters, messageColumn)} AND {EmitNode(and.Right, parameters, messageColumn)})",
            MessageOrNode or => $"({EmitNode(or.Left, parameters, messageColumn)} OR {EmitNode(or.Right, parameters, messageColumn)})",
            _ => throw new NotSupportedException($"Unsupported query node: {query.GetType().Name}")
        };
    }

    private static string EmitTerm(string term, List<object> parameters, string messageColumn)
    {
        parameters.Add($"%{EscapeLikePattern(term)}%");
        return $"{messageColumn} ILIKE ${parameters.Count} ESCAPE '\\'";
    }

    private static string EscapeLikePattern(string input) =>
        input
            .Replace(@"\", @"\\", StringComparison.Ordinal)
            .Replace("%", @"\%", StringComparison.Ordinal)
            .Replace("_", @"\_", StringComparison.Ordinal);
}
