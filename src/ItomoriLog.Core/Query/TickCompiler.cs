using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

using TimeZoneConverter;

namespace ItomoriLog.Core.Query;

/// <summary>
/// Parses TICK interval expressions (a subset of QuestDB syntax) and resolves them
/// to one or more concrete <see cref="UtcInterval"/> half-open [start, end) ranges.
/// </summary>
public sealed class TickCompiler : ITickCompiler
{
    // -----------------------------------------------------------------------
    //  ISO-8601 date/datetime formats (full -> least precision)
    // -----------------------------------------------------------------------
    private static readonly string[] DateFormats =
    [
        "yyyy-MM-dd'T'HH:mm:ss.fffffffK",
        "yyyy-MM-dd'T'HH:mm:ss.ffffffK",
        "yyyy-MM-dd'T'HH:mm:ss.fffffK",
        "yyyy-MM-dd'T'HH:mm:ss.ffffK",
        "yyyy-MM-dd'T'HH:mm:ss.fffK",
        "yyyy-MM-dd'T'HH:mm:ss.ffK",
        "yyyy-MM-dd'T'HH:mm:ss.fK",
        "yyyy-MM-dd'T'HH:mm:ssK",
        "yyyy-MM-dd'T'HH:mmK",
        "yyyy-MM-dd'T'HH:mm:ss.fffffff",
        "yyyy-MM-dd'T'HH:mm:ss.ffffff",
        "yyyy-MM-dd'T'HH:mm:ss.fffff",
        "yyyy-MM-dd'T'HH:mm:ss.ffff",
        "yyyy-MM-dd'T'HH:mm:ss.fff",
        "yyyy-MM-dd'T'HH:mm:ss.ff",
        "yyyy-MM-dd'T'HH:mm:ss.f",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm",
        "yyyy-MM-dd",
    ];

    private static readonly Regex DurationPattern = new(
        @"(\d+)\s*(ms|us|[yMwdhmsTu])",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    // =======================================================================
    //  Public API
    // =======================================================================

    public TickCompileResult Compile(string input, TickContext context)
    {
        if (string.IsNullOrWhiteSpace(input))
            return new TickCompileResult([], input ?? "", "Empty or whitespace input.");

        var trimmed = input.Trim();

        try
        {
            var tokens = Lexer.Tokenize(trimmed);
            var pos = 0;
            var node = Parser.ParseTopLevel(tokens, ref pos, context);
            if (node == null || pos < tokens.Count)
                return new TickCompileResult([], trimmed, "Unable to parse expression.");

            var raw = Evaluator.Evaluate(node, context);
            if (raw.Count == 0)
                return new TickCompileResult([], trimmed, "Expression produced no intervals.");

            var merged = MergeIntervals(raw);
            var normalized = BuildNormalized(merged);
            return new TickCompileResult(merged, normalized, null);
        }
        catch (TickCompileException ex)
        {
            return new TickCompileResult([], trimmed, ex.Message);
        }
        catch
        {
            return new TickCompileResult([], trimmed, "Unexpected error parsing expression.");
        }
    }

    public static bool TryParseDuration(string input, out TimeSpan duration)
    {
        duration = default;
        if (string.IsNullOrWhiteSpace(input))
            return false;

        var trimmed = input.Trim();
        int pos = 0;
        if (!ConsumeDuration(trimmed, ref pos, out var dur))
            return false;

        if (pos < trimmed.Length)
            return false;

        duration = dur;
        return true;
    }

    public static string FormatTimestamp(DateTimeOffset ts)
        => ts.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    // =======================================================================
    //  Interval merging - half-open [start, end)
    // =======================================================================

    internal static IReadOnlyList<UtcInterval> MergeIntervals(
        List<UtcInterval> raw)
    {
        if (raw.Count <= 1)
            return raw;

        raw.Sort((a, b) => a.Start.CompareTo(b.Start));

        var merged = new List<UtcInterval> { raw[0] };

        for (int i = 1; i < raw.Count; i++)
        {
            var last = merged[^1];
            var cur = raw[i];

            // Half-open: [a,b) and [b,c) are adjacent -> merge
            if (cur.Start <= last.ExclusiveEnd)
            {
                merged[^1] = new UtcInterval(last.Start, cur.ExclusiveEnd > last.ExclusiveEnd ? cur.ExclusiveEnd : last.ExclusiveEnd);
            }
            else
            {
                merged.Add(cur);
            }
        }

        return merged;
    }

    private static string BuildNormalized(IReadOnlyList<UtcInterval> intervals)
    {
        if (intervals.Count == 1)
        {
            var iv = intervals[0];
            return $"{FormatTimestamp(iv.Start)}..{FormatTimestamp(iv.ExclusiveEnd)}";
        }

        var sb = new StringBuilder();
        sb.Append('[');
        for (int i = 0; i < intervals.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            var iv = intervals[i];
            sb.Append(FormatTimestamp(iv.Start));
            sb.Append("..");
            sb.Append(FormatTimestamp(iv.ExclusiveEnd));
        }
        sb.Append(']');
        return sb.ToString();
    }

    // =======================================================================
    //  Shared duration consumer
    // =======================================================================

    internal static bool ConsumeDuration(string input, ref int pos, out TimeSpan duration)
    {
        duration = default;

        var match = DurationPattern.Match(input, pos);
        if (!match.Success || match.Index != pos)
            return false;

        var total = TimeSpan.Zero;
        int endPos = pos;

        while (match.Success && match.Index == endPos)
        {
            var val = long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
            var unit = match.Groups[2].Value;

            total += unit switch
            {
                "y" => TimeSpan.FromDays(val * 365),
                "M" => TimeSpan.FromDays(val * 30),
                "w" => TimeSpan.FromDays(val * 7),
                "d" => TimeSpan.FromDays(val),
                "h" => TimeSpan.FromHours(val),
                "m" => TimeSpan.FromMinutes(val),
                "s" => TimeSpan.FromSeconds(val),
                "T" or "ms" => TimeSpan.FromMilliseconds(val),
                "u" or "us" => TimeSpan.FromTicks(val * 10),
                _ => TimeSpan.Zero
            };

            endPos = match.Index + match.Length;
            match = match.NextMatch();
        }

        if (endPos == pos)
            return false;

        duration = total;
        pos = endPos;
        return true;
    }

    // #######################################################################
    //  Compile exception (for $first/$latest resolution failures)
    // #######################################################################

    private sealed class TickCompileException(string message) : Exception(message);

    // #######################################################################
    //  TOKEN TYPES
    // #######################################################################

    internal enum TokenKind
    {
        LBracket,
        RBracket,
        Comma,
        DotDot,
        Semicolon,
        Plus,
        Minus,
        Timezone,
        Variable,
        IsoLiteral,
        Number,
    }

    internal readonly record struct Token(TokenKind Kind, string Text);

    // #######################################################################
    //  LEXER
    // #######################################################################

    internal static class Lexer
    {
        public static List<Token> Tokenize(string input)
        {
            var tokens = new List<Token>();
            int pos = 0;

            while (pos < input.Length)
            {
                SkipWhitespace(input, ref pos);
                if (pos >= input.Length) break;

                char c = input[pos];

                if (c == '[') { tokens.Add(new Token(TokenKind.LBracket, "[")); pos++; continue; }
                if (c == ']') { tokens.Add(new Token(TokenKind.RBracket, "]")); pos++; continue; }
                if (c == ',') { tokens.Add(new Token(TokenKind.Comma, ",")); pos++; continue; }
                if (c == ';') { tokens.Add(new Token(TokenKind.Semicolon, ";")); pos++; continue; }
                if (c == '+') { tokens.Add(new Token(TokenKind.Plus, "+")); pos++; continue; }

                if (c == '@')
                {
                    if (TryConsumeTimezone(input, ref pos, out var tzToken))
                    {
                        tokens.Add(tzToken);
                        continue;
                    }
                    return tokens;
                }

                if (c == '-')
                {
                    if (tokens.Count > 0)
                    {
                        var prev = tokens[^1].Kind;
                        if (prev == TokenKind.Variable || prev == TokenKind.IsoLiteral ||
                            prev == TokenKind.RBracket || prev == TokenKind.Number)
                        {
                            tokens.Add(new Token(TokenKind.Minus, "-"));
                            pos++;
                            continue;
                        }
                    }
                }

                if (c == '.' && pos + 1 < input.Length && input[pos + 1] == '.')
                {
                    tokens.Add(new Token(TokenKind.DotDot, ".."));
                    pos += 2;
                    continue;
                }

                if (c == '$')
                {
                    var varToken = ConsumeVariable(input, ref pos);
                    if (varToken != null) { tokens.Add(varToken.Value); continue; }
                    return tokens;
                }

                if (char.IsDigit(c) || (c == '-' && pos + 1 < input.Length && char.IsDigit(input[pos + 1])))
                {
                    if (TryConsumeIsoLiteral(input, ref pos, out var isoToken))
                    {
                        tokens.Add(isoToken);
                        continue;
                    }
                    if (TryConsumeNumber(input, ref pos, out var numToken))
                    {
                        tokens.Add(numToken);
                        continue;
                    }
                    return tokens;
                }

                if ((c == 'T' || c == 't') && pos + 1 < input.Length && char.IsDigit(input[pos + 1]))
                {
                    if (TryConsumeTimeSuffix(input, ref pos, out var timeToken))
                    {
                        tokens.Add(timeToken);
                        continue;
                    }
                }

                return tokens;
            }

            return tokens;
        }

        private static Token? ConsumeVariable(string input, ref int pos)
        {
            var remaining = input.AsSpan(pos);
            string[] knownVariables = ["$yesterday", "$tomorrow", "$today", "$latest", "$first", "$start", "$end", "$now"];

            foreach (var variable in knownVariables)
            {
                if (remaining.StartsWith(variable, StringComparison.OrdinalIgnoreCase)
                    && IsVariableBoundary(input, pos + variable.Length))
                {
                    pos += variable.Length;
                    return new Token(TokenKind.Variable, variable.ToLowerInvariant());
                }
            }

            var start = pos;
            pos++; // consume $
            while (pos < input.Length && (char.IsLetterOrDigit(input[pos]) || input[pos] == '_'))
                pos++;

            if (pos <= start + 1)
            {
                pos = start;
                return null;
            }

            return new Token(TokenKind.Variable, input[start..pos].ToLowerInvariant());
        }

        private static bool IsVariableBoundary(string input, int pos)
        {
            if (pos >= input.Length)
                return true;

            var next = input[pos];
            if (!char.IsLetterOrDigit(next) && next != '_')
                return true;

            return (next == 'T' || next == 't')
                && pos + 1 < input.Length
                && char.IsDigit(input[pos + 1]);
        }

        private static bool TryConsumeIsoLiteral(string input, ref int pos, out Token token)
        {
            token = default;
            int maxLen = Math.Min(input.Length - pos, 35);
            var window = input.Substring(pos, maxLen);

            var dateTPrefixMatch = Regex.Match(window, @"^\d{4}-\d{2}-\d{2}T");
            if (dateTPrefixMatch.Success)
            {
                int pLen = dateTPrefixMatch.Length;
                if (pos + pLen < input.Length && input[pos + pLen] == '[')
                {
                    token = new Token(TokenKind.IsoLiteral, window[..pLen]);
                    pos += pLen;
                    return true;
                }
            }

            for (int len = maxLen; len >= 10; len--)
            {
                var candidate = window[..len];
                if (DateTimeOffset.TryParseExact(
                        candidate, DateFormats,
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AllowWhiteSpaces,
                        out _))
                {
                    token = new Token(TokenKind.IsoLiteral, candidate);
                    pos += len;
                    return true;
                }
            }

            var partialMatch = Regex.Match(window, @"^\d{4}(-\d{2})?-");
            if (partialMatch.Success)
            {
                int pLen = partialMatch.Length;
                if (pos + pLen < input.Length && input[pos + pLen] == '[')
                {
                    token = new Token(TokenKind.IsoLiteral, window[..pLen]);
                    pos += pLen;
                    return true;
                }
            }

            return false;
        }

        private static bool TryConsumeNumber(string input, ref int pos, out Token token)
        {
            token = default;
            int start = pos;
            while (pos < input.Length && char.IsDigit(input[pos]))
                pos++;

            if (pos == start)
                return false;

            if (pos < input.Length && input[pos] == ':')
            {
                while (pos < input.Length && (char.IsDigit(input[pos]) || input[pos] == ':' || input[pos] == '.'))
                    pos++;

                token = new Token(TokenKind.IsoLiteral, input[start..pos]);
                return true;
            }

            int unitStart = pos;
            while (pos < input.Length && char.IsLetter(input[pos]))
                pos++;

            token = new Token(TokenKind.Number, input[start..pos]);
            return true;
        }

        private static bool TryConsumeTimezone(string input, ref int pos, out Token token)
        {
            token = default;
            int start = pos;
            if (input[pos] != '@')
                return false;

            pos++;
            int payloadStart = pos;
            while (pos < input.Length)
            {
                char c = input[pos];
                if (char.IsWhiteSpace(c) || c == ',' || c == ';' || c == '[' || c == ']' || c == '.')
                    break;
                pos++;
            }

            if (pos <= payloadStart)
            {
                pos = start;
                return false;
            }

            token = new Token(TokenKind.Timezone, input[start..pos]);
            return true;
        }

        private static bool TryConsumeTimeSuffix(string input, ref int pos, out Token token)
        {
            token = default;
            int start = pos;
            pos++;
            while (pos < input.Length && (char.IsDigit(input[pos]) || input[pos] == ':' || input[pos] == '.'))
                pos++;

            if (pos <= start + 1)
            {
                pos = start;
                return false;
            }

            token = new Token(TokenKind.IsoLiteral, input[start..pos]);
            return true;
        }

        private static void SkipWhitespace(string s, ref int pos)
        {
            while (pos < s.Length && char.IsWhiteSpace(s[pos]))
                pos++;
        }
    }

    // #######################################################################
    //  AST NODE TYPES
    // #######################################################################

    internal abstract class AstNode { }

    internal sealed class LiteralNode(string text) : AstNode
    {
        public string Text { get; } = text;
    }

    internal sealed class VariableNode(string name) : AstNode
    {
        public string Name { get; } = name;
    }

    internal sealed class ArithmeticNode(AstNode @base, char sign, TimeSpan duration) : AstNode
    {
        public AstNode Base { get; } = @base;
        public char Sign { get; } = sign;
        public TimeSpan Duration { get; } = duration;
    }

    internal sealed class RangeNode(AstNode start, AstNode end) : AstNode
    {
        public AstNode Start { get; } = start;
        public AstNode End { get; } = end;
    }

    internal sealed class DurationSuffixNode(AstNode anchor, TimeSpan duration) : AstNode
    {
        public AstNode Anchor { get; } = anchor;
        public TimeSpan Duration { get; } = duration;
    }

    internal sealed class ListNode(List<AstNode> items) : AstNode
    {
        public List<AstNode> Items { get; } = items;
    }

    internal sealed class BracketExpansionNode(
        string prefix, List<AstNode> items, string suffix,
        BracketExpansionNode? nested = null) : AstNode
    {
        public string Prefix { get; } = prefix;
        public List<AstNode> Items { get; } = items;
        public string Suffix { get; } = suffix;
        public BracketExpansionNode? NestedExpansion { get; } = nested;
    }

    internal sealed class NumericRangeNode(int start, int end) : AstNode
    {
        public int Start { get; } = start;
        public int End { get; } = end;
    }

    internal sealed class TimezoneNode(AstNode inner, string timezone) : AstNode
    {
        public AstNode Inner { get; } = inner;
        public string Timezone { get; } = timezone;
    }

    internal sealed class TimeSuffixNode(AstNode inner, string timeSuffix) : AstNode
    {
        public AstNode Inner { get; } = inner;
        public string TimeSuffix { get; } = timeSuffix;
    }

    // #######################################################################
    //  PARSER (Recursive Descent)
    // #######################################################################

    internal static class Parser
    {
        public static AstNode? ParseTopLevel(List<Token> tokens, ref int pos, TickContext context)
        {
            if (pos >= tokens.Count) return null;

            AstNode? node;

            if (Peek(tokens, pos) == TokenKind.LBracket)
            {
                node = ParseBracketedRangeOrList(tokens, ref pos, context);
                if (node == null) return null;
            }
            else if (Peek(tokens, pos) == TokenKind.IsoLiteral)
            {
                node = ParseIsoWithOptionalBrackets(tokens, ref pos);
                if (node == null) return null;
                node = TryParseArithmetic(tokens, ref pos, node);
            }
            else if (Peek(tokens, pos) == TokenKind.Variable)
            {
                node = new VariableNode(tokens[pos].Text);
                pos++;
                node = TryParseArithmetic(tokens, ref pos, node);
            }
            else
            {
                return null;
            }

            node = ParseOptionalTimeSuffix(tokens, ref pos, node);
            node = ParseOptionalTimezone(tokens, ref pos, node);

            if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.DotDot)
            {
                pos++;
                var right = ParseAnchorExpr(tokens, ref pos);
                if (right == null) return null;
                node = new RangeNode(node, right);
            }

            if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.Semicolon)
            {
                pos++;
                if (!TryParseDurationFromTokens(tokens, ref pos, out var dur))
                    return null;
                node = new DurationSuffixNode(node, dur);
            }

            return node;
        }

        private static AstNode? ParseAnchorExpr(List<Token> tokens, ref int pos)
        {
            if (pos >= tokens.Count) return null;

            AstNode? node;

            if (Peek(tokens, pos) == TokenKind.Variable)
            {
                node = new VariableNode(tokens[pos].Text);
                pos++;
            }
            else if (Peek(tokens, pos) == TokenKind.IsoLiteral)
            {
                node = new LiteralNode(tokens[pos].Text);
                pos++;
            }
            else
            {
                return null;
            }

            node = ParseOptionalTimeSuffix(tokens, ref pos, node);
            node = ParseOptionalTimezone(tokens, ref pos, node);

            return TryParseArithmetic(tokens, ref pos, node);
        }

        private static AstNode TryParseArithmetic(List<Token> tokens, ref int pos, AstNode node)
        {
            while (pos < tokens.Count)
            {
                var kind = Peek(tokens, pos);
                if (kind != TokenKind.Plus && kind != TokenKind.Minus)
                    break;

                char sign = kind == TokenKind.Plus ? '+' : '-';
                pos++;

                if (!TryParseDurationFromTokens(tokens, ref pos, out var dur))
                    break;

                node = new ArithmeticNode(node, sign, dur);
            }

            return node;
        }

        private static AstNode? ParseIsoWithOptionalBrackets(List<Token> tokens, ref int pos)
        {
            var isoText = tokens[pos].Text;
            pos++;

            if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.LBracket)
            {
                if (isoText.EndsWith("T", StringComparison.OrdinalIgnoreCase))
                {
                    var timeList = ParseTimeListAfterDatePrefix(isoText, tokens, ref pos);
                    if (timeList == null)
                        return null;
                    return ParseOptionalTimezone(tokens, ref pos, timeList);
                }

                var expansion = ParseBracketExpansion(isoText, tokens, ref pos);
                if (expansion == null)
                    return null;
                return ParseOptionalTimezone(tokens, ref pos, expansion);
            }

            AstNode node = new LiteralNode(isoText);
            return ParseOptionalTimezone(tokens, ref pos, node);
        }

        private static AstNode? ParseTimeListAfterDatePrefix(string datePrefix, List<Token> tokens, ref int pos)
        {
            if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.LBracket)
                return null;

            pos++; // consume [
            var items = new List<AstNode>();

            while (pos < tokens.Count && Peek(tokens, pos) != TokenKind.RBracket)
            {
                if (items.Count > 0)
                {
                    if (Peek(tokens, pos) != TokenKind.Comma)
                        return null;
                    pos++;
                }

                if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.IsoLiteral)
                    return null;

                var timeText = tokens[pos].Text;
                pos++;

                if (timeText.Contains('-', StringComparison.Ordinal) ||
                    timeText.StartsWith("T", StringComparison.OrdinalIgnoreCase))
                    return null;

                AstNode itemNode = new LiteralNode(datePrefix + timeText);
                itemNode = ParseOptionalTimezone(tokens, ref pos, itemNode);
                items.Add(itemNode);
            }

            if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.RBracket)
                return null;
            pos++;

            if (items.Count == 0)
                return null;

            return new ListNode(items);
        }

        private static AstNode? ParseBracketExpansion(string prefix, List<Token> tokens, ref int pos)
        {
            if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.LBracket)
                return null;

            pos++;

            var items = ParseBracketItems(tokens, ref pos);
            if (items == null) return null;

            if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.RBracket)
                return null;
            pos++;

            string suffix = "";
            BracketExpansionNode? nested = null;

            if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.Minus)
            {
                if (pos + 1 < tokens.Count && Peek(tokens, pos + 1) == TokenKind.Number)
                {
                    var numText = tokens[pos + 1].Text;
                    suffix = "-" + numText.PadLeft(2, '0');
                    pos += 2;

                    if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.Minus &&
                        pos + 1 < tokens.Count && Peek(tokens, pos + 1) == TokenKind.LBracket)
                    {
                        suffix += "-";
                        pos++;
                        nested = ParseBracketExpansionAsNested("", tokens, ref pos);
                        if (nested == null) return null;
                    }

                    if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.IsoLiteral)
                    {
                        var extra = tokens[pos].Text;
                        if (extra.StartsWith("T", StringComparison.OrdinalIgnoreCase))
                        {
                            suffix += extra;
                            pos++;
                        }
                    }
                }
                else if (pos + 1 < tokens.Count && Peek(tokens, pos + 1) == TokenKind.LBracket)
                {
                    pos++;
                    nested = ParseBracketExpansionAsNested("-", tokens, ref pos);
                    if (nested == null) return null;
                }
            }

            if (nested == null && pos < tokens.Count && Peek(tokens, pos) == TokenKind.IsoLiteral)
            {
                var trailing = tokens[pos].Text;
                if (trailing.StartsWith("T", StringComparison.OrdinalIgnoreCase))
                {
                    suffix += trailing;
                    pos++;
                }
            }

            return new BracketExpansionNode(prefix, items, suffix, nested);
        }

        private static BracketExpansionNode? ParseBracketExpansionAsNested(
            string prefix, List<Token> tokens, ref int pos)
        {
            if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.LBracket)
                return null;

            pos++;

            var items = ParseBracketItems(tokens, ref pos);
            if (items == null) return null;

            if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.RBracket)
                return null;
            pos++;

            string suffix = "";
            if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.IsoLiteral)
            {
                var trailing = tokens[pos].Text;
                if (trailing.StartsWith("T", StringComparison.OrdinalIgnoreCase))
                {
                    suffix = trailing;
                    pos++;
                }
            }

            return new BracketExpansionNode(prefix, items, suffix);
        }

        private static List<AstNode>? ParseBracketItems(List<Token> tokens, ref int pos)
        {
            var items = new List<AstNode>();

            while (pos < tokens.Count && Peek(tokens, pos) != TokenKind.RBracket)
            {
                if (items.Count > 0)
                {
                    if (Peek(tokens, pos) != TokenKind.Comma)
                        return null;
                    pos++;
                }

                var item = ParseBracketItem(tokens, ref pos);
                if (item == null) return null;
                items.Add(item);
            }

            return items.Count > 0 ? items : null;
        }

        private static AstNode? ParseBracketItem(List<Token> tokens, ref int pos)
        {
            if (pos >= tokens.Count) return null;

            var kind = Peek(tokens, pos);

            if (kind == TokenKind.Number)
            {
                int startNum = int.Parse(tokens[pos].Text, CultureInfo.InvariantCulture);
                pos++;

                if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.DotDot)
                {
                    pos++;
                    if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.Number)
                        return null;

                    int endNum = int.Parse(tokens[pos].Text, CultureInfo.InvariantCulture);
                    pos++;
                    return new NumericRangeNode(startNum, endNum);
                }

                return new LiteralNode(startNum.ToString(CultureInfo.InvariantCulture));
            }

            if (kind == TokenKind.IsoLiteral)
            {
                AstNode node = new LiteralNode(tokens[pos].Text);
                pos++;
                node = ParseOptionalTimeSuffix(tokens, ref pos, node);
                node = ParseOptionalTimezone(tokens, ref pos, node);
                return TryParseArithmetic(tokens, ref pos, node);
            }

            if (kind == TokenKind.Variable)
            {
                AstNode node = new VariableNode(tokens[pos].Text);
                pos++;
                node = ParseOptionalTimeSuffix(tokens, ref pos, node);
                node = ParseOptionalTimezone(tokens, ref pos, node);
                return TryParseArithmetic(tokens, ref pos, node);
            }

            return null;
        }

        private static AstNode? ParseBracketedRangeOrList(List<Token> tokens, ref int pos, TickContext context)
        {
            pos++; // consume [

            var contentStart = pos;

            var left = ParseAnchorExpr(tokens, ref pos);
            if (left != null && pos < tokens.Count && Peek(tokens, pos) == TokenKind.DotDot)
            {
                pos++;
                var right = ParseAnchorExpr(tokens, ref pos);
                if (right == null)
                    return null;

                if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.RBracket)
                    return null;
                pos++;

                return new RangeNode(left, right);
            }

            pos = contentStart;
            return ParseBracketedListContents(tokens, ref pos);
        }

        private static AstNode? ParseBracketedListContents(List<Token> tokens, ref int pos)
        {
            var items = new List<AstNode>();

            while (pos < tokens.Count && Peek(tokens, pos) != TokenKind.RBracket)
            {
                if (items.Count > 0)
                {
                    if (Peek(tokens, pos) != TokenKind.Comma)
                        return null;
                    pos++;
                }

                var item = ParseDateListEntry(tokens, ref pos);
                if (item == null) return null;
                items.Add(item);
            }

            if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.RBracket)
                return null;
            pos++;

            if (items.Count == 0) return null;

            return new ListNode(items);
        }

        private static AstNode? ParseDateListEntry(List<Token> tokens, ref int pos)
        {
            if (pos >= tokens.Count) return null;

            var kind = Peek(tokens, pos);

            if (kind == TokenKind.Variable)
            {
                AstNode node = new VariableNode(tokens[pos].Text);
                pos++;
                node = ParseOptionalTimeSuffix(tokens, ref pos, node);
                node = ParseOptionalTimezone(tokens, ref pos, node);
                return TryParseArithmetic(tokens, ref pos, node);
            }

            if (kind == TokenKind.IsoLiteral)
            {
                return ParseIsoWithOptionalBrackets(tokens, ref pos);
            }

            return null;
        }

        private static bool TryParseDurationFromTokens(List<Token> tokens, ref int pos, out TimeSpan dur)
        {
            dur = default;
            if (pos >= tokens.Count) return false;

            var sb = new StringBuilder();
            int startPos = pos;

            while (pos < tokens.Count)
            {
                var k = tokens[pos].Kind;
                if (k == TokenKind.DotDot || k == TokenKind.Semicolon ||
                    k == TokenKind.LBracket || k == TokenKind.RBracket ||
                    k == TokenKind.Comma || k == TokenKind.Plus || k == TokenKind.Minus ||
                    k == TokenKind.Timezone)
                    break;

                sb.Append(tokens[pos].Text);
                pos++;
            }

            if (sb.Length == 0)
            {
                pos = startPos;
                return false;
            }

            var durationText = sb.ToString();
            int dPos = 0;
            if (!ConsumeDuration(durationText, ref dPos, out dur) || dPos < durationText.Length)
            {
                pos = startPos;
                return false;
            }

            return true;
        }

        private static AstNode ParseOptionalTimezone(List<Token> tokens, ref int pos, AstNode node)
        {
            if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.Timezone)
            {
                var tz = tokens[pos].Text;
                pos++;

                if (node is RangeNode rangeNode)
                {
                    return new RangeNode(new TimezoneNode(rangeNode.Start, tz), new TimezoneNode(rangeNode.End, tz));
                }

                return new TimezoneNode(node, tz);
            }

            return node;
        }

        private static AstNode ParseOptionalTimeSuffix(List<Token> tokens, ref int pos, AstNode node)
        {
            if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.IsoLiteral)
            {
                var t = tokens[pos].Text;
                if (t.StartsWith("T", StringComparison.OrdinalIgnoreCase))
                {
                    pos++;
                    return new TimeSuffixNode(node, t);
                }
            }

            return node;
        }

        private static TokenKind? Peek(List<Token> tokens, int pos)
            => pos < tokens.Count ? tokens[pos].Kind : null;
    }

    // #######################################################################
    //  EVALUATOR - walks AST, returns half-open intervals
    // #######################################################################

    internal static class Evaluator
    {
        private readonly record struct Anchor(DateTimeOffset Value, bool IsDateOnly);
        private readonly record struct TimezoneResolution(TimeZoneInfo? Zone, TimeSpan FixedOffset, bool IsFixedOffset);

        public static List<UtcInterval> Evaluate(AstNode node, TickContext context)
        {
            var now = context.Now;
            switch (node)
            {
                case RangeNode rn:
                {
                    var starts = EvaluateAnchors(rn.Start, context);
                    var ends = EvaluateAnchors(rn.End, context);
                    var result = new List<UtcInterval>();
                    foreach (var s in starts)
                        foreach (var e in ends)
                            result.Add(new UtcInterval(s.Value, e.IsDateOnly ? EndOfDayExclusive(e.Value) : e.Value));
                    return result;
                }

                case DurationSuffixNode dsn:
                {
                    var anchors = EvaluateAnchors(dsn.Anchor, context);
                    return anchors.Select(a => new UtcInterval(a.Value, a.Value + dsn.Duration)).ToList();
                }

                case ListNode ln:
                {
                    var result = new List<UtcInterval>();
                    foreach (var item in ln.Items)
                    {
                        var sub = Evaluate(item, context);
                        result.AddRange(sub);
                    }
                    return result;
                }

                case BracketExpansionNode ben:
                {
                    var anchors = ExpandBrackets(ben, context);
                    return anchors.Select(a => new UtcInterval(a.Value, a.IsDateOnly ? EndOfDayExclusive(a.Value) : a.Value)).ToList();
                }

                case TimezoneNode tzn:
                {
                    var anchors = EvaluateAnchors(tzn, context);
                    return anchors.Select(a => new UtcInterval(a.Value, a.IsDateOnly ? EndOfDayExclusive(a.Value) : a.Value)).ToList();
                }

                default:
                {
                    var anchors = EvaluateAnchors(node, context);
                    return anchors.Select(a => new UtcInterval(a.Value, a.IsDateOnly ? EndOfDayExclusive(a.Value) : a.Value)).ToList();
                }
            }
        }

        private static List<Anchor> EvaluateAnchors(AstNode node, TickContext context)
        {
            var now = context.Now;
            switch (node)
            {
                case VariableNode vn:
                    return [ResolveVariable(vn.Name, context)];

                case LiteralNode lit:
                    if (TryParseIso(lit.Text, out var dto, out var isDateOnly))
                        return [new Anchor(dto, isDateOnly)];
                    return [];

                case ArithmeticNode an:
                {
                    var bases = EvaluateAnchors(an.Base, context);
                    var keepDateOnly = an.Duration.Ticks % TimeSpan.FromDays(1).Ticks == 0;
                    return bases.Select(b => new Anchor(
                        an.Sign == '+' ? b.Value + an.Duration : b.Value - an.Duration,
                        b.IsDateOnly && keepDateOnly)).ToList();
                }

                case ListNode ln:
                {
                    var result = new List<Anchor>();
                    foreach (var item in ln.Items)
                        result.AddRange(EvaluateAnchors(item, context));
                    return result;
                }

                case BracketExpansionNode ben:
                    return ExpandBrackets(ben, context);

                case TimezoneNode tzn:
                    return EvaluateAnchorsInTimezone(tzn.Inner, tzn.Timezone, context);

                case TimeSuffixNode tsn:
                    return ApplyTimeSuffix(EvaluateAnchors(tsn.Inner, context), tsn.TimeSuffix);

                default:
                    return [];
            }
        }

        private static List<Anchor> ExpandBrackets(BracketExpansionNode node, TickContext context)
        {
            var values = ExpandBracketItems(node.Items);
            var results = new List<Anchor>();

            foreach (var val in values)
            {
                var padded = val.ToString(CultureInfo.InvariantCulture).PadLeft(2, '0');
                var dateStr = node.Prefix + padded + node.Suffix;

                if (node.NestedExpansion != null)
                {
                    var nested = new BracketExpansionNode(
                        dateStr + node.NestedExpansion.Prefix,
                        node.NestedExpansion.Items,
                        node.NestedExpansion.Suffix,
                        node.NestedExpansion.NestedExpansion);

                    results.AddRange(ExpandBrackets(nested, context));
                }
                else
                {
                    if (TryParseIso(dateStr, out var dto, out var isDateOnly))
                        results.Add(new Anchor(dto, isDateOnly));
                }
            }

            return results;
        }

        private static List<int> ExpandBracketItems(List<AstNode> items)
        {
            var result = new List<int>();

            foreach (var item in items)
            {
                switch (item)
                {
                    case LiteralNode lit:
                        if (int.TryParse(lit.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n))
                            result.Add(n);
                        break;

                    case NumericRangeNode nrn:
                        for (int i = nrn.Start; i <= nrn.End; i++)
                            result.Add(i);
                        break;
                }
            }

            return result;
        }

        private static Anchor ResolveVariable(string name, TickContext context)
        {
            var now = context.Now;
            return name switch
            {
                "$now" => new Anchor(now, false),
                "$today" => new Anchor(new DateTimeOffset(now.Date, now.Offset), true),
                "$yesterday" => new Anchor(new DateTimeOffset(now.Date.AddDays(-1), now.Offset), true),
                "$tomorrow" => new Anchor(new DateTimeOffset(now.Date.AddDays(1), now.Offset), true),
                "$start" => context.FirstTimestamp.HasValue
                    ? new Anchor(context.FirstTimestamp.Value, false)
                    : throw new TickCompileException("$start requires FirstTimestamp in context."),
                "$first" => context.FirstTimestamp.HasValue
                    ? new Anchor(context.FirstTimestamp.Value, false)
                    : throw new TickCompileException("$first requires FirstTimestamp in context."),
                "$end" => context.LatestTimestamp.HasValue
                    ? new Anchor(context.LatestTimestamp.Value, false)
                    : throw new TickCompileException("$end requires LatestTimestamp in context."),
                "$latest" => context.LatestTimestamp.HasValue
                    ? new Anchor(context.LatestTimestamp.Value, false)
                    : throw new TickCompileException("$latest requires LatestTimestamp in context."),
                _ when name.StartsWith('$') => throw new TickCompileException($"Unknown variable '{name}'."),
                _ => throw new TickCompileException($"Unknown token '{name}'.")
            };
        }

        private static bool TryParseIso(string text, out DateTimeOffset result, out bool isDateOnly)
        {
            isDateOnly = !text.Contains('T', StringComparison.OrdinalIgnoreCase);
            return DateTimeOffset.TryParseExact(
                text, DateFormats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AllowWhiteSpaces,
                out result);
        }

        /// <summary>Half-open: returns midnight of the next day.</summary>
        private static DateTimeOffset EndOfDayExclusive(DateTimeOffset startOfDay)
            => new DateTimeOffset(startOfDay.Year, startOfDay.Month, startOfDay.Day, 0, 0, 0, startOfDay.Offset).AddDays(1);

        private static List<Anchor> EvaluateAnchorsInTimezone(AstNode inner, string timezoneToken, TickContext context)
        {
            var now = context.Now;
            if (!TryResolveTimezone(timezoneToken, out var resolution))
                return [];

            if (inner is TimeSuffixNode tsn)
                return ApplyTimeSuffixInTimezone(EvaluateAnchorsInTimezone(tsn.Inner, timezoneToken, context), tsn.TimeSuffix, resolution);

            if (inner is VariableNode vn)
            {
                var zonedNow = ConvertInstantToTimezone(now, resolution);
                var localStart = new DateTime(zonedNow.Year, zonedNow.Month, zonedNow.Day, 0, 0, 0, DateTimeKind.Unspecified);
                return vn.Name switch
                {
                    "$now" => [new Anchor(zonedNow, false)],
                    "$today" => [new Anchor(ConvertLocalDateTimeToTimezone(localStart, resolution), true)],
                    "$yesterday" => [new Anchor(ConvertLocalDateTimeToTimezone(localStart.AddDays(-1), resolution), true)],
                    "$tomorrow" => [new Anchor(ConvertLocalDateTimeToTimezone(localStart.AddDays(1), resolution), true)],
                    "$start" => context.FirstTimestamp.HasValue
                        ? [new Anchor(ConvertInstantToTimezone(context.FirstTimestamp.Value, resolution), false)]
                        : throw new TickCompileException("$start requires FirstTimestamp in context."),
                    "$first" => context.FirstTimestamp.HasValue
                        ? [new Anchor(ConvertInstantToTimezone(context.FirstTimestamp.Value, resolution), false)]
                        : throw new TickCompileException("$first requires FirstTimestamp in context."),
                    "$end" => context.LatestTimestamp.HasValue
                        ? [new Anchor(ConvertInstantToTimezone(context.LatestTimestamp.Value, resolution), false)]
                        : throw new TickCompileException("$end requires LatestTimestamp in context."),
                    "$latest" => context.LatestTimestamp.HasValue
                        ? [new Anchor(ConvertInstantToTimezone(context.LatestTimestamp.Value, resolution), false)]
                        : throw new TickCompileException("$latest requires LatestTimestamp in context."),
                    _ when vn.Name.StartsWith('$') => throw new TickCompileException($"Unknown variable '{vn.Name}'."),
                    _ => throw new TickCompileException($"Unknown token '{vn.Name}'.")
                };
            }

            if (inner is LiteralNode lit)
            {
                if (!TryParseIso(lit.Text, out var parsed, out var isDateOnly))
                    return [];

                if (isDateOnly)
                {
                    var localMidnight = new DateTime(parsed.Year, parsed.Month, parsed.Day, 0, 0, 0, DateTimeKind.Unspecified);
                    var zonedMidnight = ConvertLocalDateTimeToTimezone(localMidnight, resolution);
                    return [new Anchor(zonedMidnight, true)];
                }

                if (HasExplicitOffset(lit.Text))
                    return [new Anchor(ConvertInstantToTimezone(parsed, resolution), false)];

                var localDateTime = new DateTime(parsed.Year, parsed.Month, parsed.Day, parsed.Hour, parsed.Minute, parsed.Second, DateTimeKind.Unspecified)
                    .AddTicks(parsed.TimeOfDay.Ticks % TimeSpan.TicksPerSecond);
                return [new Anchor(ConvertLocalDateTimeToTimezone(localDateTime, resolution), false)];
            }

            var baseAnchors = EvaluateAnchors(inner, context);
            return baseAnchors.Select(a => new Anchor(ConvertInstantToTimezone(a.Value, resolution), a.IsDateOnly)).ToList();
        }

        private static List<Anchor> ApplyTimeSuffix(List<Anchor> anchors, string timeSuffix)
        {
            if (!TryParseTimeSuffix(timeSuffix, out var timeOfDay))
                return [];

            return anchors.Select(a => new Anchor(ApplyTimeOfDay(a.Value, timeOfDay), false)).ToList();
        }

        private static List<Anchor> ApplyTimeSuffixInTimezone(
            List<Anchor> anchors,
            string timeSuffix,
            TimezoneResolution resolution)
        {
            if (!TryParseTimeSuffix(timeSuffix, out var timeOfDay))
                return [];

            var result = new List<Anchor>(anchors.Count);
            foreach (var anchor in anchors)
            {
                var localDateTime = new DateTime(anchor.Value.Year, anchor.Value.Month, anchor.Value.Day, 0, 0, 0, DateTimeKind.Unspecified)
                    .Add(timeOfDay);
                result.Add(new Anchor(ConvertLocalDateTimeToTimezone(localDateTime, resolution), false));
            }

            return result;
        }

        private static bool TryParseTimeSuffix(string timeSuffix, out TimeSpan timeOfDay)
        {
            timeOfDay = default;
            if (string.IsNullOrWhiteSpace(timeSuffix) || !timeSuffix.StartsWith("T", StringComparison.OrdinalIgnoreCase))
                return false;

            var raw = timeSuffix[1..];
            if (TimeSpan.TryParse(raw, CultureInfo.InvariantCulture, out var parsed))
            {
                timeOfDay = parsed;
                return true;
            }

            return false;
        }

        private static DateTimeOffset ApplyTimeOfDay(DateTimeOffset anchor, TimeSpan timeOfDay)
        {
            var localDateTime = new DateTime(anchor.Year, anchor.Month, anchor.Day, 0, 0, 0, DateTimeKind.Unspecified)
                .Add(timeOfDay);
            return new DateTimeOffset(localDateTime, anchor.Offset);
        }

        private static bool TryResolveTimezone(string timezoneToken, out TimezoneResolution resolution)
        {
            resolution = default;
            var tz = timezoneToken.Trim();
            if (tz.StartsWith("@", StringComparison.Ordinal))
                tz = tz[1..];

            if (tz.Equals("UTC", StringComparison.OrdinalIgnoreCase) ||
                tz.Equals("Z", StringComparison.OrdinalIgnoreCase))
            {
                resolution = new TimezoneResolution(null, TimeSpan.Zero, true);
                return true;
            }

            if (tz.Equals("local", StringComparison.OrdinalIgnoreCase))
            {
                resolution = new TimezoneResolution(TimeZoneInfo.Local, default, false);
                return true;
            }

            if (Regex.IsMatch(tz, @"^[+-]\d{2}:\d{2}$", RegexOptions.CultureInvariant))
            {
                if (TimeSpan.TryParseExact(tz[1..], @"hh\:mm", CultureInfo.InvariantCulture, out var parsedColon))
                {
                    var fixedOffset = tz[0] == '-' ? -parsedColon : parsedColon;
                    resolution = new TimezoneResolution(null, fixedOffset, true);
                    return true;
                }
            }

            if (Regex.IsMatch(tz, @"^[+-]\d{2}$", RegexOptions.CultureInvariant))
            {
                if (int.TryParse(tz, NumberStyles.Integer, CultureInfo.InvariantCulture, out var h))
                {
                    resolution = new TimezoneResolution(null, TimeSpan.FromHours(h), true);
                    return true;
                }
            }

            if (Regex.IsMatch(tz, @"^[+-]\d{4}$", RegexOptions.CultureInvariant))
            {
                var sign = tz[0] == '-' ? -1 : 1;
                if (int.TryParse(tz.Substring(1, 2), NumberStyles.Integer, CultureInfo.InvariantCulture, out var hh) &&
                    int.TryParse(tz.Substring(3, 2), NumberStyles.Integer, CultureInfo.InvariantCulture, out var mm))
                {
                    resolution = new TimezoneResolution(null, new TimeSpan(sign * hh, sign * mm, 0), true);
                    return true;
                }
            }

            if (TryGetTimeZoneInfo(tz, out var zone))
            {
                resolution = new TimezoneResolution(zone, default, false);
                return true;
            }

            return false;
        }

        private static bool TryGetTimeZoneInfo(string zoneId, out TimeZoneInfo zone)
        {
            try
            {
                zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
                return true;
            }
            catch (TimeZoneNotFoundException) { }
            catch (InvalidTimeZoneException) { }

            try
            {
                zone = TZConvert.GetTimeZoneInfo(zoneId);
                return true;
            }
            catch (TimeZoneNotFoundException) { }
            catch (InvalidTimeZoneException) { }

            zone = default!;
            return false;
        }

        private static DateTimeOffset ConvertInstantToTimezone(DateTimeOffset instant, TimezoneResolution resolution)
        {
            if (resolution.IsFixedOffset)
                return instant.ToOffset(resolution.FixedOffset);

            return TimeZoneInfo.ConvertTime(instant, resolution.Zone!);
        }

        private static DateTimeOffset ConvertLocalDateTimeToTimezone(DateTime localDateTime, TimezoneResolution resolution)
        {
            if (resolution.IsFixedOffset)
                return new DateTimeOffset(localDateTime, resolution.FixedOffset);

            var zone = resolution.Zone!;
            var normalized = localDateTime;

            while (zone.IsInvalidTime(normalized))
            {
                normalized = normalized.AddMinutes(1);
            }

            if (zone.IsAmbiguousTime(normalized))
            {
                // Pick the smaller offset (post-transition / standard time).
                var ambiguous = zone.GetAmbiguousTimeOffsets(normalized);
                var preferred = ambiguous[0] <= ambiguous[1] ? ambiguous[0] : ambiguous[1];
                return new DateTimeOffset(normalized, preferred);
            }

            return new DateTimeOffset(normalized, zone.GetUtcOffset(normalized));
        }

        private static bool HasExplicitOffset(string text)
        {
            if (text.EndsWith("Z", StringComparison.OrdinalIgnoreCase))
                return true;

            var tIndex = text.IndexOf('T');
            if (tIndex < 0)
                return false;

            for (int i = tIndex + 1; i < text.Length; i++)
            {
                if (text[i] == '+' || text[i] == '-')
                    return true;
            }

            return false;
        }
    }
}
