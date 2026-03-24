namespace ItomoriLog.Core.Query;

/// <summary>
/// The output of QueryPlanner: parameterized SQL ready for execution.
/// </summary>
public sealed record QueryResult(
    string Sql,
    IReadOnlyList<object> Parameters,
    PageDirection Direction,
    string? SetupSql = null);
