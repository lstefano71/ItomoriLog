namespace ItomoriLog.Core.Query;

public sealed record SqlEmission(
    string WhereSql,
    IReadOnlyList<object> Parameters,
    string? SetupSql = null);
