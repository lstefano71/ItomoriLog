namespace ItomoriLog.Core.Query;

public interface ITickCompiler
{
    TickCompileResult Compile(string input, TickContext context);
}
