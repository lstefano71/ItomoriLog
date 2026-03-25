using FluentAssertions;

using ItomoriLog.App;

namespace ItomoriLog.Tests;

public class BrandTests
{
    [Fact]
    public void Brand_AppName_IsItomoriLog()
    {
        Brand.AppName.Should().Be("ItomoriLog");
    }

    [Fact]
    public void Brand_DefaultSessionsRoot_ContainsAppName()
    {
        Brand.DefaultSessionsRoot.Should().Contain("ItomoriLog");
    }

    [Fact]
    public void Brand_AppVersion_IsSemVer()
    {
        Version.TryParse(Brand.AppVersion, out _).Should().BeTrue();
    }
}
