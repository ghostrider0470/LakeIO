using Azure.Core;
using FluentAssertions;
using NSubstitute;
using Xunit;

namespace LakeIO.Tests.Client;

public class LakeClientTests
{
    // Azurite-compatible connection string (never hits real Azure)
    private const string ValidConnectionString =
        "DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "EndpointSuffix=core.windows.net";

    [Fact]
    public void Constructor_WithValidConnectionString_DoesNotThrow()
    {
        var act = () => new LakeClient(ValidConnectionString);

        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_WithNullConnectionString_ThrowsArgumentException()
    {
        var act = () => new LakeClient((string)null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithEmptyConnectionString_ThrowsArgumentException()
    {
        var act = () => new LakeClient(string.Empty);

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Constructor_WithWhitespaceConnectionString_ThrowsArgumentException()
    {
        var act = () => new LakeClient("   ");

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Constructor_WithTokenCredential_DoesNotThrow()
    {
        var credential = Substitute.For<TokenCredential>();
        var uri = new Uri("https://devstoreaccount1.dfs.core.windows.net");

        var act = () => new LakeClient(uri, credential);

        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_WithNullUri_ThrowsArgumentNullException()
    {
        var credential = Substitute.For<TokenCredential>();

        var act = () => new LakeClient((Uri)null!, credential);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithNullCredential_ThrowsArgumentNullException()
    {
        var uri = new Uri("https://devstoreaccount1.dfs.core.windows.net");

        var act = () => new LakeClient(uri, (TokenCredential)null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        var act = () => new LakeClient(ValidConnectionString, null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetFileSystemClient_ReturnsFileSystemClient()
    {
        var client = new LakeClient(ValidConnectionString);

        var fs = client.GetFileSystemClient("test");

        fs.Should().NotBeNull();
        fs.Should().BeOfType<FileSystemClient>();
    }

    [Fact]
    public void GetFileSystemClient_WithNullName_ThrowsArgumentException()
    {
        var client = new LakeClient(ValidConnectionString);

        var act = () => client.GetFileSystemClient(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetFileSystemClient_WithEmptyName_ThrowsArgumentException()
    {
        var client = new LakeClient(ValidConnectionString);

        var act = () => client.GetFileSystemClient(string.Empty);

        act.Should().Throw<ArgumentException>();
    }
}
