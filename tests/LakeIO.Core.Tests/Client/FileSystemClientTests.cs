using Azure.Storage.Files.DataLake;
using FluentAssertions;
using LakeIO.Tests.Helpers;
using NSubstitute;
using Xunit;

namespace LakeIO.Tests.Client;

public class FileSystemClientTests
{
    private readonly DataLakeFileSystemClient _mockAzureClient;
    private readonly LakeClientOptions _options;
    private readonly FileSystemClient _sut;

    public FileSystemClientTests()
    {
        _mockAzureClient = MockHelpers.CreateMockFileSystemClient("my-filesystem");
        _mockAzureClient.Uri.Returns(new Uri("https://account.dfs.core.windows.net/my-filesystem"));
        _options = new LakeClientOptions();
        _sut = new FileSystemClient(_mockAzureClient, _options);
    }

    [Fact]
    public void Name_ReturnsAzureClientName()
    {
        _sut.Name.Should().Be("my-filesystem");
    }

    [Fact]
    public void Uri_ReturnsAzureClientUri()
    {
        _sut.Uri.Should().Be(new Uri("https://account.dfs.core.windows.net/my-filesystem"));
    }

    [Fact]
    public void GetDirectoryClient_ReturnsDirectoryClient()
    {
        var mockDirClient = Substitute.For<DataLakeDirectoryClient>();
        _mockAzureClient.GetDirectoryClient("my-dir").Returns(mockDirClient);

        var result = _sut.GetDirectoryClient("my-dir");

        result.Should().NotBeNull();
        result.Should().BeOfType<DirectoryClient>();
    }

    [Fact]
    public void GetDirectoryClient_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.GetDirectoryClient(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetFileClient_ReturnsFileClient()
    {
        var mockFileClient = Substitute.For<DataLakeFileClient>();
        _mockAzureClient.GetFileClient("data/test.json").Returns(mockFileClient);

        var result = _sut.GetFileClient("data/test.json");

        result.Should().NotBeNull();
        result.Should().BeOfType<FileClient>();
    }

    [Fact]
    public void GetFileClient_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.GetFileClient(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetOrCreateOperations_CachesInstance()
    {
        var callCount = 0;
        Func<DataLakeFileSystemClient, LakeClientOptions, string> factory = (_, _) =>
        {
            callCount++;
            return "created-instance";
        };

        var first = _sut.GetOrCreateOperations(factory);
        var second = _sut.GetOrCreateOperations(factory);

        first.Should().BeSameAs(second);
        callCount.Should().Be(1, "factory should be called only once; second call should return cached instance");
    }

    [Fact]
    public void GetOrCreateOperations_DifferentTypes_CreatesSeparateInstances()
    {
        var stringResult = _sut.GetOrCreateOperations<string>((_, _) => "string-ops");
        var listResult = _sut.GetOrCreateOperations<List<int>>((_, _) => new List<int> { 1, 2, 3 });

        stringResult.Should().Be("string-ops");
        listResult.Should().BeEquivalentTo(new List<int> { 1, 2, 3 });
    }

    [Fact]
    public void AzureClient_ReturnsUnderlyingClient()
    {
        _sut.AzureClient.Should().BeSameAs(_mockAzureClient);
    }

    [Fact]
    public void Options_ReturnsProvidedOptions()
    {
        _sut.Options.Should().BeSameAs(_options);
    }
}
