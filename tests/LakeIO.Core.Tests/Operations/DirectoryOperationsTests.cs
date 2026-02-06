using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using FluentAssertions;
using LakeIO.Tests.Helpers;
using Xunit;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace LakeIO.Tests.Operations;

public class DirectoryOperationsTests
{
    private readonly DataLakeFileSystemClient _mockFsClient;
    private readonly LakeClientOptions _options;
    private readonly DirectoryOperations _sut;

    public DirectoryOperationsTests()
    {
        _mockFsClient = MockHelpers.CreateMockFileSystemClient();
        _options = new LakeClientOptions();
        _sut = new DirectoryOperations(_mockFsClient, _options);
    }

    // ── GetPathsAsync ───────────────────────────────────────────────────

    [Fact]
    public async Task GetPathsAsync_YieldsPathItems()
    {
        var azureItems = new[]
        {
            DataLakeModelFactory.PathItem("folder/file1.json", false, DateTimeOffset.UtcNow, default, 100, null, null, null),
            DataLakeModelFactory.PathItem("folder/file2.json", false, DateTimeOffset.UtcNow, default, 200, null, null, null),
            DataLakeModelFactory.PathItem("folder/subfolder", true, DateTimeOffset.UtcNow, default, 0, null, null, null),
        };

        SetupGetPathsMock(azureItems);

        var results = new List<PathItem>();
        await foreach (var item in _sut.GetPathsAsync())
        {
            results.Add(item);
        }

        results.Should().HaveCount(3);
        results[0].Name.Should().Be("folder/file1.json");
        results[0].IsDirectory.Should().BeFalse();
        results[0].ContentLength.Should().Be(100);
        results[1].Name.Should().Be("folder/file2.json");
        results[2].Name.Should().Be("folder/subfolder");
        results[2].IsDirectory.Should().BeTrue();
    }

    [Fact]
    public async Task GetPathsAsync_AppliesClientSideFilter()
    {
        var azureItems = new[]
        {
            DataLakeModelFactory.PathItem("data/file1.json", false, DateTimeOffset.UtcNow, default, 100, null, null, null),
            DataLakeModelFactory.PathItem("data/file2.csv", false, DateTimeOffset.UtcNow, default, 200, null, null, null),
            DataLakeModelFactory.PathItem("data/file3.json", false, DateTimeOffset.UtcNow, default, 300, null, null, null),
        };

        SetupGetPathsMock(azureItems);

        var options = new GetPathsOptions
        {
            Filter = new PathFilter().WithExtension(".json")
        };

        var results = new List<PathItem>();
        await foreach (var item in _sut.GetPathsAsync(options))
        {
            results.Add(item);
        }

        results.Should().HaveCount(2);
        results.Should().OnlyContain(i => i.Name.EndsWith(".json"));
    }

    [Fact]
    public async Task GetPathsAsync_WithNullOptions_UsesDefaults()
    {
        var azureItems = new[]
        {
            DataLakeModelFactory.PathItem("file.txt", false, DateTimeOffset.UtcNow, default, 50, null, null, null),
        };

        SetupGetPathsMock(azureItems);

        var results = new List<PathItem>();
        await foreach (var item in _sut.GetPathsAsync(null))
        {
            results.Add(item);
        }

        results.Should().HaveCount(1);

        // Verify server-side defaults: null path, recursive=false
        _mockFsClient.Received(1).GetPathsAsync(
            null,
            false,
            false,
            Arg.Any<CancellationToken>());
    }

    // ── CountAsync ──────────────────────────────────────────────────────

    [Fact]
    public async Task CountAsync_ReturnsCorrectCount()
    {
        var azureItems = new[]
        {
            DataLakeModelFactory.PathItem("a.json", false, DateTimeOffset.UtcNow, default, 10, null, null, null),
            DataLakeModelFactory.PathItem("b.json", false, DateTimeOffset.UtcNow, default, 20, null, null, null),
            DataLakeModelFactory.PathItem("c.json", false, DateTimeOffset.UtcNow, default, 30, null, null, null),
            DataLakeModelFactory.PathItem("d.json", false, DateTimeOffset.UtcNow, default, 40, null, null, null),
            DataLakeModelFactory.PathItem("e.json", false, DateTimeOffset.UtcNow, default, 50, null, null, null),
        };

        SetupGetPathsMock(azureItems);

        var count = await _sut.CountAsync();

        count.Should().Be(5);
    }

    [Fact]
    public async Task CountAsync_WithFilter_CountsOnlyMatchingItems()
    {
        var azureItems = new[]
        {
            DataLakeModelFactory.PathItem("a.json", false, DateTimeOffset.UtcNow, default, 10, null, null, null),
            DataLakeModelFactory.PathItem("b.csv", false, DateTimeOffset.UtcNow, default, 20, null, null, null),
            DataLakeModelFactory.PathItem("c.json", false, DateTimeOffset.UtcNow, default, 30, null, null, null),
        };

        SetupGetPathsMock(azureItems);

        var options = new GetPathsOptions
        {
            Filter = new PathFilter().WithExtension(".json")
        };

        var count = await _sut.CountAsync(options);

        count.Should().Be(2);
    }

    // ── GetPropertiesAsync ──────────────────────────────────────────────

    [Fact]
    public async Task GetPropertiesAsync_ReturnsFileProperties()
    {
        var fileClient = MockHelpers.CreateMockFileClient("folder/test.json");

        var azureProperties = DataLakeModelFactory.PathProperties(
            lastModified: DateTimeOffset.UtcNow,
            creationTime: DateTimeOffset.UtcNow.AddDays(-1),
            metadata: new Dictionary<string, string> { ["key"] = "value" },
            copyCompletionTime: default,
            copyStatusDescription: null,
            copyId: null,
            copyProgress: null,
            copySource: null,
            copyStatus: CopyStatus.Success,
            isIncrementalCopy: false,
            leaseDuration: DataLakeLeaseDuration.Infinite,
            leaseState: DataLakeLeaseState.Available,
            leaseStatus: DataLakeLeaseStatus.Unlocked,
            contentLength: 1234,
            contentType: "application/json",
            eTag: new ETag("\"test-etag\""),
            contentHash: null,
            contentEncoding: null,
            contentDisposition: null,
            contentLanguage: null,
            cacheControl: null,
            acceptRanges: null,
            isServerEncrypted: false,
            encryptionKeySha256: null,
            accessTier: null,
            archiveStatus: null,
            accessTierChangeTime: default);

        var rawPropsResponse = MockHelpers.CreateMockRawResponse();
        var propsResponse = Azure.Response.FromValue(azureProperties, rawPropsResponse);
        fileClient.GetPropertiesAsync(cancellationToken: Arg.Any<CancellationToken>())
            .Returns(propsResponse);

        _mockFsClient.GetFileClient("folder/test.json").Returns(fileClient);

        var result = await _sut.GetPropertiesAsync("folder/test.json");

        result.Value.Should().NotBeNull();
        result.Value.ContentLength.Should().Be(1234);
        result.Value.ContentType.Should().Be("application/json");
        result.Value.IsDirectory.Should().BeFalse();
        result.Value.Metadata.Should().ContainKey("key");
    }

    [Fact]
    public async Task GetPropertiesAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.GetPropertiesAsync(null!);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task GetPropertiesAsync_WithEmptyPath_ThrowsArgumentException()
    {
        var act = () => _sut.GetPropertiesAsync(string.Empty);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task GetPropertiesAsync_WithWhitespacePath_ThrowsArgumentException()
    {
        var act = () => _sut.GetPropertiesAsync("   ");

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private void SetupGetPathsMock(
        Azure.Storage.Files.DataLake.Models.PathItem[] items)
    {
        var rawResponse = MockHelpers.CreateMockRawResponse();
        var page = Azure.Page<Azure.Storage.Files.DataLake.Models.PathItem>.FromValues(
            items,
            continuationToken: null,
            rawResponse);
        var pageable = AsyncPageable<Azure.Storage.Files.DataLake.Models.PathItem>.FromPages(
            new[] { page });

        _mockFsClient.GetPathsAsync(
                Arg.Any<string?>(),
                Arg.Any<bool>(),
                Arg.Any<bool>(),
                Arg.Any<CancellationToken>())
            .Returns(pageable);
    }
}
