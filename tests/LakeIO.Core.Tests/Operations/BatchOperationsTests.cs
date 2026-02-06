using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using FluentAssertions;
using LakeIO.Tests.Helpers;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace LakeIO.Tests.Operations;

public class BatchOperationsTests
{
    private readonly DataLakeFileSystemClient _mockFsClient;
    private readonly LakeClientOptions _options;
    private readonly BatchOperations _sut;

    public BatchOperationsTests()
    {
        _mockFsClient = MockHelpers.CreateMockFileSystemClient();
        _options = new LakeClientOptions();
        _sut = new BatchOperations(_mockFsClient, _options);
    }

    // ── DeleteAsync ─────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteAsync_DeletesAllFiles_ReturnsSuccessResult()
    {
        var paths = new[] { "file1.json", "file2.json", "file3.json" };

        foreach (var path in paths)
        {
            var rawResponse = MockHelpers.CreateMockRawResponse();
            var fileClient = MockHelpers.CreateMockFileClient(path);
            fileClient.DeleteAsync(cancellationToken: Arg.Any<CancellationToken>())
                .Returns(rawResponse);
            _mockFsClient.GetFileClient(path).Returns(fileClient);
        }

        var result = await _sut.DeleteAsync(paths);

        result.TotalCount.Should().Be(3);
        result.SucceededCount.Should().Be(3);
        result.FailedCount.Should().Be(0);
        result.IsFullySuccessful.Should().BeTrue();
        result.Items.Should().HaveCount(3);
        result.Items.Should().OnlyContain(i => i.Succeeded);
    }

    [Fact]
    public async Task DeleteAsync_PartialFailure_CollectsErrors()
    {
        var paths = new[] { "file1.json", "file2.json", "file3.json" };

        // First and third succeed
        var rawResponse1 = MockHelpers.CreateMockRawResponse();
        var fileClient1 = MockHelpers.CreateMockFileClient("file1.json");
        fileClient1.DeleteAsync(cancellationToken: Arg.Any<CancellationToken>())
            .Returns(rawResponse1);
        _mockFsClient.GetFileClient("file1.json").Returns(fileClient1);

        // Second fails
        var fileClient2 = MockHelpers.CreateMockFileClient("file2.json");
        fileClient2.DeleteAsync(cancellationToken: Arg.Any<CancellationToken>())
            .ThrowsAsync(new RequestFailedException("Not found"));
        _mockFsClient.GetFileClient("file2.json").Returns(fileClient2);

        var rawResponse3 = MockHelpers.CreateMockRawResponse();
        var fileClient3 = MockHelpers.CreateMockFileClient("file3.json");
        fileClient3.DeleteAsync(cancellationToken: Arg.Any<CancellationToken>())
            .Returns(rawResponse3);
        _mockFsClient.GetFileClient("file3.json").Returns(fileClient3);

        var result = await _sut.DeleteAsync(paths);

        result.TotalCount.Should().Be(3);
        result.SucceededCount.Should().Be(2);
        result.FailedCount.Should().Be(1);
        result.IsFullySuccessful.Should().BeFalse();

        var failedItem = result.Items.Single(i => !i.Succeeded);
        failedItem.Path.Should().Be("file2.json");
        failedItem.Error.Should().NotBeNullOrWhiteSpace();
        failedItem.Exception.Should().BeOfType<RequestFailedException>();
    }

    [Fact]
    public async Task DeleteAsync_EmptyList_ReturnsEmptyResult()
    {
        var result = await _sut.DeleteAsync(Array.Empty<string>());

        result.TotalCount.Should().Be(0);
        result.SucceededCount.Should().Be(0);
        result.FailedCount.Should().Be(0);
        result.IsFullySuccessful.Should().BeTrue();
        result.Items.Should().BeEmpty();
    }

    [Fact]
    public async Task DeleteAsync_WithNullPaths_ThrowsArgumentNullException()
    {
        var act = () => _sut.DeleteAsync(null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task DeleteAsync_ReportsProgress()
    {
        var paths = new[] { "file1.json", "file2.json" };
        var progressReports = new List<BatchProgress>();
        var progress = Substitute.For<IProgress<BatchProgress>>();
        progress.When(p => p.Report(Arg.Any<BatchProgress>()))
            .Do(ci => progressReports.Add(ci.Arg<BatchProgress>()));

        foreach (var path in paths)
        {
            var rawResponse = MockHelpers.CreateMockRawResponse();
            var fileClient = MockHelpers.CreateMockFileClient(path);
            fileClient.DeleteAsync(cancellationToken: Arg.Any<CancellationToken>())
                .Returns(rawResponse);
            _mockFsClient.GetFileClient(path).Returns(fileClient);
        }

        await _sut.DeleteAsync(paths, progress);

        progressReports.Should().HaveCount(2);
        progressReports[0].Completed.Should().Be(1);
        progressReports[0].Total.Should().Be(2);
        progressReports[0].CurrentPath.Should().Be("file1.json");
        progressReports[1].Completed.Should().Be(2);
        progressReports[1].Total.Should().Be(2);
        progressReports[1].CurrentPath.Should().Be("file2.json");
    }

    // ── MoveAsync ───────────────────────────────────────────────────────

    [Fact]
    public async Task MoveAsync_MovesAllFiles_ReturnsSuccessResult()
    {
        var items = new[]
        {
            new BatchMoveItem { SourcePath = "src/a.json", DestinationPath = "dst/a.json" },
            new BatchMoveItem { SourcePath = "src/b.json", DestinationPath = "dst/b.json" },
        };

        foreach (var item in items)
        {
            SetupMoveMock(item.SourcePath, succeeds: true);
        }

        var result = await _sut.MoveAsync(items);

        result.TotalCount.Should().Be(2);
        result.SucceededCount.Should().Be(2);
        result.FailedCount.Should().Be(0);
        result.IsFullySuccessful.Should().BeTrue();
    }

    [Fact]
    public async Task MoveAsync_PartialFailure_CollectsErrors()
    {
        var items = new[]
        {
            new BatchMoveItem { SourcePath = "src/a.json", DestinationPath = "dst/a.json" },
            new BatchMoveItem { SourcePath = "src/b.json", DestinationPath = "dst/b.json" },
        };

        // First succeeds
        SetupMoveMock("src/a.json", succeeds: true);

        // Second fails
        SetupMoveMock("src/b.json", succeeds: false);

        var result = await _sut.MoveAsync(items);

        result.TotalCount.Should().Be(2);
        result.SucceededCount.Should().Be(1);
        result.FailedCount.Should().Be(1);
        result.IsFullySuccessful.Should().BeFalse();

        var failedItem = result.Items.Single(i => !i.Succeeded);
        failedItem.Path.Should().Be("src/b.json");
        failedItem.Error.Should().Contain("Conflict");
    }

    [Fact]
    public async Task MoveAsync_WithNullItems_ThrowsArgumentNullException()
    {
        var act = () => _sut.MoveAsync(null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    // ── CopyAsync ───────────────────────────────────────────────────────

    [Fact]
    public async Task CopyAsync_CopiesAllFiles_ReturnsSuccessResult()
    {
        var items = new[]
        {
            new BatchCopyItem { SourcePath = "src/a.json", DestinationPath = "dst/a.json" },
            new BatchCopyItem { SourcePath = "src/b.json", DestinationPath = "dst/b.json" },
        };

        foreach (var item in items)
        {
            SetupCopyMocks(item.SourcePath, item.DestinationPath);
        }

        var result = await _sut.CopyAsync(items);

        result.TotalCount.Should().Be(2);
        result.SucceededCount.Should().Be(2);
        result.FailedCount.Should().Be(0);
        result.IsFullySuccessful.Should().BeTrue();
    }

    [Fact]
    public async Task CopyAsync_PartialFailure_CollectsErrors()
    {
        var items = new[]
        {
            new BatchCopyItem { SourcePath = "src/a.json", DestinationPath = "dst/a.json" },
            new BatchCopyItem { SourcePath = "src/b.json", DestinationPath = "dst/b.json" },
        };

        // First copy succeeds
        SetupCopyMocks("src/a.json", "dst/a.json");

        // Second copy: source read fails
        var failSrcClient = MockHelpers.CreateMockFileClient("src/b.json");
        failSrcClient.ReadStreamingAsync(Arg.Any<CancellationToken>())
            .ThrowsAsync(new RequestFailedException("Not found"));
        _mockFsClient.GetFileClient("src/b.json").Returns(failSrcClient);

        var result = await _sut.CopyAsync(items);

        result.TotalCount.Should().Be(2);
        result.SucceededCount.Should().Be(1);
        result.FailedCount.Should().Be(1);
        result.IsFullySuccessful.Should().BeFalse();

        var failedItem = result.Items.Single(i => !i.Succeeded);
        failedItem.Path.Should().Be("src/b.json");
    }

    [Fact]
    public async Task CopyAsync_WithNullItems_ThrowsArgumentNullException()
    {
        var act = () => _sut.CopyAsync(null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private void SetupMoveMock(string sourcePath, bool succeeds)
    {
        var fileClient = MockHelpers.CreateMockFileClient(sourcePath);

        if (succeeds)
        {
            var mockResponse = Substitute.For<Azure.Response<DataLakeFileClient>>();
            fileClient.RenameAsync(
                    Arg.Any<string>(),
                    Arg.Any<string>(),
                    Arg.Any<DataLakeRequestConditions>(),
                    Arg.Any<DataLakeRequestConditions>(),
                    Arg.Any<CancellationToken>())
                .Returns(mockResponse);
        }
        else
        {
            fileClient.RenameAsync(
                    Arg.Any<string>(),
                    Arg.Any<string>(),
                    Arg.Any<DataLakeRequestConditions>(),
                    Arg.Any<DataLakeRequestConditions>(),
                    Arg.Any<CancellationToken>())
                .ThrowsAsync(new RequestFailedException("Conflict"));
        }

        _mockFsClient.GetFileClient(sourcePath).Returns(fileClient);
    }

    private void SetupCopyMocks(string sourcePath, string destPath)
    {
        // Source: return a stream from ReadStreamingAsync
        var sourceFileClient = MockHelpers.CreateMockFileClient(sourcePath);
        var content = new MemoryStream(new byte[] { 1, 2, 3 });
        var details = DataLakeModelFactory.FileDownloadDetails(
            lastModified: DateTimeOffset.UtcNow,
            metadata: new Dictionary<string, string>(),
            contentRange: null,
            eTag: new ETag("\"test\""),
            contentEncoding: null,
            cacheControl: null,
            contentDisposition: null,
            contentLanguage: null,
            copyCompletionTime: default,
            copyStatusDescription: null,
            copyId: null,
            copyProgress: null,
            copySource: null,
            copyStatus: CopyStatus.Success,
            leaseDuration: DataLakeLeaseDuration.Infinite,
            leaseState: DataLakeLeaseState.Available,
            leaseStatus: DataLakeLeaseStatus.Unlocked,
            acceptRanges: null,
            isServerEncrypted: false,
            encryptionKeySha256: null,
            contentHash: null);
        var streamingResult = DataLakeModelFactory.DataLakeFileReadStreamingResult(content, details);
        var rawResponse = MockHelpers.CreateMockRawResponse();
        var downloadResponse = Azure.Response.FromValue(streamingResult, rawResponse);
        sourceFileClient.ReadStreamingAsync(Arg.Any<CancellationToken>())
            .Returns(downloadResponse);

        // Destination: succeed on upload
        var destFileClient = MockHelpers.CreateMockFileClient(destPath);
        var uploadResponse = MockHelpers.CreateUploadResponse();
        destFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Any<DataLakeFileUploadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        // Wire up: GetFileClient returns correct client based on path
        _mockFsClient.GetFileClient(sourcePath).Returns(sourceFileClient);
        _mockFsClient.GetFileClient(destPath).Returns(destFileClient);
    }
}
