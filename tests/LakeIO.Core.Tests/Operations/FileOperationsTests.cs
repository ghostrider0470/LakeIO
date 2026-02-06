using System.Text;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using FluentAssertions;
using LakeIO.Tests.Helpers;
using NSubstitute;
using Xunit;

namespace LakeIO.Tests.Operations;

public class FileOperationsTests
{
    private readonly DataLakeFileSystemClient _mockFsClient;
    private readonly DataLakeFileClient _mockFileClient;
    private readonly LakeClientOptions _options;
    private readonly FileOperations _sut;

    public FileOperationsTests()
    {
        _mockFsClient = MockHelpers.CreateMockFileSystemClient();
        _mockFileClient = MockHelpers.CreateMockFileClient("data/test.bin");
        MockHelpers.SetupFileClientOnFsClient(_mockFsClient, _mockFileClient);
        _options = new LakeClientOptions();
        _sut = new FileOperations(_mockFsClient, _options);
    }

    // ── UploadAsync ─────────────────────────────────────────────────────

    [Fact]
    public async Task UploadAsync_UploadsStreamContent()
    {
        var content = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 });
        var uploadResponse = MockHelpers.CreateUploadResponse();

        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Any<DataLakeFileUploadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        await _sut.UploadAsync("data/test.bin", content);

        await _mockFileClient.Received(1).UploadAsync(
            Arg.Any<Stream>(),
            Arg.Any<DataLakeFileUploadOptions>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task UploadAsync_SetsContentType_WhenProvided()
    {
        var content = new MemoryStream(new byte[] { 1, 2, 3 });
        DataLakeFileUploadOptions? capturedOptions = null;
        var uploadResponse = MockHelpers.CreateUploadResponse();

        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Do<DataLakeFileUploadOptions>(o => capturedOptions = o),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        await _sut.UploadAsync("data/test.bin", content, contentType: "application/octet-stream");

        capturedOptions.Should().NotBeNull();
        capturedOptions!.HttpHeaders.Should().NotBeNull();
        capturedOptions.HttpHeaders.ContentType.Should().Be("application/octet-stream");
    }

    [Fact]
    public async Task UploadAsync_WithOverwriteFalse_SetsCondition()
    {
        var content = new MemoryStream(new byte[] { 1, 2, 3 });
        DataLakeFileUploadOptions? capturedOptions = null;
        var uploadResponse = MockHelpers.CreateUploadResponse();

        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Do<DataLakeFileUploadOptions>(o => capturedOptions = o),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        await _sut.UploadAsync("data/test.bin", content, overwrite: false);

        capturedOptions!.Conditions.Should().NotBeNull();
        capturedOptions.Conditions!.IfNoneMatch.Should().Be(new ETag("*"));
    }

    [Fact]
    public async Task UploadAsync_ReturnsStorageResultWithPath()
    {
        var content = new MemoryStream(new byte[] { 1, 2, 3 });
        var uploadResponse = MockHelpers.CreateUploadResponse();

        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Any<DataLakeFileUploadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        var result = await _sut.UploadAsync("data/test.bin", content);

        result.Value.Should().NotBeNull();
        result.Value.Path.Should().Be("data/test.bin");
    }

    [Fact]
    public async Task UploadAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.UploadAsync(null!, new MemoryStream());

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task UploadAsync_WithNullContent_ThrowsArgumentNullException()
    {
        var act = () => _sut.UploadAsync("data/test.bin", null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    // ── DownloadAsync ───────────────────────────────────────────────────

    [Fact]
    public async Task DownloadAsync_ReturnsFileContent()
    {
        var expectedBytes = Encoding.UTF8.GetBytes("hello world");
        var binaryData = new BinaryData(expectedBytes);
        var downloadResponse = MockHelpers.CreateContentResponse(binaryData);

        _mockFileClient.ReadContentAsync(Arg.Any<CancellationToken>())
            .Returns(downloadResponse);

        var result = await _sut.DownloadAsync("data/test.bin");

        result.Value.Should().NotBeNull();
        result.Value.ToArray().Should().BeEquivalentTo(expectedBytes);
    }

    [Fact]
    public async Task DownloadAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.DownloadAsync(null!);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ── DownloadStreamAsync ─────────────────────────────────────────────

    [Fact]
    public async Task DownloadStreamAsync_ReturnsStream()
    {
        var content = new MemoryStream(Encoding.UTF8.GetBytes("stream content"));
        var downloadResponse = MockHelpers.CreateStreamingResponse(content);

        _mockFileClient.ReadStreamingAsync(cancellationToken: Arg.Any<CancellationToken>())
            .Returns(downloadResponse);

        var result = await _sut.DownloadStreamAsync("data/test.bin");

        result.Value.Should().NotBeNull();
        result.Value.Should().BeAssignableTo<Stream>();
        using var reader = new StreamReader(result.Value);
        var text = await reader.ReadToEndAsync();
        text.Should().Be("stream content");
    }

    [Fact]
    public async Task DownloadStreamAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.DownloadStreamAsync(null!);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ── DeleteAsync ─────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteAsync_CallsDeleteOnFileClient()
    {
        var deleteResponse = MockHelpers.CreateMockRawResponse();
        _mockFileClient.DeleteAsync(
                Arg.Any<DataLakeRequestConditions>(),
                Arg.Any<CancellationToken>())
            .Returns(deleteResponse);

        await _sut.DeleteAsync("data/test.bin");

        await _mockFileClient.Received(1).DeleteAsync(
            Arg.Any<DataLakeRequestConditions>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DeleteAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.DeleteAsync(null!);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ── ExistsAsync ─────────────────────────────────────────────────────

    [Fact]
    public async Task ExistsAsync_ReturnsTrue_WhenFileExists()
    {
        var rawResponse = MockHelpers.CreateMockRawResponse();
        var existsResponse = Azure.Response.FromValue(true, rawResponse);

        _mockFileClient.ExistsAsync(Arg.Any<CancellationToken>())
            .Returns(existsResponse);

        var result = await _sut.ExistsAsync("data/test.bin");

        result.Value.Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_ReturnsFalse_WhenFileDoesNotExist()
    {
        var rawResponse = MockHelpers.CreateMockRawResponse();
        var existsResponse = Azure.Response.FromValue(false, rawResponse);

        _mockFileClient.ExistsAsync(Arg.Any<CancellationToken>())
            .Returns(existsResponse);

        var result = await _sut.ExistsAsync("data/test.bin");

        result.Value.Should().BeFalse();
    }

    [Fact]
    public async Task ExistsAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.ExistsAsync(null!);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ── GetPropertiesAsync ──────────────────────────────────────────────

    [Fact]
    public async Task GetPropertiesAsync_ReturnsProperties()
    {
        var pathProps = MockHelpers.CreatePathProperties(
            contentLength: 2048,
            contentType: "application/json",
            etag: "\"props-etag\"");
        var rawResponse = MockHelpers.CreateMockRawResponse();
        var propsResponse = Azure.Response.FromValue(pathProps, rawResponse);

        _mockFileClient.GetPropertiesAsync(cancellationToken: Arg.Any<CancellationToken>())
            .Returns(propsResponse);

        var result = await _sut.GetPropertiesAsync("data/test.json");

        result.Value.Should().NotBeNull();
        result.Value.ContentLength.Should().Be(2048);
        result.Value.ContentType.Should().Be("application/json");
    }

    [Fact]
    public async Task GetPropertiesAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.GetPropertiesAsync(null!);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ── MoveAsync ───────────────────────────────────────────────────────

    [Fact]
    public async Task MoveAsync_CallsRenameAsync()
    {
        var renamedClient = MockHelpers.CreateMockFileClient("data/destination.bin");
        var rawResponse = MockHelpers.CreateMockRawResponse();
        var renameResponse = Azure.Response.FromValue(renamedClient, rawResponse);

        _mockFileClient.RenameAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<DataLakeRequestConditions>(),
                Arg.Any<DataLakeRequestConditions>(),
                Arg.Any<CancellationToken>())
            .Returns(renameResponse);

        var result = await _sut.MoveAsync("data/source.bin", "data/destination.bin");

        await _mockFileClient.Received(1).RenameAsync(
            "data/destination.bin",
            Arg.Any<string>(),
            Arg.Any<DataLakeRequestConditions>(),
            Arg.Any<DataLakeRequestConditions>(),
            Arg.Any<CancellationToken>());

        result.Value.Path.Should().Be("data/destination.bin");
    }

    [Fact]
    public async Task MoveAsync_WithOverwriteFalse_SetsDestinationConditions()
    {
        var renamedClient = MockHelpers.CreateMockFileClient("data/destination.bin");
        var rawResponse = MockHelpers.CreateMockRawResponse();
        var renameResponse = Azure.Response.FromValue(renamedClient, rawResponse);
        DataLakeRequestConditions? capturedConditions = null;

        _mockFileClient.RenameAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<DataLakeRequestConditions>(),
                Arg.Do<DataLakeRequestConditions>(c => capturedConditions = c),
                Arg.Any<CancellationToken>())
            .Returns(renameResponse);

        await _sut.MoveAsync("data/source.bin", "data/destination.bin", overwrite: false);

        capturedConditions.Should().NotBeNull();
        capturedConditions!.IfNoneMatch.Should().Be(new ETag("*"));
    }

    [Fact]
    public async Task MoveAsync_WithNullSourcePath_ThrowsArgumentException()
    {
        var act = () => _sut.MoveAsync(null!, "data/destination.bin");

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task MoveAsync_WithNullDestinationPath_ThrowsArgumentException()
    {
        var act = () => _sut.MoveAsync("data/source.bin", null!);

        await act.Should().ThrowAsync<ArgumentException>();
    }
}
