using System.Text;
using System.Text.Json;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using FluentAssertions;
using LakeIO.Tests.Helpers;
using NSubstitute;
using Xunit;

namespace LakeIO.Tests.Operations;

public class JsonOperationsTests
{
    private readonly DataLakeFileSystemClient _mockFsClient;
    private readonly DataLakeFileClient _mockFileClient;
    private readonly LakeClientOptions _options;
    private readonly JsonOperations _sut;

    public JsonOperationsTests()
    {
        _mockFsClient = MockHelpers.CreateMockFileSystemClient();
        _mockFileClient = MockHelpers.CreateMockFileClient("data/test.json");
        MockHelpers.SetupFileClientOnFsClient(_mockFsClient, _mockFileClient);
        _options = new LakeClientOptions();
        _sut = new JsonOperations(_mockFsClient, _options);
    }

    // ── WriteAsync ──────────────────────────────────────────────────────

    [Fact]
    public async Task WriteAsync_SerializesAndUploads()
    {
        var record = new TestRecord { Id = 1, Name = "Alice" };
        Stream? capturedStream = null;
        var uploadResponse = MockHelpers.CreateUploadResponse();

        _mockFileClient.UploadAsync(
                Arg.Do<Stream>(s =>
                {
                    using var reader = new StreamReader(s, leaveOpen: true);
                    var json = reader.ReadToEnd();
                    capturedStream = new MemoryStream(Encoding.UTF8.GetBytes(json));
                }),
                Arg.Any<DataLakeFileUploadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        await _sut.WriteAsync("data/test.json", record);

        await _mockFileClient.Received(1).UploadAsync(
            Arg.Any<Stream>(),
            Arg.Any<DataLakeFileUploadOptions>(),
            Arg.Any<CancellationToken>());

        capturedStream.Should().NotBeNull();
        capturedStream!.Position = 0;
        var deserialized = await JsonSerializer.DeserializeAsync<TestRecord>(capturedStream);
        deserialized.Should().NotBeNull();
        deserialized!.Id.Should().Be(1);
        deserialized.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task WriteAsync_SetsContentTypeToJson()
    {
        var record = new TestRecord { Id = 1, Name = "Test" };
        DataLakeFileUploadOptions? capturedOptions = null;
        var uploadResponse = MockHelpers.CreateUploadResponse();

        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Do<DataLakeFileUploadOptions>(o => capturedOptions = o),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        await _sut.WriteAsync("data/test.json", record);

        capturedOptions.Should().NotBeNull();
        capturedOptions!.HttpHeaders.ContentType.Should().Be("application/json");
    }

    [Fact]
    public async Task WriteAsync_WithOverwriteFalse_SetsIfNoneMatchCondition()
    {
        var record = new TestRecord { Id = 1, Name = "Test" };
        DataLakeFileUploadOptions? capturedOptions = null;
        var uploadResponse = MockHelpers.CreateUploadResponse();

        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Do<DataLakeFileUploadOptions>(o => capturedOptions = o),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        await _sut.WriteAsync("data/test.json", record, overwrite: false);

        capturedOptions.Should().NotBeNull();
        capturedOptions!.Conditions.Should().NotBeNull();
        capturedOptions.Conditions!.IfNoneMatch.Should().Be(new ETag("*"));
    }

    [Fact]
    public async Task WriteAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.WriteAsync<TestRecord>(null!, new TestRecord());

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task WriteAsync_WithNullValue_ThrowsArgumentNullException()
    {
        var act = () => _sut.WriteAsync<TestRecord>("data/test.json", null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task WriteAsync_ReturnsStorageResultWithPath()
    {
        var record = new TestRecord { Id = 1, Name = "Test" };
        var uploadResponse = MockHelpers.CreateUploadResponse();

        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Any<DataLakeFileUploadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        var result = await _sut.WriteAsync("data/test.json", record);

        result.Value.Should().NotBeNull();
        result.Value.Path.Should().Be("data/test.json");
        result.Value.ETag.Should().NotBeNull();
    }

    // ── ReadAsync ───────────────────────────────────────────────────────

    [Fact]
    public async Task ReadAsync_DeserializesFromStream()
    {
        var expected = new TestRecord { Id = 42, Name = "Bob" };
        var json = JsonSerializer.Serialize(expected);
        var content = new MemoryStream(Encoding.UTF8.GetBytes(json));

        var downloadResponse = MockHelpers.CreateStreamingResponse(content);
        _mockFileClient.ReadStreamingAsync(cancellationToken: Arg.Any<CancellationToken>())
            .Returns(downloadResponse);

        var result = await _sut.ReadAsync<TestRecord>("data/test.json");

        result.Value.Should().NotBeNull();
        result.Value!.Id.Should().Be(42);
        result.Value.Name.Should().Be("Bob");
    }

    [Fact]
    public async Task ReadAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.ReadAsync<TestRecord>(null!);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ── ReadNdjsonAsync ─────────────────────────────────────────────────

    [Fact]
    public async Task ReadNdjsonAsync_YieldsDeserializedRecords()
    {
        var ndjson = "{\"Id\":1,\"Name\":\"Alice\"}\n{\"Id\":2,\"Name\":\"Bob\"}\n{\"Id\":3,\"Name\":\"Charlie\"}\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(ndjson));

        _mockFileClient.OpenReadAsync(
                Arg.Any<DataLakeOpenReadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(stream);

        var records = new List<TestRecord>();
        await foreach (var record in _sut.ReadNdjsonAsync<TestRecord>("data/test.ndjson"))
        {
            records.Add(record);
        }

        records.Should().HaveCount(3);
        records[0].Id.Should().Be(1);
        records[0].Name.Should().Be("Alice");
        records[1].Id.Should().Be(2);
        records[2].Id.Should().Be(3);
    }

    [Fact]
    public async Task ReadNdjsonAsync_SkipsNullRecords()
    {
        var ndjson = "{\"Id\":1,\"Name\":\"Alice\"}\nnull\n{\"Id\":2,\"Name\":\"Bob\"}\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(ndjson));

        _mockFileClient.OpenReadAsync(
                Arg.Any<DataLakeOpenReadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(stream);

        var records = new List<TestRecord>();
        await foreach (var record in _sut.ReadNdjsonAsync<TestRecord>("data/test.ndjson"))
        {
            records.Add(record);
        }

        records.Should().HaveCount(2);
        records[0].Id.Should().Be(1);
        records[1].Id.Should().Be(2);
    }

    [Fact]
    public async Task ReadNdjsonAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = async () =>
        {
            await foreach (var _ in _sut.ReadNdjsonAsync<TestRecord>(null!))
            {
            }
        };

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ── AppendNdjsonAsync ───────────────────────────────────────────────

    [Fact]
    public async Task AppendNdjsonAsync_AppendsJsonLineToFile()
    {
        var record = new TestRecord { Id = 1, Name = "Alice" };
        Stream? capturedStream = null;

        // Mock GetPropertiesAsync to return ContentLength=0 (empty file)
        var pathProps = MockHelpers.CreatePathProperties(contentLength: 0);
        var rawPropsResponse = MockHelpers.CreateMockRawResponse();
        var propsResponse = Azure.Response.FromValue(pathProps, rawPropsResponse);
        _mockFileClient.GetPropertiesAsync(cancellationToken: Arg.Any<CancellationToken>())
            .Returns(propsResponse);

        // Capture the stream passed to AppendAsync
        var appendRawResponse = MockHelpers.CreateMockRawResponse();
        _mockFileClient.AppendAsync(
                Arg.Do<Stream>(s =>
                {
                    using var reader = new StreamReader(s, leaveOpen: true);
                    var readContent = reader.ReadToEnd();
                    capturedStream = new MemoryStream(Encoding.UTF8.GetBytes(readContent));
                }),
                Arg.Any<long>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(appendRawResponse));

        // Mock FlushAsync
        var flushPathInfo = MockHelpers.CreateMockPathInfo();
        var flushRawResponse = MockHelpers.CreateMockRawResponse();
        var flushResponse = Azure.Response.FromValue(flushPathInfo, flushRawResponse);
        _mockFileClient.FlushAsync(
                Arg.Any<long>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(flushResponse);

        await _sut.AppendNdjsonAsync("data/test.ndjson", record);

        capturedStream.Should().NotBeNull();
        capturedStream!.Position = 0;
        using var reader2 = new StreamReader(capturedStream);
        var line = reader2.ReadToEnd();
        line.Should().Contain("\"Id\":1");
        line.Should().Contain("\"Name\":\"Alice\"");
        line.Should().EndWith("\n");
    }

    [Fact]
    public async Task AppendNdjsonAsync_CreatesFileIfNotExists()
    {
        var record = new TestRecord { Id = 1, Name = "Alice" };

        // Mock GetPropertiesAsync to throw PathNotFound
        _mockFileClient.GetPropertiesAsync(cancellationToken: Arg.Any<CancellationToken>())
            .Returns<Azure.Response<PathProperties>>(x =>
                throw new RequestFailedException(404, "Not Found", "PathNotFound", null));

        // Mock CreateIfNotExistsAsync
        var createPathInfo = MockHelpers.CreateMockPathInfo();
        var createRawResponse = MockHelpers.CreateMockRawResponse();
        var createResponse = Azure.Response.FromValue(createPathInfo, createRawResponse);
        _mockFileClient.CreateIfNotExistsAsync(cancellationToken: Arg.Any<CancellationToken>())
            .Returns(createResponse);

        // Mock AppendAsync
        var appendRawResponse = MockHelpers.CreateMockRawResponse();
        _mockFileClient.AppendAsync(
                Arg.Any<Stream>(),
                Arg.Any<long>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(appendRawResponse));

        // Mock FlushAsync
        var flushPathInfo = MockHelpers.CreateMockPathInfo();
        var flushRawResponse = MockHelpers.CreateMockRawResponse();
        var flushResponse = Azure.Response.FromValue(flushPathInfo, flushRawResponse);
        _mockFileClient.FlushAsync(
                Arg.Any<long>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(flushResponse);

        await _sut.AppendNdjsonAsync("data/test.ndjson", record);

        await _mockFileClient.Received(1).CreateIfNotExistsAsync(
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task AppendNdjsonAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.AppendNdjsonAsync("", new TestRecord());

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task AppendNdjsonAsync_WithNullValue_ThrowsArgumentNullException()
    {
        var act = () => _sut.AppendNdjsonAsync<TestRecord>("data/test.ndjson", null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }
}
