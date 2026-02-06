using System.Text;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using FluentAssertions;
using LakeIO.Tests.Helpers;
using NSubstitute;
using Xunit;

namespace LakeIO.Tests.Operations;

public class CsvOperationsTests
{
    private readonly DataLakeFileSystemClient _mockFsClient;
    private readonly DataLakeFileClient _mockFileClient;
    private readonly LakeClientOptions _options;
    private readonly CsvOperations _sut;

    public CsvOperationsTests()
    {
        _mockFsClient = MockHelpers.CreateMockFileSystemClient();
        _mockFileClient = MockHelpers.CreateMockFileClient("data/test.csv");
        MockHelpers.SetupFileClientOnFsClient(_mockFsClient, _mockFileClient);
        _options = new LakeClientOptions();
        _sut = new CsvOperations(_mockFsClient, _options);
    }

    // ── WriteAsync ──────────────────────────────────────────────────────

    [Fact]
    public async Task WriteAsync_SerializesAndUploads()
    {
        var items = new List<TestRecord>
        {
            new() { Id = 1, Name = "Alice" },
            new() { Id = 2, Name = "Bob" }
        };
        Stream? capturedStream = null;
        var uploadResponse = MockHelpers.CreateUploadResponse();

        _mockFileClient.UploadAsync(
                Arg.Do<Stream>(s =>
                {
                    using var reader = new StreamReader(s, leaveOpen: true);
                    var csv = reader.ReadToEnd();
                    capturedStream = new MemoryStream(Encoding.UTF8.GetBytes(csv));
                }),
                Arg.Any<DataLakeFileUploadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        await _sut.WriteAsync("data/test.csv", items);

        await _mockFileClient.Received(1).UploadAsync(
            Arg.Any<Stream>(),
            Arg.Any<DataLakeFileUploadOptions>(),
            Arg.Any<CancellationToken>());

        capturedStream.Should().NotBeNull();
        capturedStream!.Position = 0;
        using var reader2 = new StreamReader(capturedStream);
        var content = reader2.ReadToEnd();
        content.Should().Contain("Id");
        content.Should().Contain("Name");
        content.Should().Contain("Alice");
        content.Should().Contain("Bob");
    }

    [Fact]
    public async Task WriteAsync_SetsContentTypeToCsv()
    {
        var items = new List<TestRecord> { new() { Id = 1, Name = "Test" } };
        DataLakeFileUploadOptions? capturedOptions = null;
        var uploadResponse = MockHelpers.CreateUploadResponse();

        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Do<DataLakeFileUploadOptions>(o => capturedOptions = o),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        await _sut.WriteAsync("data/test.csv", items);

        capturedOptions.Should().NotBeNull();
        capturedOptions!.HttpHeaders.ContentType.Should().Be("text/csv");
    }

    [Fact]
    public async Task WriteAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.WriteAsync<TestRecord>(null!, new List<TestRecord>());

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task WriteAsync_WithNullItems_ThrowsArgumentNullException()
    {
        var act = () => _sut.WriteAsync<TestRecord>("data/test.csv", null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task WriteAsync_WithOverwriteFalse_SetsCondition()
    {
        var items = new List<TestRecord> { new() { Id = 1, Name = "Test" } };
        DataLakeFileUploadOptions? capturedOptions = null;
        var uploadResponse = MockHelpers.CreateUploadResponse();

        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Do<DataLakeFileUploadOptions>(o => capturedOptions = o),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        await _sut.WriteAsync("data/test.csv", items, overwrite: false);

        capturedOptions!.Conditions.Should().NotBeNull();
        capturedOptions.Conditions!.IfNoneMatch.Should().Be(new ETag("*"));
    }

    // ── ReadAsync ───────────────────────────────────────────────────────

    [Fact]
    public async Task ReadAsync_DeserializesFromStream()
    {
        var csvContent = "Id,Name\r\n1,Alice\r\n2,Bob\r\n";
        var content = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));

        var downloadResponse = MockHelpers.CreateStreamingResponse(content);
        _mockFileClient.ReadStreamingAsync(cancellationToken: Arg.Any<CancellationToken>())
            .Returns(downloadResponse);

        var result = await _sut.ReadAsync<TestRecord>("data/test.csv");

        result.Value.Should().HaveCount(2);
        result.Value[0].Id.Should().Be(1);
        result.Value[0].Name.Should().Be("Alice");
        result.Value[1].Id.Should().Be(2);
        result.Value[1].Name.Should().Be("Bob");
    }

    [Fact]
    public async Task ReadAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.ReadAsync<TestRecord>(null!);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ── ReadStreamAsync ─────────────────────────────────────────────────

    [Fact]
    public async Task ReadStreamAsync_YieldsRecords()
    {
        var csvContent = "Id,Name\r\n1,Alice\r\n2,Bob\r\n3,Charlie\r\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));

        _mockFileClient.OpenReadAsync(
                Arg.Any<DataLakeOpenReadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(stream);

        var records = new List<TestRecord>();
        await foreach (var record in _sut.ReadStreamAsync<TestRecord>("data/test.csv"))
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
    public async Task ReadStreamAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = async () =>
        {
            await foreach (var _ in _sut.ReadStreamAsync<TestRecord>(null!))
            {
            }
        };

        await act.Should().ThrowAsync<ArgumentException>();
    }
}
