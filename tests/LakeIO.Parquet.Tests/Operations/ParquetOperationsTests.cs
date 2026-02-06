using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using FluentAssertions;
using NSubstitute;
using Parquet;
using Parquet.Schema;
using Parquet.Serialization;
using Xunit;

namespace LakeIO.Parquet.Tests.Operations;

/// <summary>
/// Tests for <see cref="ParquetOperations"/> using mocked Azure SDK clients
/// and real in-memory Parquet data for read operations.
/// </summary>
public class ParquetOperationsTests
{
    private readonly DataLakeFileSystemClient _mockFsClient;
    private readonly DataLakeFileClient _mockFileClient;
    private readonly LakeClientOptions _options;
    private readonly ParquetOperations _sut;

    public ParquetOperationsTests()
    {
        _mockFsClient = Substitute.For<DataLakeFileSystemClient>();
        _mockFsClient.Name.Returns("test-fs");

        _mockFileClient = Substitute.For<DataLakeFileClient>();
        _mockFileClient.Path.Returns("test-path");
        _mockFileClient.Name.Returns("test-path");

        _mockFsClient.GetFileClient(Arg.Any<string>()).Returns(_mockFileClient);

        _options = new LakeClientOptions();
        _sut = new ParquetOperations(_mockFsClient, _options);
    }

    // ===== WriteAsync Tests =====

    [Fact]
    public async Task WriteAsync_UploadsParquetData()
    {
        // Arrange
        var uploadResponse = CreateUploadResponse();
        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Any<DataLakeFileUploadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        var items = new List<TestSensorData>
        {
            new() { Id = 1, SensorId = "S1", Value = 42.5, Timestamp = DateTime.UtcNow },
            new() { Id = 2, SensorId = "S2", Value = 37.1, Timestamp = DateTime.UtcNow }
        };

        // Act
        var result = await _sut.WriteAsync("data/sensors.parquet", items);

        // Assert
        result.Should().NotBeNull();
        result.Value.Path.Should().Be("test-path");
        await _mockFileClient.Received(1).UploadAsync(
            Arg.Any<Stream>(),
            Arg.Any<DataLakeFileUploadOptions>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task WriteAsync_WithNullPath_ThrowsArgumentException()
    {
        var items = new List<TestSensorData> { new() { Id = 1 } };

        var act = () => _sut.WriteAsync<TestSensorData>(null!, items);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task WriteAsync_WithEmptyPath_ThrowsArgumentException()
    {
        var items = new List<TestSensorData> { new() { Id = 1 } };

        var act = () => _sut.WriteAsync("", items);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task WriteAsync_WithNullItems_ThrowsArgumentNullException()
    {
        var act = () => _sut.WriteAsync<TestSensorData>("data/test.parquet", null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    // ===== GetSchemaAsync Tests =====

    [Fact]
    public async Task GetSchemaAsync_ReturnsSchema()
    {
        // Arrange: create real Parquet data in memory
        var parquetStream = await CreateInMemoryParquetStream();
        _mockFileClient.OpenReadAsync(
                Arg.Any<DataLakeOpenReadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(parquetStream);

        // Act
        var schema = await _sut.GetSchemaAsync("data/sensors.parquet");

        // Assert
        schema.Should().NotBeNull();
        schema.GetDataFields().Should().HaveCount(4);
        schema.GetDataFields().Select(f => f.Name)
            .Should().Contain(new[] { "Id", "SensorId", "Value", "Timestamp" });
    }

    [Fact]
    public async Task GetSchemaAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = () => _sut.GetSchemaAsync(null!);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task GetSchemaAsync_WithEmptyPath_ThrowsArgumentException()
    {
        var act = () => _sut.GetSchemaAsync("");

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ===== ReadStreamAsync Tests =====

    [Fact]
    public async Task ReadStreamAsync_YieldsRecords()
    {
        // Arrange: create real Parquet data in memory
        var parquetStream = await CreateInMemoryParquetStream();
        _mockFileClient.OpenReadAsync(
                Arg.Any<DataLakeOpenReadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(parquetStream);

        // Act
        var records = new List<TestSensorData>();
        await foreach (var record in _sut.ReadStreamAsync<TestSensorData>("data/sensors.parquet"))
        {
            records.Add(record);
        }

        // Assert
        records.Should().HaveCount(2);
        records[0].Id.Should().Be(1);
        records[0].SensorId.Should().Be("S1");
        records[0].Value.Should().Be(42.5);
        records[1].Id.Should().Be(2);
        records[1].SensorId.Should().Be("S2");
        records[1].Value.Should().Be(37.1);
    }

    [Fact]
    public async Task ReadStreamAsync_WithNullPath_ThrowsArgumentException()
    {
        var act = async () =>
        {
            await foreach (var _ in _sut.ReadStreamAsync<TestSensorData>(null!))
            {
                // Should not reach here
            }
        };

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task ReadStreamAsync_WithEmptyPath_ThrowsArgumentException()
    {
        var act = async () =>
        {
            await foreach (var _ in _sut.ReadStreamAsync<TestSensorData>(""))
            {
                // Should not reach here
            }
        };

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ===== WriteStreamAsync Tests =====

    [Fact]
    public async Task WriteStreamAsync_WithNullPath_ThrowsArgumentException()
    {
        var items = ToAsyncEnumerable(new List<TestSensorData> { new() { Id = 1 } });

        var act = () => _sut.WriteStreamAsync(null!, items);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task WriteStreamAsync_WithEmptyPath_ThrowsArgumentException()
    {
        var items = ToAsyncEnumerable(new List<TestSensorData> { new() { Id = 1 } });

        var act = () => _sut.WriteStreamAsync("", items);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task WriteStreamAsync_WithNullItems_ThrowsArgumentNullException()
    {
        var act = () => _sut.WriteStreamAsync<TestSensorData>("data/test.parquet", null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    // ===== MergeAsync Tests =====

    [Fact]
    public async Task MergeAsync_WithNullPath_ThrowsArgumentException()
    {
        var items = new List<TestSensorData> { new() { Id = 1 } };

        var act = () => _sut.MergeAsync<TestSensorData>(null!, items);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task MergeAsync_WithEmptyPath_ThrowsArgumentException()
    {
        var items = new List<TestSensorData> { new() { Id = 1 } };

        var act = () => _sut.MergeAsync("", items);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task MergeAsync_WithNullItems_ThrowsArgumentNullException()
    {
        var act = () => _sut.MergeAsync<TestSensorData>("data/test.parquet", null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    // ===== CompactNdjsonAsync Tests =====

    [Fact]
    public async Task CompactNdjsonAsync_WithNullNdjsonPath_ThrowsArgumentException()
    {
        var act = () => _sut.CompactNdjsonAsync<TestSensorData>(null!, "output.parquet");

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task CompactNdjsonAsync_WithNullParquetPath_ThrowsArgumentException()
    {
        var act = () => _sut.CompactNdjsonAsync<TestSensorData>("input.ndjson", null!);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ===== Helper Methods =====

    /// <summary>
    /// Creates a MemoryStream containing valid Parquet data with two TestSensorData records.
    /// This enables testing read operations against real Parquet binary data while mocking
    /// only the Azure SDK layer.
    /// </summary>
    private static async Task<MemoryStream> CreateInMemoryParquetStream()
    {
        var items = new List<TestSensorData>
        {
            new() { Id = 1, SensorId = "S1", Value = 42.5, Timestamp = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc) },
            new() { Id = 2, SensorId = "S2", Value = 37.1, Timestamp = new DateTime(2025, 1, 2, 0, 0, 0, DateTimeKind.Utc) }
        };

        var ms = new MemoryStream();
        await ParquetSerializer.SerializeAsync(items, ms);
        ms.Position = 0;
        return ms;
    }

    /// <summary>
    /// Creates a mock Azure.Response of PathInfo suitable for UploadAsync return values.
    /// </summary>
    private static Azure.Response<PathInfo> CreateUploadResponse()
    {
        var pathInfo = DataLakeModelFactory.PathInfo(
            new ETag("test-etag"),
            DateTimeOffset.UtcNow);
        var rawResponse = Substitute.For<Azure.Response>();
        rawResponse.Status.Returns(200);
        return Azure.Response.FromValue(pathInfo, rawResponse);
    }

    /// <summary>
    /// Converts a list to an IAsyncEnumerable for testing WriteStreamAsync.
    /// </summary>
    private static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(IEnumerable<T> source)
    {
        foreach (var item in source)
        {
            yield return item;
        }
        await Task.CompletedTask; // Satisfy async requirement
    }

    /// <summary>
    /// Test DTO for Parquet tests with typical sensor data fields.
    /// Duplicated from Core.Tests helpers to avoid cross-project dependency issues.
    /// </summary>
    public class TestSensorData
    {
        public int Id { get; set; }
        public string SensorId { get; set; } = string.Empty;
        public double Value { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
