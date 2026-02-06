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

    // ===== ValidateAsync Tests — Quick Level =====

    [Fact]
    public async Task ValidateAsync_QuickLevel_ValidFile_ReturnsValid()
    {
        // Arrange
        var properties = CreateMockPathProperties(contentLength: 1024);
        var propertiesResponse = Azure.Response.FromValue(properties, Substitute.For<Azure.Response>());
        _mockFileClient.GetPropertiesAsync(
                conditions: Arg.Any<DataLakeRequestConditions>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(propertiesResponse);

        // Act
        var result = await _sut.ValidateAsync("data/sensors.parquet", ParquetValidationLevel.Quick);

        // Assert
        result.IsValid.Should().BeTrue();
        result.Level.Should().Be(ParquetValidationLevel.Quick);
        result.FileSize.Should().Be(1024);
        result.RowGroupCount.Should().BeNull();
        result.FieldNames.Should().BeNull();
    }

    [Fact]
    public async Task ValidateAsync_QuickLevel_FileTooSmall_ReturnsInvalid()
    {
        // Arrange
        var properties = CreateMockPathProperties(contentLength: 8);
        var propertiesResponse = Azure.Response.FromValue(properties, Substitute.For<Azure.Response>());
        _mockFileClient.GetPropertiesAsync(
                conditions: Arg.Any<DataLakeRequestConditions>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(propertiesResponse);

        // Act
        var result = await _sut.ValidateAsync("data/sensors.parquet", ParquetValidationLevel.Quick);

        // Assert
        result.IsValid.Should().BeFalse();
        result.ErrorReason.Should().Contain("less than 12 bytes");
        result.FileSize.Should().Be(8);
        result.Level.Should().Be(ParquetValidationLevel.Quick);
    }

    [Fact]
    public async Task ValidateAsync_QuickLevel_ExactMinimumSize_ReturnsValid()
    {
        // Arrange
        var properties = CreateMockPathProperties(contentLength: 12);
        var propertiesResponse = Azure.Response.FromValue(properties, Substitute.For<Azure.Response>());
        _mockFileClient.GetPropertiesAsync(
                conditions: Arg.Any<DataLakeRequestConditions>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(propertiesResponse);

        // Act
        var result = await _sut.ValidateAsync("data/sensors.parquet", ParquetValidationLevel.Quick);

        // Assert
        result.IsValid.Should().BeTrue();
        result.Level.Should().Be(ParquetValidationLevel.Quick);
        result.FileSize.Should().Be(12);
    }

    // ===== ValidateAsync Tests — Standard Level =====

    [Fact]
    public async Task ValidateAsync_StandardLevel_ValidParquetMagic_ReturnsValid()
    {
        // Arrange
        var properties = CreateMockPathProperties(contentLength: 100);
        var propertiesResponse = Azure.Response.FromValue(properties, Substitute.For<Azure.Response>());
        _mockFileClient.GetPropertiesAsync(
                conditions: Arg.Any<DataLakeRequestConditions>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(propertiesResponse);

        // PAR1 magic bytes
        var par1Bytes = new byte[] { 0x50, 0x41, 0x52, 0x31 };
        var par1Content = BinaryData.FromBytes(par1Bytes);
        var contentResponse = CreateMockContentResponse(par1Content);

        // Both header and footer reads return valid PAR1 bytes
        _mockFileClient.ReadContentAsync(
                Arg.Any<DataLakeFileReadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(contentResponse);

        // Act
        var result = await _sut.ValidateAsync("data/sensors.parquet", ParquetValidationLevel.Standard);

        // Assert
        result.IsValid.Should().BeTrue();
        result.Level.Should().Be(ParquetValidationLevel.Standard);
    }

    [Fact]
    public async Task ValidateAsync_StandardLevel_InvalidHeader_ReturnsInvalid()
    {
        // Arrange
        var properties = CreateMockPathProperties(contentLength: 100);
        var propertiesResponse = Azure.Response.FromValue(properties, Substitute.For<Azure.Response>());
        _mockFileClient.GetPropertiesAsync(
                conditions: Arg.Any<DataLakeRequestConditions>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(propertiesResponse);

        // Invalid header bytes (not PAR1)
        var invalidBytes = new byte[] { 0x00, 0x00, 0x00, 0x00 };
        var invalidContent = BinaryData.FromBytes(invalidBytes);
        var contentResponse = CreateMockContentResponse(invalidContent);

        _mockFileClient.ReadContentAsync(
                Arg.Any<DataLakeFileReadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(contentResponse);

        // Act
        var result = await _sut.ValidateAsync("data/sensors.parquet", ParquetValidationLevel.Standard);

        // Assert
        result.IsValid.Should().BeFalse();
        result.ErrorReason.Should().Contain("header magic");
    }

    [Fact]
    public async Task ValidateAsync_StandardLevel_InvalidFooter_ReturnsInvalid()
    {
        // Arrange
        var properties = CreateMockPathProperties(contentLength: 100);
        var propertiesResponse = Azure.Response.FromValue(properties, Substitute.For<Azure.Response>());
        _mockFileClient.GetPropertiesAsync(
                conditions: Arg.Any<DataLakeRequestConditions>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(propertiesResponse);

        // Header: valid PAR1, Footer: invalid
        var par1Bytes = new byte[] { 0x50, 0x41, 0x52, 0x31 };
        var invalidBytes = new byte[] { 0x00, 0x00, 0x00, 0x00 };

        // Pre-compute return values to avoid NS4000
        var headerResponse = CreateMockContentResponse(BinaryData.FromBytes(par1Bytes));
        var footerResponse = CreateMockContentResponse(BinaryData.FromBytes(invalidBytes));

        var callCount = 0;
        _mockFileClient.ReadContentAsync(
                Arg.Any<DataLakeFileReadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var call = Interlocked.Increment(ref callCount);
                return call == 1 ? headerResponse : footerResponse;
            });

        // Act
        var result = await _sut.ValidateAsync("data/sensors.parquet", ParquetValidationLevel.Standard);

        // Assert
        result.IsValid.Should().BeFalse();
        result.ErrorReason.Should().Contain("footer magic");
    }

    // ===== ValidateAsync Tests — Deep Level =====

    [Fact]
    public async Task ValidateAsync_DeepLevel_ValidParquet_ReturnsMetadata()
    {
        // Arrange
        var properties = CreateMockPathProperties(contentLength: 1024);
        var propertiesResponse = Azure.Response.FromValue(properties, Substitute.For<Azure.Response>());
        _mockFileClient.GetPropertiesAsync(
                conditions: Arg.Any<DataLakeRequestConditions>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(propertiesResponse);

        // Valid PAR1 magic for header and footer
        var par1Bytes = new byte[] { 0x50, 0x41, 0x52, 0x31 };
        var contentResponse = CreateMockContentResponse(BinaryData.FromBytes(par1Bytes));
        _mockFileClient.ReadContentAsync(
                Arg.Any<DataLakeFileReadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(contentResponse);

        // Create real in-memory Parquet stream for Deep level OpenReadAsync
        var parquetStream = await CreateInMemoryParquetStream();
        _mockFileClient.OpenReadAsync(
                Arg.Any<DataLakeOpenReadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(parquetStream);

        // Act
        var result = await _sut.ValidateAsync("data/sensors.parquet", ParquetValidationLevel.Deep);

        // Assert
        result.IsValid.Should().BeTrue();
        result.Level.Should().Be(ParquetValidationLevel.Deep);
        result.RowGroupCount.Should().BeGreaterThanOrEqualTo(1);
        result.FieldNames.Should().NotBeNull();
        result.FieldNames.Should().Contain("Id");
        result.FieldNames.Should().Contain("SensorId");
        result.FieldNames.Should().Contain("Value");
        result.FieldNames.Should().Contain("Timestamp");
    }

    // ===== ValidateAsync Tests — Error Handling =====

    [Fact]
    public async Task ValidateAsync_FileNotFound_ReturnsInvalid()
    {
        // Arrange: GetPropertiesAsync throws 404
        _mockFileClient.GetPropertiesAsync(
                conditions: Arg.Any<DataLakeRequestConditions>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns<Azure.Response<PathProperties>>(_ =>
                throw new RequestFailedException(404, "PathNotFound"));

        // Act
        var result = await _sut.ValidateAsync("data/missing.parquet", ParquetValidationLevel.Quick);

        // Assert
        result.IsValid.Should().BeFalse();
        result.ErrorReason.Should().Contain("PathNotFound");
    }

    [Fact]
    public async Task ValidateAsync_NullPath_ThrowsArgumentException()
    {
        // Act
        var act = () => _sut.ValidateAsync(null!);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task ValidateAsync_EmptyPath_ThrowsArgumentException()
    {
        // Act
        var act = () => _sut.ValidateAsync("");

        // Assert
        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ===== Post-Write Validation Tests =====

    [Fact]
    public async Task WriteAsync_ValidateAfterWriteTrue_CallsValidation()
    {
        // Arrange: successful upload
        var uploadResponse = CreateUploadResponse();
        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Any<DataLakeFileUploadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        // Quick validation passes (file >= 12 bytes)
        var properties = CreateMockPathProperties(contentLength: 512);
        var propertiesResponse = Azure.Response.FromValue(properties, Substitute.For<Azure.Response>());
        _mockFileClient.GetPropertiesAsync(
                conditions: Arg.Any<DataLakeRequestConditions>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(propertiesResponse);

        var items = new List<TestSensorData>
        {
            new() { Id = 1, SensorId = "S1", Value = 42.5, Timestamp = DateTime.UtcNow }
        };

        // Act
        var result = await _sut.WriteAsync("data/sensors.parquet", items,
            new ParquetOptions { ValidateAfterWrite = true });

        // Assert: no exception, result is valid
        result.Should().NotBeNull();
        result.Value.Path.Should().Be("test-path");
    }

    [Fact]
    public async Task WriteAsync_ValidateAfterWriteTrue_ValidationFails_ThrowsInvalidOperationException()
    {
        // Arrange: successful upload
        var uploadResponse = CreateUploadResponse();
        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Any<DataLakeFileUploadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        // Quick validation fails (file too small)
        var properties = CreateMockPathProperties(contentLength: 5);
        var propertiesResponse = Azure.Response.FromValue(properties, Substitute.For<Azure.Response>());
        _mockFileClient.GetPropertiesAsync(
                conditions: Arg.Any<DataLakeRequestConditions>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(propertiesResponse);

        var items = new List<TestSensorData>
        {
            new() { Id = 1, SensorId = "S1", Value = 42.5, Timestamp = DateTime.UtcNow }
        };

        // Act
        var act = () => _sut.WriteAsync("data/sensors.parquet", items,
            new ParquetOptions { ValidateAfterWrite = true });

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Post-write validation failed*");
    }

    [Fact]
    public async Task WriteAsync_ValidateAfterWriteNull_SkipsValidation()
    {
        // Arrange: successful upload
        var uploadResponse = CreateUploadResponse();
        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Any<DataLakeFileUploadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        var items = new List<TestSensorData>
        {
            new() { Id = 1, SensorId = "S1", Value = 42.5, Timestamp = DateTime.UtcNow }
        };

        // Act: no options (ValidateAfterWrite is null)
        var result = await _sut.WriteAsync("data/sensors.parquet", items);

        // Assert: no GetPropertiesAsync call (validation skipped)
        result.Should().NotBeNull();
        await _mockFileClient.DidNotReceive().GetPropertiesAsync(
            conditions: Arg.Any<DataLakeRequestConditions>(),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task WriteAsync_ValidateAfterWriteFalse_SkipsValidation()
    {
        // Arrange: successful upload
        var uploadResponse = CreateUploadResponse();
        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Any<DataLakeFileUploadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        var items = new List<TestSensorData>
        {
            new() { Id = 1, SensorId = "S1", Value = 42.5, Timestamp = DateTime.UtcNow }
        };

        // Act: ValidateAfterWrite = false
        var result = await _sut.WriteAsync("data/sensors.parquet", items,
            new ParquetOptions { ValidateAfterWrite = false });

        // Assert: no GetPropertiesAsync call (validation skipped)
        result.Should().NotBeNull();
        await _mockFileClient.DidNotReceive().GetPropertiesAsync(
            conditions: Arg.Any<DataLakeRequestConditions>(),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task WriteStreamAsync_ValidateAfterWriteTrue_CallsValidation()
    {
        // Arrange: successful upload
        var uploadResponse = CreateUploadResponse();
        _mockFileClient.UploadAsync(
                Arg.Any<Stream>(),
                Arg.Any<DataLakeFileUploadOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(uploadResponse);

        // Quick validation passes (file >= 12 bytes)
        var properties = CreateMockPathProperties(contentLength: 512);
        var propertiesResponse = Azure.Response.FromValue(properties, Substitute.For<Azure.Response>());
        _mockFileClient.GetPropertiesAsync(
                conditions: Arg.Any<DataLakeRequestConditions>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(propertiesResponse);

        var items = ToAsyncEnumerable(new List<TestSensorData>
        {
            new() { Id = 1, SensorId = "S1", Value = 42.5, Timestamp = DateTime.UtcNow }
        });

        // Act
        var result = await _sut.WriteStreamAsync("data/sensors.parquet", items,
            new ParquetOptions { ValidateAfterWrite = true });

        // Assert: no exception, result is valid
        result.Should().NotBeNull();
        result.Value.Path.Should().Be("test-path");
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
    /// Creates a mock <see cref="PathProperties"/> via the DataLake model factory with the specified content length.
    /// </summary>
    private static PathProperties CreateMockPathProperties(long contentLength = 1024)
    {
        return DataLakeModelFactory.PathProperties(
            lastModified: DateTimeOffset.UtcNow,
            creationTime: DateTimeOffset.UtcNow.AddDays(-1),
            metadata: new Dictionary<string, string>(),
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
            contentLength: contentLength,
            contentType: "application/octet-stream",
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
            accessTierChangeTime: default,
            isDirectory: false);
    }

    /// <summary>
    /// Creates a mock <see cref="Azure.Response{T}"/> of <see cref="DataLakeFileReadResult"/>
    /// for ReadContentAsync return values.
    /// </summary>
    private static Azure.Response<DataLakeFileReadResult> CreateMockContentResponse(BinaryData content)
    {
        var details = DataLakeModelFactory.FileDownloadDetails(
            lastModified: DateTimeOffset.UtcNow,
            metadata: new Dictionary<string, string>(),
            contentRange: null,
            eTag: new ETag("\"test-etag\""),
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
        var readResult = DataLakeModelFactory.DataLakeFileReadResult(content, details);
        return Azure.Response.FromValue(readResult, Substitute.For<Azure.Response>());
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
