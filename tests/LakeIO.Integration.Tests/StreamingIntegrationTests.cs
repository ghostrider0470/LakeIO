using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using FluentAssertions;
using NSubstitute;
using Xunit;

namespace LakeIO.Integration.Tests;

// ==========================================================================
// Unit-level streaming tests (always run, no Azure needed)
// ==========================================================================

/// <summary>
/// Validates that <see cref="ChunkedUploadStream"/> correctly chunks data and operates
/// with bounded memory. These tests use mocked <see cref="DataLakeFileClient"/> and
/// run without an Azure connection.
/// </summary>
public class ChunkedUploadStreamUnitTests
{
    private static DataLakeFileClient CreateMockFileClient(Action? onAppend = null)
    {
        var mock = Substitute.For<DataLakeFileClient>();

        // Mock AppendAsync using the overload ChunkedUploadStream calls:
        //   AppendAsync(Stream content, long offset, cancellationToken: token)
        // This maps to AppendAsync(Stream, long, DataLakeFileAppendOptions, CancellationToken)
        mock.AppendAsync(Arg.Any<Stream>(), Arg.Any<long>(),
                Arg.Any<DataLakeFileAppendOptions>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(callInfo =>
            {
                onAppend?.Invoke();
                return Task.FromResult(Substitute.For<Azure.Response>());
            });

        // Mock FlushAsync -- called by DisposeAsync to commit the file
        // ChunkedUploadStream calls: FlushAsync(_fileOffset, cancellationToken: token)
        mock.FlushAsync(Arg.Any<long>(), Arg.Any<bool?>(), Arg.Any<bool?>(),
                Arg.Any<PathHttpHeaders>(), Arg.Any<DataLakeRequestConditions>(),
                Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(_ =>
            {
                var pathInfo = DataLakeModelFactory.PathInfo(new ETag("test"), DateTimeOffset.UtcNow);
                return Task.FromResult(Azure.Response.FromValue(pathInfo, Substitute.For<Azure.Response>()));
            });

        return mock;
    }

    [Fact]
    public async Task LargeWrite_ChunksCorrectly()
    {
        // Arrange
        var appendCallCount = 0;
        var mockFileClient = CreateMockFileClient(onAppend: () => Interlocked.Increment(ref appendCallCount));

        const int chunkSize = 4096; // 4KB chunks
        const int totalBytes = 100 * 1024; // 100KB total
        var data = new byte[1024]; // Write in 1KB blocks
        new Random(42).NextBytes(data);

        // Act
        await using (var stream = new ChunkedUploadStream(mockFileClient, chunkSize))
        {
            for (var i = 0; i < totalBytes / data.Length; i++)
            {
                await stream.WriteAsync(data, 0, data.Length);
            }
        }

        // Assert -- 100KB / 4KB = 25 full chunks, no partial remainder
        appendCallCount.Should().Be(25, "100KB of data with 4KB chunks should produce 25 append calls");
    }

    [Fact]
    public async Task MemoryBounded_DoesNotBufferEntirePayload()
    {
        // Arrange
        var mockFileClient = CreateMockFileClient();

        const int chunkSize = 64 * 1024; // 64KB chunk size
        const int totalBytes = 10 * 1024 * 1024; // 10MB total
        var writeBlock = new byte[1024]; // Write in 1KB blocks

        // Record baseline memory before the operation
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var baselineMemory = GC.GetTotalMemory(forceFullCollection: true);

        // Act
        long peakDelta = 0;
        await using (var stream = new ChunkedUploadStream(mockFileClient, chunkSize))
        {
            for (var i = 0; i < totalBytes / writeBlock.Length; i++)
            {
                await stream.WriteAsync(writeBlock, 0, writeBlock.Length);

                // Sample memory periodically (every 100 iterations)
                if (i % 100 == 0)
                {
                    var currentDelta = GC.GetTotalMemory(false) - baselineMemory;
                    if (currentDelta > peakDelta)
                        peakDelta = currentDelta;
                }
            }
        }

        // Assert -- memory should stay bounded (well under 5MB over baseline)
        // The only significant allocation should be the chunk buffer (~64KB from ArrayPool)
        // plus minor overhead. 5MB threshold is very generous.
        peakDelta.Should().BeLessThan(5 * 1024 * 1024,
            "ChunkedUploadStream should not buffer the entire 10MB payload; memory delta should stay bounded");
    }
}

// ==========================================================================
// Integration-level streaming tests (skip when no Azure connection)
// ==========================================================================

/// <summary>
/// Integration tests that stream data to a real Azure Data Lake account.
/// Skips gracefully when <c>LAKEIO_TEST_CONNECTION_STRING</c> is not set.
/// </summary>
public class StreamingIntegrationTests : IntegrationTestBase
{
    [Fact]
    public async Task StreamingUpload_LargeFile_CompletesWithoutOOM()
    {
        // Arrange -- use Azure SDK directly to get a DataLakeFileClient
        // (ChunkedUploadStream operates on the Azure SDK type, not the LakeIO wrapper)
        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvVar)!;
        var fileSystemName = Environment.GetEnvironmentVariable(FileSystemEnvVar)!;
        var azureServiceClient = new DataLakeServiceClient(connectionString);
        var azureFsClient = azureServiceClient.GetFileSystemClient(fileSystemName);

        var path = UniquePath(".bin");
        var azureFileClient = azureFsClient.GetFileClient(path);

        // Create the file first (ChunkedUploadStream requires an existing file)
        await azureFileClient.CreateIfNotExistsAsync();

        const int totalBytes = 10 * 1024 * 1024; // 10MB
        const int chunkSize = 256 * 1024; // 256KB chunks
        var writeBlock = new byte[4096]; // 4KB write blocks
        new Random(42).NextBytes(writeBlock);

        // Act -- stream 10MB of data
        await using (var uploadStream = new ChunkedUploadStream(azureFileClient, chunkSize))
        {
            for (var i = 0; i < totalBytes / writeBlock.Length; i++)
            {
                await uploadStream.WriteAsync(writeBlock, 0, writeBlock.Length);
            }
        }

        // Assert -- verify the file exists and has the expected size
        var existsResult = await FileSystem.Files().ExistsAsync(path);
        existsResult.Value.Should().BeTrue("the uploaded file should exist");

        var properties = await azureFileClient.GetPropertiesAsync();
        properties.Value.ContentLength.Should().Be(totalBytes,
            "the uploaded file should be exactly 10MB");
    }
}
