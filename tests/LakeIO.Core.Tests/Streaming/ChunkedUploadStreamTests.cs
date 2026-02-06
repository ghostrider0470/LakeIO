using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using FluentAssertions;
using LakeIO.Tests.Helpers;
using NSubstitute;
using Xunit;

namespace LakeIO.Tests.Streaming;

public class ChunkedUploadStreamTests
{
    private readonly DataLakeFileClient _mockFileClient;
    private long _lastFlushOffset = -1;
    private int _appendCallCount;
    private int _flushCallCount;
    private readonly List<long> _appendOffsets = new();

    public ChunkedUploadStreamTests()
    {
        _mockFileClient = MockHelpers.CreateMockFileClient("test/file.bin");

        // Wire AppendAsync -- track calls manually since NSubstitute overload matching
        // can be tricky with Azure SDK virtual methods
        _mockFileClient
            .AppendAsync(
                Arg.Any<Stream>(),
                Arg.Any<long>(),
                Arg.Any<DataLakeFileAppendOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                _appendCallCount++;
                _appendOffsets.Add(callInfo.ArgAt<long>(1));
                return Substitute.For<Azure.Response>();
            });

        // Wire FlushAsync -- ChunkedUploadStream calls FlushAsync(offset, cancellationToken:)
        // which resolves to the 3-param overload: FlushAsync(long, DataLakeFileFlushOptions?, CancellationToken)
        var pathInfo = MockHelpers.CreateMockPathInfo();
        var flushResponse = Azure.Response.FromValue(pathInfo, MockHelpers.CreateMockRawResponse());
        _mockFileClient
            .FlushAsync(
                Arg.Any<long>(),
                Arg.Any<DataLakeFileFlushOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                _flushCallCount++;
                _lastFlushOffset = callInfo.ArgAt<long>(0);
                return flushResponse;
            });
    }

    [Fact]
    public void Constructor_WithNullFileClient_ThrowsArgumentNullException()
    {
        var act = () => new ChunkedUploadStream(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithZeroChunkSize_ThrowsArgumentOutOfRangeException()
    {
        var act = () => new ChunkedUploadStream(_mockFileClient, chunkSize: 0);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Constructor_WithNegativeChunkSize_ThrowsArgumentOutOfRangeException()
    {
        var act = () => new ChunkedUploadStream(_mockFileClient, chunkSize: -1);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void CanWrite_ReturnsTrue_WhenNotDisposed()
    {
        using var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);

        stream.CanWrite.Should().BeTrue();
    }

    [Fact]
    public async Task CanWrite_ReturnsFalse_AfterDispose()
    {
        var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);
        await stream.DisposeAsync();

        stream.CanWrite.Should().BeFalse();
    }

    [Fact]
    public void CanRead_ReturnsFalse()
    {
        using var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);

        stream.CanRead.Should().BeFalse();
    }

    [Fact]
    public void CanSeek_ReturnsFalse()
    {
        using var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);

        stream.CanSeek.Should().BeFalse();
    }

    [Fact]
    public void Read_ThrowsNotSupportedException()
    {
        using var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);

        var act = () => stream.Read(new byte[10], 0, 10);

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void Seek_ThrowsNotSupportedException()
    {
        using var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);

        var act = () => stream.Seek(0, SeekOrigin.Begin);

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void Write_Synchronous_ThrowsNotSupportedException()
    {
        using var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);

        var act = () => stream.Write(new byte[10], 0, 10);

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public async Task WriteAsync_DataSmallerThanChunk_DoesNotCallAppend()
    {
        await using var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);

        await stream.WriteAsync(new byte[500], 0, 500, TestContext.Current.CancellationToken);

        _appendCallCount.Should().Be(0, "buffer is not full, no AppendAsync should be called yet");
    }

    [Fact]
    public async Task WriteAsync_DataExceedsChunk_CallsAppendAsync()
    {
        await using var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);

        await stream.WriteAsync(new byte[2000], 0, 2000, TestContext.Current.CancellationToken);

        _appendCallCount.Should().BeGreaterOrEqualTo(1, "at least one chunk should have been flushed");
        _appendOffsets.Should().Contain(0L, "first chunk should be at offset 0");
    }

    [Fact]
    public async Task DisposeAsync_FlushesRemainingBufferAndCommits()
    {
        var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);
        await stream.WriteAsync(new byte[500], 0, 500, TestContext.Current.CancellationToken);
        await stream.DisposeAsync();

        // The remaining 500 bytes should be flushed via AppendAsync
        _appendCallCount.Should().Be(1, "remaining 500 bytes should be appended on dispose");

        // FlushAsync should be called with the total offset (500)
        _flushCallCount.Should().BeGreaterOrEqualTo(1, "FlushAsync should be called to commit");
        _lastFlushOffset.Should().Be(500, "commit offset should equal total bytes written");
    }

    [Fact]
    public async Task DisposeAsync_NoData_StillCallsFlush()
    {
        var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);
        await stream.DisposeAsync();

        // No AppendAsync should be called (no data to append)
        _appendCallCount.Should().Be(0, "no data was written, no AppendAsync needed");

        // FlushAsync should still be called with offset 0
        _flushCallCount.Should().BeGreaterOrEqualTo(1, "FlushAsync should be called to commit even with no data");
        _lastFlushOffset.Should().Be(0, "commit offset should be 0 when no data was written");
    }

    [Fact]
    public async Task Length_TracksWrittenBytes()
    {
        await using var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 4096);

        stream.Length.Should().Be(0);

        await stream.WriteAsync(new byte[100], 0, 100, TestContext.Current.CancellationToken);
        stream.Length.Should().Be(100);

        await stream.WriteAsync(new byte[200], 0, 200, TestContext.Current.CancellationToken);
        stream.Length.Should().Be(300);
    }

    [Fact]
    public async Task Position_MatchesLength()
    {
        await using var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 4096);

        await stream.WriteAsync(new byte[150], 0, 150, TestContext.Current.CancellationToken);

        stream.Position.Should().Be(150);
        stream.Position.Should().Be(stream.Length);
    }

    [Fact]
    public async Task WriteAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);
        await stream.DisposeAsync();

        var act = async () => await stream.WriteAsync(new byte[10], 0, 10, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task WriteAsync_MultipleChunks_TracksOffsetCorrectly()
    {
        await using var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 100);

        // Write 250 bytes -- should trigger 2 chunk flushes (100 + 100), leaving 50 buffered
        await stream.WriteAsync(new byte[250], 0, 250, TestContext.Current.CancellationToken);

        stream.Length.Should().Be(250);
        _appendCallCount.Should().Be(2, "two full chunks should have been flushed");
        _appendOffsets.Should().Contain(0L, "first chunk at offset 0");
        _appendOffsets.Should().Contain(100L, "second chunk at offset 100");
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_IsIdempotent()
    {
        var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);
        await stream.DisposeAsync();
        await stream.DisposeAsync();

        // FlushAsync should only be called once (first dispose)
        _flushCallCount.Should().Be(1, "second DisposeAsync should be a no-op");
    }

    [Fact]
    public void SetLength_ThrowsNotSupportedException()
    {
        using var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);

        var act = () => stream.SetLength(100);

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void PositionSet_ThrowsNotSupportedException()
    {
        using var stream = new ChunkedUploadStream(_mockFileClient, chunkSize: 1024);

        var act = () => stream.Position = 50;

        act.Should().Throw<NotSupportedException>();
    }
}
