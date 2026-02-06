using System.Buffers;
using Azure.Storage.Files.DataLake;

namespace LakeIO;

/// <summary>
/// A write-only <see cref="Stream"/> that buffers writes and uploads to Azure Data Lake
/// in chunks via <see cref="DataLakeFileClient.AppendAsync"/> and commits on dispose
/// via <see cref="DataLakeFileClient.FlushAsync(long, bool, string, Azure.Storage.Files.DataLake.Models.PathHttpHeaders, Azure.Storage.Files.DataLake.Models.DataLakeRequestConditions, CancellationToken)"/>.
/// </summary>
/// <remarks>
/// <para>Use this stream when a producer (serializer) writes data incrementally and you
/// want to upload progressively to Azure without buffering the entire file in memory.</para>
/// <para>The target file must already exist (call <c>CreateIfNotExistsAsync</c> before writing).
/// All appended data is uncommitted until <see cref="DisposeAsync"/> calls FlushAsync.</para>
/// <para>Not thread-safe. Designed for single-writer scenarios.</para>
/// </remarks>
public sealed class ChunkedUploadStream : Stream, IAsyncDisposable
{
    private readonly DataLakeFileClient _fileClient;
    private readonly CancellationToken _cancellationToken;
    private readonly int _chunkSize;
    private byte[] _buffer;
    private int _bufferPosition;
    private long _fileOffset;
    private bool _disposed;

    /// <summary>
    /// Initializes a new <see cref="ChunkedUploadStream"/> that writes to the specified file client.
    /// </summary>
    /// <param name="fileClient">The Azure Data Lake file client to upload to. The file must already exist.</param>
    /// <param name="chunkSize">
    /// Buffer threshold in bytes. When the internal buffer reaches this size, data is
    /// flushed to Azure via AppendAsync. Default: 4MB.
    /// </param>
    /// <param name="cancellationToken">Cancellation token passed to all Azure SDK calls.</param>
    /// <exception cref="ArgumentNullException"><paramref name="fileClient"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="chunkSize"/> is less than or equal to zero.</exception>
    public ChunkedUploadStream(
        DataLakeFileClient fileClient,
        int chunkSize = 4 * 1024 * 1024,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fileClient);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkSize);

        _fileClient = fileClient;
        _chunkSize = chunkSize;
        _cancellationToken = cancellationToken;
        _buffer = ArrayPool<byte>.Shared.Rent(chunkSize);
    }

    /// <inheritdoc />
    public override bool CanRead => false;

    /// <inheritdoc />
    public override bool CanSeek => false;

    /// <inheritdoc />
    public override bool CanWrite => !_disposed;

    /// <summary>
    /// Gets the total number of bytes written so far (both flushed and buffered).
    /// </summary>
    public override long Length => _fileOffset + _bufferPosition;

    /// <summary>
    /// Gets the total number of bytes written so far. Setting is not supported.
    /// </summary>
    /// <exception cref="NotSupportedException">Always thrown on set.</exception>
    public override long Position
    {
        get => _fileOffset + _bufferPosition;
        set => throw new NotSupportedException("ChunkedUploadStream does not support seeking.");
    }

    /// <summary>
    /// Not supported. Use <see cref="WriteAsync(byte[], int, int, CancellationToken)"/> instead.
    /// </summary>
    /// <exception cref="NotSupportedException">Always thrown.</exception>
    public override void Write(byte[] buffer, int offset, int count) =>
        throw new NotSupportedException("Use WriteAsync. Synchronous writes are not supported.");

    /// <summary>
    /// Writes a sequence of bytes to the internal buffer. When the buffer reaches
    /// <c>chunkSize</c>, the chunk is automatically flushed to Azure via AppendAsync.
    /// </summary>
    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (offset + count > buffer.Length)
            throw new ArgumentException("The sum of offset and count is greater than the buffer length.");

        var remaining = count;
        var sourceOffset = offset;

        while (remaining > 0)
        {
            var spaceInBuffer = _chunkSize - _bufferPosition;
            var bytesToCopy = Math.Min(remaining, spaceInBuffer);

            Buffer.BlockCopy(buffer, sourceOffset, _buffer, _bufferPosition, bytesToCopy);
            _bufferPosition += bytesToCopy;
            sourceOffset += bytesToCopy;
            remaining -= bytesToCopy;

            if (_bufferPosition >= _chunkSize)
            {
                await FlushChunkAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Writes a sequence of bytes to the internal buffer. When the buffer reaches
    /// <c>chunkSize</c>, the chunk is automatically flushed to Azure via AppendAsync.
    /// </summary>
    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var remaining = buffer.Length;
        var sourceOffset = 0;

        while (remaining > 0)
        {
            var spaceInBuffer = _chunkSize - _bufferPosition;
            var bytesToCopy = Math.Min(remaining, spaceInBuffer);

            buffer.Slice(sourceOffset, bytesToCopy).Span.CopyTo(_buffer.AsSpan(_bufferPosition, bytesToCopy));
            _bufferPosition += bytesToCopy;
            sourceOffset += bytesToCopy;
            remaining -= bytesToCopy;

            if (_bufferPosition >= _chunkSize)
            {
                await FlushChunkAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Synchronous flush is a no-op. Use <see cref="FlushAsync(CancellationToken)"/>
    /// to flush buffered data to Azure.
    /// </summary>
    public override void Flush()
    {
        // No-op: synchronous flush not supported; use FlushAsync.
    }

    /// <summary>
    /// Flushes any remaining buffered data to Azure via AppendAsync.
    /// This does NOT commit the file (commit happens on dispose).
    /// </summary>
    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_bufferPosition > 0)
        {
            await FlushChunkAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Flushes remaining buffered data, commits the file via <see cref="DataLakeFileClient.FlushAsync(long, bool, string, Azure.Storage.Files.DataLake.Models.PathHttpHeaders, Azure.Storage.Files.DataLake.Models.DataLakeRequestConditions, CancellationToken)"/>,
    /// and returns the rented buffer to <see cref="ArrayPool{T}.Shared"/>.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        try
        {
            if (_bufferPosition > 0)
            {
                await FlushChunkAsync().ConfigureAwait(false);
            }

            // Commit all appended data
            await _fileClient.FlushAsync(_fileOffset, cancellationToken: _cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnBuffer();
            _disposed = true;
        }

        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            ReturnBuffer();
            _disposed = true;
        }

        base.Dispose(disposing);
    }

    /// <summary>
    /// Not supported. This is a write-only stream.
    /// </summary>
    /// <exception cref="NotSupportedException">Always thrown.</exception>
    public override int Read(byte[] buffer, int offset, int count) =>
        throw new NotSupportedException("ChunkedUploadStream is write-only.");

    /// <summary>
    /// Not supported. This is a non-seekable stream.
    /// </summary>
    /// <exception cref="NotSupportedException">Always thrown.</exception>
    public override long Seek(long offset, SeekOrigin origin) =>
        throw new NotSupportedException("ChunkedUploadStream does not support seeking.");

    /// <summary>
    /// Not supported. This is a non-seekable stream.
    /// </summary>
    /// <exception cref="NotSupportedException">Always thrown.</exception>
    public override void SetLength(long value) =>
        throw new NotSupportedException("ChunkedUploadStream does not support SetLength.");

    /// <summary>
    /// Uploads the current buffer contents to Azure via AppendAsync and resets the buffer position.
    /// </summary>
    private async Task FlushChunkAsync()
    {
        if (_bufferPosition == 0)
            return;

        using var memoryStream = new MemoryStream(_buffer, 0, _bufferPosition, writable: false);
        await _fileClient.AppendAsync(memoryStream, _fileOffset, cancellationToken: _cancellationToken).ConfigureAwait(false);

        _fileOffset += _bufferPosition;
        _bufferPosition = 0;
    }

    private void ReturnBuffer()
    {
        if (_buffer is not null)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = null!;
        }
    }
}
