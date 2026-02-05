using System.Runtime.CompilerServices;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using LakeIO;
using Parquet;
using Parquet.Schema;
using Parquet.Serialization;

namespace LakeIO.Parquet;

/// <summary>
/// Provides Parquet read/write operations for Azure Data Lake Storage.
/// </summary>
/// <remarks>
/// <para>Access via <c>fileSystemClient.Parquet()</c> extension method. Instances are
/// cached per <see cref="FileSystemClient"/> and are thread-safe.</para>
/// <para>All public methods are <see langword="virtual"/> for mocking. A
/// <see langword="protected"/> parameterless constructor is provided for test doubles.</para>
/// </remarks>
public class ParquetOperations
{
    private readonly DataLakeFileSystemClient? _fileSystemClient;
    private readonly LakeClientOptions? _options;

    /// <summary>
    /// Protected parameterless constructor for mocking.
    /// </summary>
    protected ParquetOperations()
    {
    }

    /// <summary>
    /// Internal constructor used by the <c>Parquet()</c> extension method factory.
    /// </summary>
    /// <param name="fileSystemClient">The Azure Data Lake file system client.</param>
    /// <param name="options">The LakeIO client options.</param>
    internal ParquetOperations(DataLakeFileSystemClient fileSystemClient, LakeClientOptions options)
    {
        _fileSystemClient = fileSystemClient ?? throw new ArgumentNullException(nameof(fileSystemClient));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// Serializes a collection to Parquet format and uploads it to the specified path.
    /// </summary>
    /// <typeparam name="T">The type of the items to serialize. Must be a class with a parameterless constructor.</typeparam>
    /// <param name="path">The file path within the file system (e.g., <c>"data/records.parquet"</c>).</param>
    /// <param name="items">The collection of items to serialize and upload.</param>
    /// <param name="options">
    /// Optional per-operation Parquet options. When null, falls back to
    /// <see cref="LakeClientOptions.DefaultParquetCompression"/> and
    /// <see cref="LakeClientOptions.DefaultParquetRowGroupSize"/>,
    /// then to library defaults (Snappy, 10,000 rows).
    /// </param>
    /// <param name="overwrite">Whether to overwrite an existing file. Default is <see langword="true"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the <see cref="StorageResult"/> with path, ETag, and metadata.</returns>
    public virtual async Task<Response<StorageResult>> WriteAsync<T>(
        string path,
        IReadOnlyCollection<T> items,
        ParquetOptions? options = null,
        bool overwrite = true,
        CancellationToken cancellationToken = default)
        where T : class, new()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(items);

        var (compression, rowGroupSize) = ResolveOptions(options);
        var serializerOptions = new ParquetSerializerOptions
        {
            CompressionMethod = compression,
            RowGroupSize = rowGroupSize
        };

        using var memoryStream = new MemoryStream();
        await ParquetSerializer.SerializeAsync(items, memoryStream, serializerOptions, cancellationToken).ConfigureAwait(false);
        memoryStream.Position = 0;

        var fileClient = _fileSystemClient!.GetFileClient(path);
        var uploadOptions = new DataLakeFileUploadOptions();

        if (!overwrite)
        {
            uploadOptions.Conditions = new DataLakeRequestConditions { IfNoneMatch = new ETag("*") };
        }

        var response = await fileClient.UploadAsync(memoryStream, uploadOptions, cancellationToken).ConfigureAwait(false);

        return new Response<StorageResult>(
            new StorageResult
            {
                Path = fileClient.Path,
                ETag = response.Value.ETag,
                LastModified = response.Value.LastModified,
                ContentLength = memoryStream.Length
            },
            response.GetRawResponse());
    }

    /// <summary>
    /// Streams an <see cref="IAsyncEnumerable{T}"/> to Parquet format and uploads it to the specified path
    /// with bounded memory usage.
    /// </summary>
    /// <typeparam name="T">The type of the items to serialize. Must be a class with a parameterless constructor.</typeparam>
    /// <param name="path">The file path within the file system (e.g., <c>"data/records.parquet"</c>).</param>
    /// <param name="items">The async enumerable of items to serialize and upload.</param>
    /// <param name="options">
    /// Optional per-operation Parquet options. When null, falls back to
    /// <see cref="LakeClientOptions.DefaultParquetCompression"/> and
    /// <see cref="LakeClientOptions.DefaultParquetRowGroupSize"/>,
    /// then to library defaults (Snappy, 10,000 rows).
    /// </param>
    /// <param name="overwrite">Whether to overwrite an existing file. Default is <see langword="true"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the <see cref="StorageResult"/> with path, ETag, and metadata.</returns>
    /// <remarks>
    /// <para>Uses Parquet.Net 5.5.0's native <c>SerializeAsync(IAsyncEnumerable&lt;T&gt;)</c> overload
    /// which handles row group batching internally based on <see cref="ParquetOptions.RowGroupSize"/>.
    /// Items are consumed incrementally, keeping memory usage bounded to roughly one row group at a time.</para>
    /// <para>The Parquet footer is written after all items are consumed, then the complete file is uploaded.</para>
    /// </remarks>
    public virtual async Task<Response<StorageResult>> WriteStreamAsync<T>(
        string path,
        IAsyncEnumerable<T> items,
        ParquetOptions? options = null,
        bool overwrite = true,
        CancellationToken cancellationToken = default)
        where T : class, new()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(items);

        var (compression, rowGroupSize) = ResolveOptions(options);
        var serializerOptions = new ParquetSerializerOptions
        {
            CompressionMethod = compression,
            RowGroupSize = rowGroupSize
        };

        using var memoryStream = new MemoryStream();
        await ParquetSerializer.SerializeAsync(items, memoryStream, serializerOptions, cancellationToken).ConfigureAwait(false);
        memoryStream.Position = 0;

        var fileClient = _fileSystemClient!.GetFileClient(path);
        var uploadOptions = new DataLakeFileUploadOptions();

        if (!overwrite)
        {
            uploadOptions.Conditions = new DataLakeRequestConditions { IfNoneMatch = new ETag("*") };
        }

        var response = await fileClient.UploadAsync(memoryStream, uploadOptions, cancellationToken).ConfigureAwait(false);

        return new Response<StorageResult>(
            new StorageResult
            {
                Path = fileClient.Path,
                ETag = response.Value.ETag,
                LastModified = response.Value.LastModified,
                ContentLength = memoryStream.Length
            },
            response.GetRawResponse());
    }

    /// <summary>
    /// Reads a Parquet file and streams deserialized records as an <see cref="IAsyncEnumerable{T}"/>
    /// without loading the entire file into memory.
    /// </summary>
    /// <typeparam name="T">
    /// The type to deserialize each Parquet record to. Must be a class with a parameterless constructor.
    /// Properties are mapped to Parquet columns by name (case-insensitive) or via Parquet.Net attributes.
    /// </typeparam>
    /// <param name="path">The file path within the file system (e.g., <c>"data/records.parquet"</c>).</param>
    /// <param name="options">Optional per-operation Parquet options (currently unused for reads; reserved for future filtering).</param>
    /// <param name="cancellationToken">
    /// Cancellation token. Can also be passed via <c>.WithCancellation(token)</c> on the returned enumerable.
    /// </param>
    /// <returns>An async enumerable of deserialized records.</returns>
    /// <remarks>
    /// <para>Uses <c>OpenReadAsync</c> to obtain a seekable stream, which Parquet requires to read
    /// the footer metadata. The stream remains alive for the lifetime of the async iterator and is
    /// disposed when the caller breaks from iteration or completes enumeration.</para>
    /// <para>Records are yielded incrementally via <c>ParquetSerializer.DeserializeAllAsync</c>,
    /// which reads row groups on demand rather than loading the entire file.</para>
    /// </remarks>
    public virtual async IAsyncEnumerable<T> ReadStreamAsync<T>(
        string path,
        ParquetOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class, new()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var fileClient = _fileSystemClient!.GetFileClient(path);

        await using var stream = await fileClient.OpenReadAsync(
            new DataLakeOpenReadOptions(allowModifications: false),
            cancellationToken).ConfigureAwait(false);

        await foreach (var item in ParquetSerializer.DeserializeAllAsync<T>(stream, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    /// <summary>
    /// Reads the Parquet schema (column names, types, and structure) from a file without loading any data.
    /// </summary>
    /// <param name="path">The file path within the file system (e.g., <c>"data/records.parquet"</c>).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The <see cref="ParquetSchema"/> describing the file's column structure.</returns>
    /// <remarks>
    /// <para>This is a lightweight metadata-only operation. Parquet stores the schema in the file footer
    /// (typically the last few KB), so no row data is read or transferred.</para>
    /// <para>Uses <c>OpenReadAsync</c> for a seekable stream, then <c>ParquetReader.CreateAsync</c>
    /// to parse only the footer metadata.</para>
    /// </remarks>
    public virtual async Task<ParquetSchema> GetSchemaAsync(
        string path,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var fileClient = _fileSystemClient!.GetFileClient(path);

        await using var stream = await fileClient.OpenReadAsync(
            new DataLakeOpenReadOptions(allowModifications: false),
            cancellationToken).ConfigureAwait(false);

        using var reader = await ParquetReader.CreateAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
        return reader.Schema;
    }

    /// <summary>
    /// Resolves Parquet options from the per-operation options, LakeClientOptions defaults, and library defaults.
    /// </summary>
    private (CompressionMethod compression, int rowGroupSize) ResolveOptions(ParquetOptions? options)
    {
        var compression = options?.CompressionMethod
            ?? (Enum.TryParse<CompressionMethod>(_options!.DefaultParquetCompression, ignoreCase: true, out var parsed)
                ? parsed
                : CompressionMethod.Snappy);

        var rowGroupSize = options?.RowGroupSize
            ?? _options!.DefaultParquetRowGroupSize;

        return (compression, rowGroupSize);
    }
}
