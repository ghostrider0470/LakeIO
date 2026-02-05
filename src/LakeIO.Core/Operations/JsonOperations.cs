using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

namespace LakeIO;

/// <summary>
/// Provides JSON and NDJSON read/write operations for Azure Data Lake Storage.
/// </summary>
/// <remarks>
/// <para>Access via <c>fileSystemClient.Json()</c> extension method. Instances are
/// cached per <see cref="FileSystemClient"/> and are thread-safe.</para>
/// <para>All public methods are <see langword="virtual"/> for mocking. A
/// <see langword="protected"/> parameterless constructor is provided for test doubles.</para>
/// </remarks>
public class JsonOperations
{
    private readonly DataLakeFileSystemClient? _fileSystemClient;
    private readonly LakeClientOptions? _options;

    /// <summary>
    /// Protected parameterless constructor for mocking.
    /// </summary>
    protected JsonOperations()
    {
    }

    /// <summary>
    /// Internal constructor used by the <c>Json()</c> extension method factory.
    /// </summary>
    /// <param name="fileSystemClient">The Azure Data Lake file system client.</param>
    /// <param name="options">The LakeIO client options.</param>
    internal JsonOperations(DataLakeFileSystemClient fileSystemClient, LakeClientOptions options)
    {
        _fileSystemClient = fileSystemClient ?? throw new ArgumentNullException(nameof(fileSystemClient));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// Serializes an object to JSON and uploads it to the specified path.
    /// </summary>
    /// <typeparam name="T">The type of the object to serialize.</typeparam>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="value">The object to serialize and upload.</param>
    /// <param name="options">Optional per-operation JSON options. Falls back to <see cref="LakeClientOptions.JsonSerializerOptions"/>.</param>
    /// <param name="overwrite">Whether to overwrite an existing file. Default is <see langword="true"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the <see cref="StorageResult"/> with path, ETag, and metadata.</returns>
    public virtual async Task<Response<StorageResult>> WriteAsync<T>(
        string path,
        T value,
        JsonOptions? options = null,
        bool overwrite = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(value);

        var jsonOptions = options?.SerializerOptions ?? _options!.JsonSerializerOptions;
        var fileClient = _fileSystemClient!.GetFileClient(path);

        using var stream = new MemoryStream();
        await JsonSerializer.SerializeAsync(stream, value, jsonOptions, cancellationToken).ConfigureAwait(false);
        stream.Position = 0;

        var uploadOptions = new DataLakeFileUploadOptions
        {
            HttpHeaders = new PathHttpHeaders { ContentType = "application/json" }
        };

        if (!overwrite)
        {
            uploadOptions.Conditions = new DataLakeRequestConditions { IfNoneMatch = new ETag("*") };
        }

        var response = await fileClient.UploadAsync(stream, uploadOptions, cancellationToken).ConfigureAwait(false);

        return new Response<StorageResult>(
            new StorageResult
            {
                Path = fileClient.Path,
                ETag = response.Value.ETag,
                LastModified = response.Value.LastModified,
                ContentLength = stream.Length
            },
            response.GetRawResponse());
    }

    /// <summary>
    /// Reads a JSON file and deserializes it to the specified type using lazy streaming.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="options">Optional per-operation JSON options. Falls back to <see cref="LakeClientOptions.JsonSerializerOptions"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the deserialized object.</returns>
    public virtual async Task<Response<T?>> ReadAsync<T>(
        string path,
        JsonOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var jsonOptions = options?.SerializerOptions ?? _options!.JsonSerializerOptions;
        var fileClient = _fileSystemClient!.GetFileClient(path);

        var downloadInfo = await fileClient.ReadStreamingAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        await using var content = downloadInfo.Value.Content;
        var value = await JsonSerializer.DeserializeAsync<T>(content, jsonOptions, cancellationToken).ConfigureAwait(false);

        return new Response<T?>(value, downloadInfo.GetRawResponse());
    }

    /// <summary>
    /// Appends a serialized JSON line to an NDJSON file using append + flush semantics.
    /// Creates the file if it does not exist.
    /// </summary>
    /// <typeparam name="T">The type of the object to serialize.</typeparam>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="value">The object to serialize and append as an NDJSON line.</param>
    /// <param name="options">Optional per-operation JSON options. Falls back to <see cref="LakeClientOptions.JsonSerializerOptions"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the <see cref="StorageResult"/> with updated path and metadata.</returns>
    public virtual async Task<Response<StorageResult>> AppendNdjsonAsync<T>(
        string path,
        T value,
        JsonOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(value);

        var jsonOptions = options?.SerializerOptions ?? _options!.JsonSerializerOptions;
        var fileClient = _fileSystemClient!.GetFileClient(path);

        // Serialize to NDJSON line (BOM-free UTF-8)
        using var memoryStream = new MemoryStream();
        await using (var writer = new StreamWriter(memoryStream, new UTF8Encoding(false), leaveOpen: true))
        {
            var json = JsonSerializer.Serialize(value, jsonOptions);
            await writer.WriteLineAsync(json).ConfigureAwait(false);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        memoryStream.Position = 0;

        // Get current file offset (or create the file if it doesn't exist)
        long offset;
        try
        {
            var properties = await fileClient.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            offset = properties.Value.ContentLength;
        }
        catch (RequestFailedException ex) when (ex.ErrorCode is "PathNotFound" or "BlobNotFound")
        {
            await fileClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            offset = 0;
        }

        // Append data at the current offset
        await fileClient.AppendAsync(memoryStream, offset, cancellationToken: cancellationToken).ConfigureAwait(false);

        // Flush to commit the appended data
        var flushResponse = await fileClient.FlushAsync(offset + memoryStream.Length, cancellationToken: cancellationToken).ConfigureAwait(false);

        return new Response<StorageResult>(
            new StorageResult
            {
                Path = fileClient.Path,
                ETag = flushResponse.Value.ETag,
                LastModified = flushResponse.Value.LastModified,
                ContentLength = offset + memoryStream.Length
            },
            flushResponse.GetRawResponse());
    }

    /// <summary>
    /// Reads an NDJSON file and streams deserialized records as an <see cref="IAsyncEnumerable{T}"/>
    /// without loading the entire file into memory.
    /// </summary>
    /// <typeparam name="T">The type to deserialize each NDJSON record to.</typeparam>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="options">Optional per-operation JSON options. Falls back to <see cref="LakeClientOptions.JsonSerializerOptions"/>.</param>
    /// <param name="cancellationToken">Cancellation token. Use <c>WithCancellation(token)</c> on the returned enumerable.</param>
    /// <returns>An async enumerable of deserialized records.</returns>
    public virtual async IAsyncEnumerable<T> ReadNdjsonAsync<T>(
        string path,
        JsonOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var jsonOptions = options?.SerializerOptions ?? _options!.JsonSerializerOptions;
        var fileClient = _fileSystemClient!.GetFileClient(path);

        await using var stream = await fileClient.OpenReadAsync(
            new DataLakeOpenReadOptions(allowModifications: false),
            cancellationToken).ConfigureAwait(false);

        await foreach (var item in JsonSerializer.DeserializeAsyncEnumerable<T>(
            stream,
            topLevelValues: true,
            jsonOptions,
            cancellationToken).ConfigureAwait(false))
        {
            if (item is not null)
            {
                yield return item;
            }
        }
    }
}
