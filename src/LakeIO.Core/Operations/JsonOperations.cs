using System.Diagnostics;
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

        using var activity = LakeIOActivitySource.Source.StartActivity("json.write");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "json.write");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
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

            var response = await _options!.RetryHelper.ExecuteAsync(async ct =>
            {
                stream.Position = 0;
                return await fileClient.UploadAsync(stream, uploadOptions, ct).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);

            var result = new Response<StorageResult>(
                new StorageResult
                {
                    Path = fileClient.Path,
                    ETag = response.Value.ETag,
                    LastModified = response.Value.LastModified,
                    ContentLength = stream.Length
                },
                response.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "json.write"));
            LakeIOMetrics.BytesTransferred.Add(stream.Length,
                new KeyValuePair<string, object?>("direction", "write"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "json.write"));

            activity?.SetTag("lakeio.bytes", stream.Length);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "json.write"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "json.write"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
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

        using var activity = LakeIOActivitySource.Source.StartActivity("json.read");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "json.read");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var jsonOptions = options?.SerializerOptions ?? _options!.JsonSerializerOptions;
            var fileClient = _fileSystemClient!.GetFileClient(path);

            var downloadInfo = await fileClient.ReadStreamingAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            await using var content = downloadInfo.Value.Content;
            var value = await JsonSerializer.DeserializeAsync<T>(content, jsonOptions, cancellationToken).ConfigureAwait(false);

            var result = new Response<T?>(value, downloadInfo.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "json.read"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "json.read"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "json.read"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "json.read"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
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

        using var activity = LakeIOActivitySource.Source.StartActivity("json.append_ndjson");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "json.append_ndjson");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
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

            // Wrap entire GetProperties+Append+Flush sequence so offset is re-read on retry
            long finalPosition = 0;
            var flushResponse = await _options!.RetryHelper.ExecuteAsync(async ct =>
            {
                // Re-read offset on each attempt (concurrent writers may change it)
                long offset;
                try
                {
                    var properties = await fileClient.GetPropertiesAsync(cancellationToken: ct).ConfigureAwait(false);
                    offset = properties.Value.ContentLength;
                }
                catch (RequestFailedException ex) when (ex.ErrorCode is "PathNotFound" or "BlobNotFound")
                {
                    await fileClient.CreateIfNotExistsAsync(cancellationToken: ct).ConfigureAwait(false);
                    offset = 0;
                }

                memoryStream.Position = 0;
                await fileClient.AppendAsync(memoryStream, offset, cancellationToken: ct).ConfigureAwait(false);
                finalPosition = offset + memoryStream.Length;
                return await fileClient.FlushAsync(finalPosition, cancellationToken: ct).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);

            var result = new Response<StorageResult>(
                new StorageResult
                {
                    Path = fileClient.Path,
                    ETag = flushResponse.Value.ETag,
                    LastModified = flushResponse.Value.LastModified,
                    ContentLength = finalPosition
                },
                flushResponse.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "json.append_ndjson"));
            LakeIOMetrics.BytesTransferred.Add(memoryStream.Length,
                new KeyValuePair<string, object?>("direction", "write"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "json.append_ndjson"));

            activity?.SetTag("lakeio.bytes", memoryStream.Length);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "json.append_ndjson"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "json.append_ndjson"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
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

        using var activity = LakeIOActivitySource.Source.StartActivity("json.read_ndjson");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "json.read_ndjson");

        var startTimestamp = Stopwatch.GetTimestamp();
        var success = false;
        try
        {
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

            success = true;
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            if (success)
            {
                LakeIOMetrics.OperationsTotal.Add(1,
                    new KeyValuePair<string, object?>("operation_type", "json.read_ndjson"));
                LakeIOMetrics.OperationDuration.Record(elapsed,
                    new KeyValuePair<string, object?>("operation_type", "json.read_ndjson"));
                activity?.SetStatus(ActivityStatusCode.Ok);
            }
            else
            {
                activity?.SetStatus(ActivityStatusCode.Error, "Operation failed");
                activity?.SetTag("lakeio.error", true);
                LakeIOMetrics.OperationsTotal.Add(1,
                    new KeyValuePair<string, object?>("operation_type", "json.read_ndjson"),
                    new KeyValuePair<string, object?>("error", "true"));
                LakeIOMetrics.OperationDuration.Record(elapsed,
                    new KeyValuePair<string, object?>("operation_type", "json.read_ndjson"),
                    new KeyValuePair<string, object?>("error", "true"));
            }
        }
    }
}
