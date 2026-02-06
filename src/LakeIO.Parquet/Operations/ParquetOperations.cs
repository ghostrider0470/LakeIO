using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
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

        using var activity = LakeIOActivitySource.Source.StartActivity("parquet.write");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "parquet.write");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var (compression, rowGroupSize) = ResolveOptions(options);
            var serializerOptions = new ParquetSerializerOptions
            {
                CompressionMethod = compression,
                RowGroupSize = rowGroupSize
            };

            using var memoryStream = StreamPool.Manager.GetStream("ParquetOperations.WriteAsync");
            await ParquetSerializer.SerializeAsync(items, memoryStream, serializerOptions, cancellationToken).ConfigureAwait(false);
            memoryStream.Position = 0;

            var fileClient = _fileSystemClient!.GetFileClient(path);
            var uploadOptions = new DataLakeFileUploadOptions();

            if (!overwrite)
            {
                uploadOptions.Conditions = new DataLakeRequestConditions { IfNoneMatch = new ETag("*") };
            }

            var response = await fileClient.UploadAsync(memoryStream, uploadOptions, cancellationToken).ConfigureAwait(false);

            var result = new Response<StorageResult>(
                new StorageResult
                {
                    Path = fileClient.Path,
                    ETag = response.Value.ETag,
                    LastModified = response.Value.LastModified,
                    ContentLength = memoryStream.Length
                },
                response.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "parquet.write"));
            LakeIOMetrics.BytesTransferred.Add(memoryStream.Length,
                new KeyValuePair<string, object?>("direction", "write"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "parquet.write"));

            activity?.SetTag("lakeio.bytes", memoryStream.Length);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "parquet.write"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "parquet.write"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
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

        using var activity = LakeIOActivitySource.Source.StartActivity("parquet.write_stream");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "parquet.write_stream");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var (compression, rowGroupSize) = ResolveOptions(options);
            var serializerOptions = new ParquetSerializerOptions
            {
                CompressionMethod = compression,
                RowGroupSize = rowGroupSize
            };

            using var memoryStream = StreamPool.Manager.GetStream("ParquetOperations.WriteStreamAsync");
            await ParquetSerializer.SerializeAsync(items, memoryStream, serializerOptions, cancellationToken).ConfigureAwait(false);
            memoryStream.Position = 0;

            var fileClient = _fileSystemClient!.GetFileClient(path);
            var uploadOptions = new DataLakeFileUploadOptions();

            if (!overwrite)
            {
                uploadOptions.Conditions = new DataLakeRequestConditions { IfNoneMatch = new ETag("*") };
            }

            var response = await fileClient.UploadAsync(memoryStream, uploadOptions, cancellationToken).ConfigureAwait(false);

            var result = new Response<StorageResult>(
                new StorageResult
                {
                    Path = fileClient.Path,
                    ETag = response.Value.ETag,
                    LastModified = response.Value.LastModified,
                    ContentLength = memoryStream.Length
                },
                response.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "parquet.write_stream"));
            LakeIOMetrics.BytesTransferred.Add(memoryStream.Length,
                new KeyValuePair<string, object?>("direction", "write"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "parquet.write_stream"));

            activity?.SetTag("lakeio.bytes", memoryStream.Length);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "parquet.write_stream"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "parquet.write_stream"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
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

        using var activity = LakeIOActivitySource.Source.StartActivity("parquet.read_stream");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "parquet.read_stream");

        var startTimestamp = Stopwatch.GetTimestamp();
        var success = false;
        try
        {
            var fileClient = _fileSystemClient!.GetFileClient(path);

            await using var stream = await fileClient.OpenReadAsync(
                new DataLakeOpenReadOptions(allowModifications: false),
                cancellationToken).ConfigureAwait(false);

            await foreach (var item in ParquetSerializer.DeserializeAllAsync<T>(stream, cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }

            success = true;
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            if (success)
            {
                LakeIOMetrics.OperationsTotal.Add(1,
                    new KeyValuePair<string, object?>("operation_type", "parquet.read_stream"));
                LakeIOMetrics.OperationDuration.Record(elapsed,
                    new KeyValuePair<string, object?>("operation_type", "parquet.read_stream"));
                activity?.SetStatus(ActivityStatusCode.Ok);
            }
            else
            {
                activity?.SetStatus(ActivityStatusCode.Error, "Operation failed");
                activity?.SetTag("lakeio.error", true);
                LakeIOMetrics.OperationsTotal.Add(1,
                    new KeyValuePair<string, object?>("operation_type", "parquet.read_stream"),
                    new KeyValuePair<string, object?>("error", "true"));
                LakeIOMetrics.OperationDuration.Record(elapsed,
                    new KeyValuePair<string, object?>("operation_type", "parquet.read_stream"),
                    new KeyValuePair<string, object?>("error", "true"));
            }
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

        using var activity = LakeIOActivitySource.Source.StartActivity("parquet.get_schema");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "parquet.get_schema");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var fileClient = _fileSystemClient!.GetFileClient(path);

            await using var stream = await fileClient.OpenReadAsync(
                new DataLakeOpenReadOptions(allowModifications: false),
                cancellationToken).ConfigureAwait(false);

            using var reader = await ParquetReader.CreateAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "parquet.get_schema"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "parquet.get_schema"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return reader.Schema;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "parquet.get_schema"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "parquet.get_schema"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Merges new data into an existing Parquet file with automatic schema evolution,
    /// or creates a new file if one does not exist at the specified path.
    /// </summary>
    /// <typeparam name="T">The type of the items to merge. Must be a class with a parameterless constructor.</typeparam>
    /// <param name="path">The file path within the file system (e.g., <c>"data/records.parquet"</c>).</param>
    /// <param name="items">The collection of items to merge into the file.</param>
    /// <param name="options">
    /// Optional per-operation Parquet options. When null, falls back to
    /// <see cref="LakeClientOptions.DefaultParquetCompression"/> and
    /// <see cref="LakeClientOptions.DefaultParquetRowGroupSize"/>,
    /// then to library defaults (Snappy, 10,000 rows).
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the <see cref="StorageResult"/> with path, ETag, and metadata.</returns>
    /// <remarks>
    /// <para>Schema evolution uses the <see cref="SchemaEvolver"/> with the AddNewColumnsAsNullable strategy:
    /// existing columns are preserved in their original order and any new columns from <typeparamref name="T"/>
    /// are appended as nullable fields.</para>
    /// <para>When the file does not exist, a new file is created via <see cref="WriteAsync{T}"/>.</para>
    /// <para>When the file exists, existing row groups are preserved and a new row group containing
    /// <paramref name="items"/> is appended using Parquet.Net's <c>Append</c> mode. The complete file
    /// (existing content plus new row group) is then re-uploaded.</para>
    /// </remarks>
    public virtual async Task<Response<StorageResult>> MergeAsync<T>(
        string path,
        IReadOnlyCollection<T> items,
        ParquetOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : class, new()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(items);

        using var activity = LakeIOActivitySource.Source.StartActivity("parquet.merge");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "parquet.merge");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var fileClient = _fileSystemClient!.GetFileClient(path);

            // Check if file exists
            bool fileExists;
            try
            {
                await fileClient.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                fileExists = true;
            }
            catch (RequestFailedException ex) when (ex.ErrorCode is "PathNotFound" or "BlobNotFound")
            {
                fileExists = false;
            }

            // If file doesn't exist, delegate to WriteAsync for initial creation
            if (!fileExists)
            {
                var writeResult = await WriteAsync(path, items, options, overwrite: true, cancellationToken).ConfigureAwait(false);

                // WriteAsync records its own bytes -- only record merge operation count and duration here
                var elapsedNew = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
                LakeIOMetrics.OperationsTotal.Add(1,
                    new KeyValuePair<string, object?>("operation_type", "parquet.merge"));
                LakeIOMetrics.OperationDuration.Record(elapsedNew,
                    new KeyValuePair<string, object?>("operation_type", "parquet.merge"));

                activity?.SetStatus(ActivityStatusCode.Ok);
                return writeResult;
            }

            // File exists: download, evolve schema, append new row group, re-upload
            var (compression, rowGroupSize) = ResolveOptions(options);

            // Read existing schema from file footer
            await using var downloadStream = await fileClient.OpenReadAsync(
                new DataLakeOpenReadOptions(allowModifications: false), cancellationToken).ConfigureAwait(false);

            // Copy to a seekable pooled MemoryStream (needed for Parquet append)
            using var fileStream = StreamPool.Manager.GetStream("ParquetOperations.MergeAsync");
            await downloadStream.CopyToAsync(fileStream, cancellationToken).ConfigureAwait(false);
            fileStream.Position = 0;

            // Read existing schema and get incoming schema from CLR type
            using var reader = await ParquetReader.CreateAsync(fileStream, cancellationToken: cancellationToken).ConfigureAwait(false);
            var existingSchema = reader.Schema;
            var incomingSchema = typeof(T).GetParquetSchema(forWriting: true);

            // Evolve schema (preserves existing column order, appends new as nullable)
            var evolver = new SchemaEvolver();
            var mergedSchema = evolver.Evolve(existingSchema, incomingSchema);

            // Reset stream for append (Parquet.Net reads footer, appends row group, writes new footer)
            fileStream.Position = 0;

            var serializerOptions = new ParquetSerializerOptions
            {
                CompressionMethod = compression,
                RowGroupSize = rowGroupSize,
                Append = true
            };

            await ParquetSerializer.SerializeAsync(items, fileStream, serializerOptions, cancellationToken).ConfigureAwait(false);

            // Upload the merged file
            fileStream.Position = 0;
            var uploadOptions = new DataLakeFileUploadOptions();

            var response = await fileClient.UploadAsync(fileStream, uploadOptions, cancellationToken).ConfigureAwait(false);

            var result = new Response<StorageResult>(
                new StorageResult
                {
                    Path = fileClient.Path,
                    ETag = response.Value.ETag,
                    LastModified = response.Value.LastModified,
                    ContentLength = fileStream.Length
                },
                response.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "parquet.merge"));
            LakeIOMetrics.BytesTransferred.Add(fileStream.Length,
                new KeyValuePair<string, object?>("direction", "write"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "parquet.merge"));

            activity?.SetTag("lakeio.bytes", fileStream.Length);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "parquet.merge"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "parquet.merge"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Compacts an NDJSON file into Parquet format with bounded memory usage.
    /// </summary>
    /// <typeparam name="T">The type to deserialize each NDJSON record to and serialize as Parquet. Must be a class with a parameterless constructor.</typeparam>
    /// <param name="ndjsonPath">The path to the source NDJSON file within the file system.</param>
    /// <param name="parquetPath">The path to the destination Parquet file within the file system.</param>
    /// <param name="options">
    /// Optional per-operation Parquet options. When null, falls back to
    /// <see cref="LakeClientOptions.DefaultParquetCompression"/> and
    /// <see cref="LakeClientOptions.DefaultParquetRowGroupSize"/>,
    /// then to library defaults (Snappy, 10,000 rows).
    /// </param>
    /// <param name="overwrite">Whether to overwrite an existing Parquet file. Default is <see langword="true"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the <see cref="StorageResult"/> with path, ETag, and metadata.</returns>
    /// <remarks>
    /// <para>The NDJSON file is read as a streaming <see cref="IAsyncEnumerable{T}"/> via
    /// <see cref="JsonSerializer.DeserializeAsyncEnumerable{TValue}(Stream, JsonSerializerOptions?, CancellationToken)"/>
    /// with <c>topLevelValues: true</c> for NDJSON support.</para>
    /// <para>Records are piped directly into <see cref="WriteStreamAsync{T}"/>, which uses Parquet.Net's
    /// native <c>IAsyncEnumerable</c> serialization with row group batching. This keeps memory bounded
    /// end-to-end to roughly one row group at a time.</para>
    /// <para>The source NDJSON file is NOT deleted after compaction. The caller is responsible for
    /// cleaning up the source file if desired.</para>
    /// </remarks>
    public virtual async Task<Response<StorageResult>> CompactNdjsonAsync<T>(
        string ndjsonPath,
        string parquetPath,
        ParquetOptions? options = null,
        bool overwrite = true,
        CancellationToken cancellationToken = default)
        where T : class, new()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(ndjsonPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(parquetPath);

        using var activity = LakeIOActivitySource.Source.StartActivity("parquet.compact_ndjson");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", ndjsonPath);
        activity?.SetTag("lakeio.destination_path", parquetPath);
        activity?.SetTag("lakeio.operation", "parquet.compact_ndjson");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            // Read NDJSON via streaming deserialization
            var fileClient = _fileSystemClient!.GetFileClient(ndjsonPath);
            await using var stream = await fileClient.OpenReadAsync(
                new DataLakeOpenReadOptions(allowModifications: false), cancellationToken).ConfigureAwait(false);

            var jsonOptions = _options!.JsonSerializerOptions;
            var items = JsonSerializer.DeserializeAsyncEnumerable<T>(
                stream,
                topLevelValues: true,
                jsonOptions,
                cancellationToken);

            // Filter nulls (DeserializeAsyncEnumerable can yield null for invalid lines)
            async IAsyncEnumerable<T> FilterNulls([EnumeratorCancellation] CancellationToken ct = default)
            {
                await foreach (var item in items.WithCancellation(ct).ConfigureAwait(false))
                {
                    if (item is not null) yield return item;
                }
            }

            // Pipe NDJSON items through Parquet streaming write (bounded memory)
            // WriteStreamAsync records its own bytes -- only record compact operation count and duration here
            var result = await WriteStreamAsync(parquetPath, FilterNulls(cancellationToken), options, overwrite, cancellationToken).ConfigureAwait(false);

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "parquet.compact_ndjson"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "parquet.compact_ndjson"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "parquet.compact_ndjson"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "parquet.compact_ndjson"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
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
