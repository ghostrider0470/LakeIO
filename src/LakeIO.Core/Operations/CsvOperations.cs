using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using CsvHelper;
using CsvHelper.Configuration;

namespace LakeIO;

/// <summary>
/// Provides CSV read/write operations for Azure Data Lake Storage.
/// </summary>
/// <remarks>
/// <para>Access via <c>fileSystemClient.Csv()</c> extension method. Instances are
/// cached per <see cref="FileSystemClient"/> and are thread-safe.</para>
/// <para>All public methods are <see langword="virtual"/> for mocking. A
/// <see langword="protected"/> parameterless constructor is provided for test doubles.</para>
/// <para>Per-operation <see cref="CsvOptions"/> overrides fall back to
/// <see cref="LakeClientOptions.Csv"/> defaults when properties are null.</para>
/// </remarks>
public class CsvOperations
{
    private readonly DataLakeFileSystemClient? _fileSystemClient;
    private readonly LakeClientOptions? _options;

    /// <summary>
    /// Protected parameterless constructor for mocking.
    /// </summary>
    protected CsvOperations()
    {
    }

    /// <summary>
    /// Internal constructor used by the <c>Csv()</c> extension method factory.
    /// </summary>
    /// <param name="fileSystemClient">The Azure Data Lake file system client.</param>
    /// <param name="options">The LakeIO client options.</param>
    internal CsvOperations(DataLakeFileSystemClient fileSystemClient, LakeClientOptions options)
    {
        _fileSystemClient = fileSystemClient ?? throw new ArgumentNullException(nameof(fileSystemClient));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// Serializes a collection to CSV and uploads it to the specified path.
    /// </summary>
    /// <typeparam name="T">The type of items to serialize.</typeparam>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="items">The collection of items to serialize and upload.</param>
    /// <param name="options">Optional per-operation CSV options. Falls back to <see cref="LakeClientOptions.Csv"/>.</param>
    /// <param name="overwrite">Whether to overwrite an existing file. Default is <see langword="true"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the <see cref="StorageResult"/> with path, ETag, and metadata.</returns>
    public virtual async Task<Response<StorageResult>> WriteAsync<T>(
        string path,
        IEnumerable<T> items,
        CsvOptions? options = null,
        bool overwrite = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(items);

        var config = BuildCsvConfiguration(options);
        var fileClient = _fileSystemClient!.GetFileClient(path);

        using var memoryStream = new MemoryStream();
        await using (var writer = new StreamWriter(memoryStream, new UTF8Encoding(false), leaveOpen: true))
        await using (var csvWriter = new CsvWriter(writer, config))
        {
            await csvWriter.WriteRecordsAsync(items, cancellationToken).ConfigureAwait(false);
        }

        memoryStream.Position = 0;

        var uploadOptions = new DataLakeFileUploadOptions
        {
            HttpHeaders = new PathHttpHeaders { ContentType = "text/csv" }
        };

        if (!overwrite)
        {
            uploadOptions.Conditions = new DataLakeRequestConditions { IfNoneMatch = new ETag("*") };
        }

        var response = await fileClient.UploadAsync(memoryStream, uploadOptions, cancellationToken)
            .ConfigureAwait(false);

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
    /// Downloads a CSV file and deserializes it to a typed collection.
    /// </summary>
    /// <typeparam name="T">The type to deserialize each CSV record to.</typeparam>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="options">Optional per-operation CSV options. Falls back to <see cref="LakeClientOptions.Csv"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the deserialized <see cref="IReadOnlyList{T}"/>.</returns>
    public virtual async Task<Response<IReadOnlyList<T>>> ReadAsync<T>(
        string path,
        CsvOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var config = BuildCsvConfiguration(options);
        var fileClient = _fileSystemClient!.GetFileClient(path);

        var downloadInfo = await fileClient.ReadStreamingAsync(
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await using var content = downloadInfo.Value.Content;
        using var reader = new StreamReader(content, new UTF8Encoding(false));
        using var csvReader = new CsvReader(reader, config);

        var records = new List<T>();
        await foreach (var record in csvReader.GetRecordsAsync<T>(cancellationToken).ConfigureAwait(false))
        {
            records.Add(record);
        }

        return new Response<IReadOnlyList<T>>(records, downloadInfo.GetRawResponse());
    }

    /// <summary>
    /// Streams CSV records from a file as an <see cref="IAsyncEnumerable{T}"/>
    /// without loading the entire file into memory.
    /// </summary>
    /// <typeparam name="T">The type to deserialize each CSV record to.</typeparam>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="options">Optional per-operation CSV options. Falls back to <see cref="LakeClientOptions.Csv"/>.</param>
    /// <param name="cancellationToken">Cancellation token. Use <c>WithCancellation(token)</c> on the returned enumerable.</param>
    /// <returns>An async enumerable of deserialized CSV records.</returns>
    public virtual async IAsyncEnumerable<T> ReadStreamAsync<T>(
        string path,
        CsvOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var config = BuildCsvConfiguration(options);
        var fileClient = _fileSystemClient!.GetFileClient(path);

        await using var stream = await fileClient.OpenReadAsync(
            new DataLakeOpenReadOptions(allowModifications: false),
            cancellationToken).ConfigureAwait(false);

        using var reader = new StreamReader(stream, new UTF8Encoding(false));
        using var csvReader = new CsvReader(reader, config);

        await foreach (var record in csvReader.GetRecordsAsync<T>(cancellationToken).ConfigureAwait(false))
        {
            yield return record;
        }
    }

    /// <summary>
    /// Builds a <see cref="CsvConfiguration"/> by resolving the per-operation
    /// <see cref="CsvOptions"/> against <see cref="LakeClientOptions.Csv"/> defaults.
    /// </summary>
    /// <param name="options">Optional per-operation overrides.</param>
    /// <returns>A fresh, immutable <see cref="CsvConfiguration"/>.</returns>
    private CsvConfiguration BuildCsvConfiguration(CsvOptions? options)
    {
        var culture = options?.CultureName is not null
            ? new CultureInfo(options.CultureName)
            : CultureInfo.InvariantCulture;

        var delimiter = options?.Delimiter ?? _options!.Csv.Delimiter;
        var hasHeader = options?.HasHeader ?? _options!.Csv.HasHeader;

        return new CsvConfiguration(culture)
        {
            Delimiter = delimiter,
            HasHeaderRecord = hasHeader
        };
    }
}
