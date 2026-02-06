using System.Diagnostics;
using System.Runtime.CompilerServices;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

namespace LakeIO;

/// <summary>
/// Provides directory listing, searching, counting, and property retrieval operations
/// for Azure Data Lake Storage.
/// </summary>
/// <remarks>
/// <para>Access via <c>fileSystemClient.Directory()</c> extension method. Instances are
/// cached per <see cref="FileSystemClient"/> and are thread-safe.</para>
/// <para>All public methods are <see langword="virtual"/> for mocking. A
/// <see langword="protected"/> parameterless constructor is provided for test doubles.</para>
/// </remarks>
public class DirectoryOperations
{
    private readonly DataLakeFileSystemClient? _fileSystemClient;
    private readonly LakeClientOptions? _options;

    /// <summary>
    /// Protected parameterless constructor for mocking.
    /// </summary>
    protected DirectoryOperations()
    {
    }

    /// <summary>
    /// Internal constructor used by the <c>Directory()</c> extension method factory.
    /// </summary>
    /// <param name="fileSystemClient">The Azure Data Lake file system client.</param>
    /// <param name="options">The LakeIO client options.</param>
    internal DirectoryOperations(DataLakeFileSystemClient fileSystemClient, LakeClientOptions options)
    {
        _fileSystemClient = fileSystemClient ?? throw new ArgumentNullException(nameof(fileSystemClient));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// Lists paths in the file system as an <see cref="IAsyncEnumerable{T}"/> without client-side buffering.
    /// </summary>
    /// <param name="options">
    /// Optional listing options. <see cref="GetPathsOptions.Path"/> and <see cref="GetPathsOptions.Recursive"/>
    /// are applied server-side by the Azure Data Lake REST API. <see cref="GetPathsOptions.Filter"/> is applied
    /// client-side during enumeration, enabling rich filtering by extension, date, size, etc.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// An <see cref="IAsyncEnumerable{PathItem}"/> that streams results page-by-page from the server.
    /// Items are yielded one at a time without materializing the entire result set.
    /// </returns>
    public virtual async IAsyncEnumerable<PathItem> GetPathsAsync(
        GetPathsOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var activity = LakeIOActivitySource.Source.StartActivity("directory.list");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", options?.Path);
        activity?.SetTag("lakeio.operation", "directory.list");

        var startTimestamp = Stopwatch.GetTimestamp();
        var success = false;
        try
        {
            var path = options?.Path;
            var recursive = options?.Recursive ?? false;
            var predicate = options?.Filter?.Build();

            await foreach (var azureItem in _fileSystemClient!.GetPathsAsync(path, recursive, false, cancellationToken).ConfigureAwait(false))
            {
                var item = PathItem.FromAzure(azureItem);

                if (predicate is null || predicate(item))
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
                    new KeyValuePair<string, object?>("operation_type", "directory.list"));
                LakeIOMetrics.OperationDuration.Record(elapsed,
                    new KeyValuePair<string, object?>("operation_type", "directory.list"));
                activity?.SetStatus(ActivityStatusCode.Ok);
            }
            else
            {
                activity?.SetStatus(ActivityStatusCode.Error, "Operation failed");
                activity?.SetTag("lakeio.error", true);
                LakeIOMetrics.OperationsTotal.Add(1,
                    new KeyValuePair<string, object?>("operation_type", "directory.list"),
                    new KeyValuePair<string, object?>("error", "true"));
                LakeIOMetrics.OperationDuration.Record(elapsed,
                    new KeyValuePair<string, object?>("operation_type", "directory.list"),
                    new KeyValuePair<string, object?>("error", "true"));
            }
        }
    }

    /// <summary>
    /// Counts the number of paths matching the specified options without materializing all items into a collection.
    /// </summary>
    /// <remarks>
    /// <para>This method iterates through server pages using <see cref="GetPathsAsync"/> but only maintains
    /// a running counter. No <see cref="System.Collections.Generic.List{T}"/> or array is allocated for results.</para>
    /// <para>Note: This still makes HTTP calls to enumerate all matching paths. For large result sets,
    /// consider whether the count is truly needed or if streaming via <see cref="GetPathsAsync"/> is more appropriate.</para>
    /// </remarks>
    /// <param name="options">
    /// Optional listing options. Server-side and client-side filters are applied identically to <see cref="GetPathsAsync"/>.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of matching paths.</returns>
    public virtual async Task<long> CountAsync(
        GetPathsOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        using var activity = LakeIOActivitySource.Source.StartActivity("directory.count");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", options?.Path);
        activity?.SetTag("lakeio.operation", "directory.count");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            long count = 0;

            await foreach (var _ in GetPathsAsync(options, cancellationToken).ConfigureAwait(false))
            {
                count++;
            }

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "directory.count"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "directory.count"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return count;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "directory.count"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "directory.count"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Gets detailed properties for a specific file or directory path.
    /// </summary>
    /// <param name="path">The path within the file system to inspect.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A <see cref="Response{T}"/> containing the <see cref="FileProperties"/> with content length,
    /// content type, timestamps, metadata, and other path attributes.
    /// </returns>
    public virtual async Task<Response<FileProperties>> GetPropertiesAsync(
        string path,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        using var activity = LakeIOActivitySource.Source.StartActivity("directory.get_properties");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "directory.get_properties");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var fileClient = _fileSystemClient!.GetFileClient(path);

            var response = await fileClient.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            var properties = FileProperties.FromAzure(response.Value);

            var result = new Response<FileProperties>(properties, response.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "directory.get_properties"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "directory.get_properties"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "directory.get_properties"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "directory.get_properties"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }
}
