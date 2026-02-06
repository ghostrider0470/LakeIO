using System.Diagnostics;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

namespace LakeIO;

/// <summary>
/// Provides batch operations (delete, move, copy) for multiple files in Azure Data Lake Storage.
/// </summary>
/// <remarks>
/// <para>Access via <c>fileSystemClient.Batch()</c> extension method. Instances are
/// cached per <see cref="FileSystemClient"/> and are thread-safe.</para>
/// <para>All public methods are <see langword="virtual"/> for mocking. A
/// <see langword="protected"/> parameterless constructor is provided for test doubles.</para>
/// <para>Batch operations iterate sequentially and collect per-item errors without
/// throwing on partial failure. Use <see cref="BatchResult.IsFullySuccessful"/> and
/// <see cref="BatchResult.Items"/> to inspect individual outcomes.</para>
/// </remarks>
public class BatchOperations
{
    private readonly DataLakeFileSystemClient? _fileSystemClient;
    private readonly LakeClientOptions? _options;

    /// <summary>
    /// Protected parameterless constructor for mocking.
    /// </summary>
    protected BatchOperations()
    {
    }

    /// <summary>
    /// Internal constructor used by the <c>Batch()</c> extension method factory.
    /// </summary>
    /// <param name="fileSystemClient">The Azure Data Lake file system client.</param>
    /// <param name="options">The LakeIO client options.</param>
    internal BatchOperations(DataLakeFileSystemClient fileSystemClient, LakeClientOptions options)
    {
        _fileSystemClient = fileSystemClient ?? throw new ArgumentNullException(nameof(fileSystemClient));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// Deletes multiple files by path, collecting per-item results.
    /// </summary>
    /// <param name="paths">The file paths to delete.</param>
    /// <param name="progress">Optional progress reporter. Reports after each item.</param>
    /// <param name="cancellationToken">Cancellation token. Checked before each item.</param>
    /// <returns>A <see cref="BatchResult"/> with per-item success/failure details.</returns>
    public virtual async Task<BatchResult> DeleteAsync(
        IEnumerable<string> paths,
        IProgress<BatchProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(paths);

        using var activity = LakeIOActivitySource.Source.StartActivity("batch.delete");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.operation", "batch.delete");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var pathList = paths as IReadOnlyList<string> ?? paths.ToList();
            var count = pathList.Count;
            activity?.SetTag("lakeio.batch.count", count);

            var items = new List<BatchItemResult>(count);
            var succeeded = 0;
            var failed = 0;

            for (var i = 0; i < count; i++)
            {
                var path = pathList[i];
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    await _options!.RetryHelper.ExecuteAsync(async ct =>
                    {
                        await _fileSystemClient!.GetFileClient(path)
                            .DeleteAsync(cancellationToken: ct)
                            .ConfigureAwait(false);
                    }, cancellationToken).ConfigureAwait(false);

                    items.Add(new BatchItemResult { Path = path, Succeeded = true });
                    succeeded++;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    items.Add(new BatchItemResult
                    {
                        Path = path,
                        Succeeded = false,
                        Error = ex.Message,
                        Exception = ex
                    });
                    failed++;
                }

                progress?.Report(new BatchProgress
                {
                    Completed = i + 1,
                    Total = count,
                    CurrentPath = path
                });
            }

            var result = new BatchResult
            {
                TotalCount = count,
                SucceededCount = succeeded,
                FailedCount = failed,
                Items = items
            };

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "batch.delete"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "batch.delete"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "batch.delete"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "batch.delete"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Moves (renames) multiple files, collecting per-item results.
    /// </summary>
    /// <param name="items">The move items specifying source and destination paths.</param>
    /// <param name="progress">Optional progress reporter. Reports after each item.</param>
    /// <param name="cancellationToken">Cancellation token. Checked before each item.</param>
    /// <returns>A <see cref="BatchResult"/> with per-item success/failure details.</returns>
    public virtual async Task<BatchResult> MoveAsync(
        IEnumerable<BatchMoveItem> items,
        IProgress<BatchProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(items);

        using var activity = LakeIOActivitySource.Source.StartActivity("batch.move");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.operation", "batch.move");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var itemList = items as IReadOnlyList<BatchMoveItem> ?? items.ToList();
            var count = itemList.Count;
            activity?.SetTag("lakeio.batch.count", count);

            var results = new List<BatchItemResult>(count);
            var succeeded = 0;
            var failed = 0;

            for (var i = 0; i < count; i++)
            {
                var item = itemList[i];
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    var fileClient = _fileSystemClient!.GetFileClient(item.SourcePath);

                    DataLakeRequestConditions? destConditions = null;
                    if (!item.Overwrite)
                    {
                        destConditions = new DataLakeRequestConditions { IfNoneMatch = new ETag("*") };
                    }

                    await _options!.RetryHelper.ExecuteAsync(async ct =>
                    {
                        await fileClient.RenameAsync(
                            item.DestinationPath,
                            destinationConditions: destConditions,
                            cancellationToken: ct).ConfigureAwait(false);
                    }, cancellationToken).ConfigureAwait(false);

                    results.Add(new BatchItemResult { Path = item.SourcePath, Succeeded = true });
                    succeeded++;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    results.Add(new BatchItemResult
                    {
                        Path = item.SourcePath,
                        Succeeded = false,
                        Error = ex.Message,
                        Exception = ex
                    });
                    failed++;
                }

                progress?.Report(new BatchProgress
                {
                    Completed = i + 1,
                    Total = count,
                    CurrentPath = item.SourcePath
                });
            }

            var result = new BatchResult
            {
                TotalCount = count,
                SucceededCount = succeeded,
                FailedCount = failed,
                Items = results
            };

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "batch.move"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "batch.move"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "batch.move"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "batch.move"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Copies multiple files using download-then-upload, collecting per-item results.
    /// </summary>
    /// <param name="items">The copy items specifying source and destination paths.</param>
    /// <param name="progress">Optional progress reporter. Reports after each item.</param>
    /// <param name="cancellationToken">Cancellation token. Checked before each item.</param>
    /// <returns>A <see cref="BatchResult"/> with per-item success/failure details.</returns>
    public virtual async Task<BatchResult> CopyAsync(
        IEnumerable<BatchCopyItem> items,
        IProgress<BatchProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(items);

        using var activity = LakeIOActivitySource.Source.StartActivity("batch.copy");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.operation", "batch.copy");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var itemList = items as IReadOnlyList<BatchCopyItem> ?? items.ToList();
            var count = itemList.Count;
            activity?.SetTag("lakeio.batch.count", count);

            var results = new List<BatchItemResult>(count);
            var succeeded = 0;
            var failed = 0;

            for (var i = 0; i < count; i++)
            {
                var item = itemList[i];
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    await CopyFileAsync(item.SourcePath, item.DestinationPath, item.Overwrite, cancellationToken)
                        .ConfigureAwait(false);

                    results.Add(new BatchItemResult { Path = item.SourcePath, Succeeded = true });
                    succeeded++;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    results.Add(new BatchItemResult
                    {
                        Path = item.SourcePath,
                        Succeeded = false,
                        Error = ex.Message,
                        Exception = ex
                    });
                    failed++;
                }

                progress?.Report(new BatchProgress
                {
                    Completed = i + 1,
                    Total = count,
                    CurrentPath = item.SourcePath
                });
            }

            var result = new BatchResult
            {
                TotalCount = count,
                SucceededCount = succeeded,
                FailedCount = failed,
                Items = results
            };

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "batch.copy"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "batch.copy"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "batch.copy"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "batch.copy"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    private async Task CopyFileAsync(
        string sourcePath,
        string destinationPath,
        bool overwrite,
        CancellationToken cancellationToken)
    {
        var sourceFileClient = _fileSystemClient!.GetFileClient(sourcePath);

        var downloadResult = await sourceFileClient
            .ReadStreamingAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        await using var content = downloadResult.Value.Content;

        var destFileClient = _fileSystemClient.GetFileClient(destinationPath);

        var uploadOptions = new DataLakeFileUploadOptions();
        if (!overwrite)
        {
            uploadOptions.Conditions = new DataLakeRequestConditions { IfNoneMatch = new ETag("*") };
        }

        // Note: If source stream is not seekable, retry after partial upload will fail.
        // This is acceptable -- buffering into memory defeats the streaming purpose.
        await _options!.RetryHelper.ExecuteAsync(async ct =>
        {
            if (content.CanSeek) content.Position = 0;
            await destFileClient.UploadAsync(content, uploadOptions, ct).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }
}
