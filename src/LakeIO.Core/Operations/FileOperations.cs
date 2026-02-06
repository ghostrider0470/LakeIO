using System.Diagnostics;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

namespace LakeIO;

/// <summary>
/// Provides raw file operations (upload, download, delete, exists, properties, move, copy)
/// for Azure Data Lake Storage.
/// </summary>
/// <remarks>
/// <para>Access via <c>fileSystemClient.Files()</c> extension method. Instances are
/// cached per <see cref="FileSystemClient"/> and are thread-safe.</para>
/// <para>All public methods are <see langword="virtual"/> for mocking. A
/// <see langword="protected"/> parameterless constructor is provided for test doubles.</para>
/// </remarks>
public class FileOperations
{
    private readonly DataLakeFileSystemClient? _fileSystemClient;
    private readonly LakeClientOptions? _options;

    /// <summary>
    /// Protected parameterless constructor for mocking.
    /// </summary>
    protected FileOperations()
    {
    }

    /// <summary>
    /// Internal constructor used by the <c>Files()</c> extension method factory.
    /// </summary>
    /// <param name="fileSystemClient">The Azure Data Lake file system client.</param>
    /// <param name="options">The LakeIO client options.</param>
    internal FileOperations(DataLakeFileSystemClient fileSystemClient, LakeClientOptions options)
    {
        _fileSystemClient = fileSystemClient ?? throw new ArgumentNullException(nameof(fileSystemClient));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// Uploads a stream to the specified file path.
    /// </summary>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="content">The stream content to upload.</param>
    /// <param name="contentType">Optional content type (e.g. "application/octet-stream"). Sets the HTTP Content-Type header.</param>
    /// <param name="overwrite">Whether to overwrite an existing file. Default is <see langword="true"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the <see cref="StorageResult"/> with path, ETag, and metadata.</returns>
    public virtual async Task<Response<StorageResult>> UploadAsync(
        string path,
        Stream content,
        string? contentType = null,
        bool overwrite = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(content);

        using var activity = LakeIOActivitySource.Source.StartActivity("file.upload");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "file.upload");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var fileClient = _fileSystemClient!.GetFileClient(path);

            var uploadOptions = new DataLakeFileUploadOptions();

            if (contentType is not null)
            {
                uploadOptions.HttpHeaders = new PathHttpHeaders { ContentType = contentType };
            }

            if (!overwrite)
            {
                uploadOptions.Conditions = new DataLakeRequestConditions { IfNoneMatch = new ETag("*") };
            }

            var response = await _options!.RetryHelper.ExecuteAsync(async ct =>
            {
                if (content.CanSeek) content.Position = 0;
                return await fileClient.UploadAsync(content, uploadOptions, ct).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);

            var result = new Response<StorageResult>(
                new StorageResult
                {
                    Path = fileClient.Path,
                    ETag = response.Value.ETag,
                    LastModified = response.Value.LastModified,
                    ContentLength = content.CanSeek ? content.Length : null
                },
                response.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.upload"));
            if (content.CanSeek)
            {
                LakeIOMetrics.BytesTransferred.Add(content.Length,
                    new KeyValuePair<string, object?>("direction", "write"));
                activity?.SetTag("lakeio.bytes", content.Length);
            }
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.upload"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.upload"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.upload"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Downloads a file as <see cref="BinaryData"/> (suitable for small files that fit in memory).
    /// </summary>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the file content as <see cref="BinaryData"/>.</returns>
    public virtual async Task<Response<BinaryData>> DownloadAsync(
        string path,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        using var activity = LakeIOActivitySource.Source.StartActivity("file.download");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "file.download");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var fileClient = _fileSystemClient!.GetFileClient(path);

            var downloadInfo = await fileClient.ReadContentAsync(cancellationToken).ConfigureAwait(false);

            var result = new Response<BinaryData>(downloadInfo.Value.Content, downloadInfo.GetRawResponse());

            var bytesRead = downloadInfo.Value.Content.ToMemory().Length;
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.download"));
            LakeIOMetrics.BytesTransferred.Add(bytesRead,
                new KeyValuePair<string, object?>("direction", "read"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.download"));

            activity?.SetTag("lakeio.bytes", bytesRead);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.download"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.download"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Downloads a file as a <see cref="Stream"/> (suitable for large files to avoid memory pressure).
    /// </summary>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the file content as a <see cref="Stream"/>.</returns>
    public virtual async Task<Response<Stream>> DownloadStreamAsync(
        string path,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        using var activity = LakeIOActivitySource.Source.StartActivity("file.download_stream");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "file.download_stream");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var fileClient = _fileSystemClient!.GetFileClient(path);

            var downloadInfo = await fileClient.ReadStreamingAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            var result = new Response<Stream>(downloadInfo.Value.Content, downloadInfo.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.download_stream"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.download_stream"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.download_stream"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.download_stream"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Deletes a file at the specified path.
    /// </summary>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public virtual async Task DeleteAsync(
        string path,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        using var activity = LakeIOActivitySource.Source.StartActivity("file.delete");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "file.delete");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var fileClient = _fileSystemClient!.GetFileClient(path);

            await _options!.RetryHelper.ExecuteAsync(async ct =>
            {
                await fileClient.DeleteAsync(cancellationToken: ct).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.delete"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.delete"));

            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.delete"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.delete"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Checks whether a file exists at the specified path.
    /// </summary>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing <see langword="true"/> if the file exists.</returns>
    public virtual async Task<Response<bool>> ExistsAsync(
        string path,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        using var activity = LakeIOActivitySource.Source.StartActivity("file.exists");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "file.exists");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var fileClient = _fileSystemClient!.GetFileClient(path);

            var response = await fileClient.ExistsAsync(cancellationToken).ConfigureAwait(false);

            var result = new Response<bool>(response.Value, response.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.exists"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.exists"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.exists"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.exists"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Gets properties (size, modified date, metadata) for a file at the specified path.
    /// </summary>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the <see cref="PathProperties"/>.</returns>
    public virtual async Task<Response<PathProperties>> GetPropertiesAsync(
        string path,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        using var activity = LakeIOActivitySource.Source.StartActivity("file.get_properties");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "file.get_properties");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var fileClient = _fileSystemClient!.GetFileClient(path);

            var response = await fileClient.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            var result = new Response<PathProperties>(response.Value, response.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.get_properties"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.get_properties"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.get_properties"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.get_properties"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Moves (renames) a file from one path to another using server-side rename.
    /// </summary>
    /// <param name="sourcePath">The current file path.</param>
    /// <param name="destinationPath">The new file path.</param>
    /// <param name="overwrite">Whether to overwrite an existing file at the destination. Default is <see langword="false"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the <see cref="StorageResult"/> with the new path.</returns>
    public virtual async Task<Response<StorageResult>> MoveAsync(
        string sourcePath,
        string destinationPath,
        bool overwrite = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);

        using var activity = LakeIOActivitySource.Source.StartActivity("file.move");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", sourcePath);
        activity?.SetTag("lakeio.destination_path", destinationPath);
        activity?.SetTag("lakeio.operation", "file.move");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var fileClient = _fileSystemClient!.GetFileClient(sourcePath);

            DataLakeRequestConditions? destConditions = null;
            if (!overwrite)
            {
                destConditions = new DataLakeRequestConditions { IfNoneMatch = new ETag("*") };
            }

            var response = await _options!.RetryHelper.ExecuteAsync(async ct =>
            {
                return await fileClient.RenameAsync(
                    destinationPath,
                    destinationConditions: destConditions,
                    cancellationToken: ct).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);

            var result = new Response<StorageResult>(
                new StorageResult
                {
                    Path = response.Value.Path,
                    ETag = null,
                    LastModified = null,
                    ContentLength = null
                },
                response.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.move"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.move"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.move"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.move"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Downloads a byte range of a file as <see cref="BinaryData"/>, without downloading the entire file.
    /// </summary>
    /// <param name="path">The file path within the file system.</param>
    /// <param name="offset">The byte offset to start reading from.</param>
    /// <param name="length">The number of bytes to read.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the requested byte range as <see cref="BinaryData"/>.</returns>
    /// <remarks>
    /// This is a read operation and is NOT wrapped in application-level retry.
    /// The Azure SDK transport layer handles transient retries for reads.
    /// </remarks>
    public virtual async Task<Response<BinaryData>> DownloadRangeAsync(
        string path,
        long offset,
        long length,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        using var activity = LakeIOActivitySource.Source.StartActivity("file.download_range");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", path);
        activity?.SetTag("lakeio.operation", "file.download_range");
        activity?.SetTag("lakeio.offset", offset);
        activity?.SetTag("lakeio.length", length);

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var fileClient = _fileSystemClient!.GetFileClient(path);

            var readOptions = new DataLakeFileReadOptions { Range = new HttpRange(offset, length) };

            var downloadInfo = await fileClient.ReadContentAsync(readOptions, cancellationToken).ConfigureAwait(false);

            var result = new Response<BinaryData>(downloadInfo.Value.Content, downloadInfo.GetRawResponse());

            var bytesRead = downloadInfo.Value.Content.ToMemory().Length;
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.download_range"));
            LakeIOMetrics.BytesTransferred.Add(bytesRead,
                new KeyValuePair<string, object?>("direction", "read"));
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.download_range"));

            activity?.SetTag("lakeio.bytes", bytesRead);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.download_range"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.download_range"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Copies a file within the same container using a download-then-upload pattern.
    /// </summary>
    /// <param name="sourcePath">The source file path.</param>
    /// <param name="destinationPath">The destination file path within the same container.</param>
    /// <param name="overwrite">Whether to overwrite an existing file at the destination. Default is <see langword="false"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the <see cref="StorageResult"/> with the destination path.</returns>
    /// <remarks>
    /// <para>The download (read) step is NOT retry-wrapped. The upload (mutation) step IS retry-wrapped.</para>
    /// <para>This operation is not atomic: if the upload fails after a partial write, the destination
    /// file may be left in an inconsistent state.</para>
    /// </remarks>
    public virtual async Task<Response<StorageResult>> CopyAsync(
        string sourcePath,
        string destinationPath,
        bool overwrite = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);

        using var activity = LakeIOActivitySource.Source.StartActivity("file.copy");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", sourcePath);
        activity?.SetTag("lakeio.destination_path", destinationPath);
        activity?.SetTag("lakeio.operation", "file.copy");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
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

            var uploadResponse = await _options!.RetryHelper.ExecuteAsync(async ct =>
            {
                if (content.CanSeek) content.Position = 0;
                return await destFileClient.UploadAsync(content, uploadOptions, ct).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);

            var result = new Response<StorageResult>(
                new StorageResult
                {
                    Path = destFileClient.Path,
                    ETag = uploadResponse.Value.ETag,
                    LastModified = uploadResponse.Value.LastModified,
                    ContentLength = content.CanSeek ? content.Length : null
                },
                uploadResponse.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.copy"));
            if (content.CanSeek)
            {
                LakeIOMetrics.BytesTransferred.Add(content.Length,
                    new KeyValuePair<string, object?>("direction", "write"));
                activity?.SetTag("lakeio.bytes", content.Length);
            }
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.copy"));

            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.copy"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.copy"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }

    /// <summary>
    /// Copies a file to a different file system (cross-container copy) using a download-then-upload pattern.
    /// </summary>
    /// <param name="sourcePath">The source file path within the current file system.</param>
    /// <param name="targetFileSystem">The target <see cref="FileSystemClient"/> to copy to.</param>
    /// <param name="targetPath">The destination file path within the target file system.</param>
    /// <param name="deleteSource">Whether to delete the source file after a successful copy (move semantics). Default is <see langword="false"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Response{T}"/> containing the <see cref="StorageResult"/> with the target path.</returns>
    /// <remarks>
    /// <para>The download (read) step is NOT retry-wrapped. The upload and optional delete (mutations) are retry-wrapped.</para>
    /// <para>This operation is not atomic. If <paramref name="deleteSource"/> is <see langword="true"/> and the
    /// delete fails after a successful copy, the file will exist in both locations. The response is returned
    /// before the delete attempt, so callers know the copy succeeded.</para>
    /// <para>The target file is always overwritten (cross-container copies are intentional operations).</para>
    /// </remarks>
    public virtual async Task<Response<StorageResult>> CopyToAsync(
        string sourcePath,
        FileSystemClient targetFileSystem,
        string targetPath,
        bool deleteSource = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        ArgumentNullException.ThrowIfNull(targetFileSystem);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetPath);

        using var activity = LakeIOActivitySource.Source.StartActivity("file.copy_to");
        activity?.SetTag("lakeio.filesystem", _fileSystemClient!.Name);
        activity?.SetTag("lakeio.path", sourcePath);
        activity?.SetTag("lakeio.target_filesystem", targetFileSystem.Name);
        activity?.SetTag("lakeio.target_path", targetPath);
        activity?.SetTag("lakeio.operation", "file.copy_to");

        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var sourceFileClient = _fileSystemClient!.GetFileClient(sourcePath);

            var downloadResult = await sourceFileClient
                .ReadStreamingAsync(cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            await using var content = downloadResult.Value.Content;

            var destFileClient = targetFileSystem.AzureClient.GetFileClient(targetPath);

            var uploadResponse = await _options!.RetryHelper.ExecuteAsync(async ct =>
            {
                if (content.CanSeek) content.Position = 0;
                return await destFileClient.UploadAsync(content, new DataLakeFileUploadOptions(), ct).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);

            var result = new Response<StorageResult>(
                new StorageResult
                {
                    Path = destFileClient.Path,
                    ETag = uploadResponse.Value.ETag,
                    LastModified = uploadResponse.Value.LastModified,
                    ContentLength = content.CanSeek ? content.Length : null
                },
                uploadResponse.GetRawResponse());

            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.copy_to"));
            if (content.CanSeek)
            {
                LakeIOMetrics.BytesTransferred.Add(content.Length,
                    new KeyValuePair<string, object?>("direction", "write"));
                activity?.SetTag("lakeio.bytes", content.Length);
            }
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.copy_to"));

            activity?.SetStatus(ActivityStatusCode.Ok);

            if (deleteSource)
            {
                await _options.RetryHelper.ExecuteAsync(async ct =>
                {
                    await sourceFileClient.DeleteAsync(cancellationToken: ct).ConfigureAwait(false);
                }, cancellationToken).ConfigureAwait(false);
            }

            return result;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("lakeio.error", true);
            LakeIOMetrics.OperationsTotal.Add(1,
                new KeyValuePair<string, object?>("operation_type", "file.copy_to"),
                new KeyValuePair<string, object?>("error", "true"));
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
            LakeIOMetrics.OperationDuration.Record(elapsed,
                new KeyValuePair<string, object?>("operation_type", "file.copy_to"),
                new KeyValuePair<string, object?>("error", "true"));
            throw;
        }
    }
}
