using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

namespace LakeIO;

/// <summary>
/// Provides raw file operations (upload, download, delete, exists, properties, move)
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

        var response = await fileClient.UploadAsync(content, uploadOptions, cancellationToken).ConfigureAwait(false);

        return new Response<StorageResult>(
            new StorageResult
            {
                Path = fileClient.Path,
                ETag = response.Value.ETag,
                LastModified = response.Value.LastModified,
                ContentLength = content.CanSeek ? content.Length : null
            },
            response.GetRawResponse());
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

        var fileClient = _fileSystemClient!.GetFileClient(path);

        var downloadInfo = await fileClient.ReadContentAsync(cancellationToken).ConfigureAwait(false);

        return new Response<BinaryData>(downloadInfo.Value.Content, downloadInfo.GetRawResponse());
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

        var fileClient = _fileSystemClient!.GetFileClient(path);

        var downloadInfo = await fileClient.ReadStreamingAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        return new Response<Stream>(downloadInfo.Value.Content, downloadInfo.GetRawResponse());
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

        var fileClient = _fileSystemClient!.GetFileClient(path);

        await fileClient.DeleteAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
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

        var fileClient = _fileSystemClient!.GetFileClient(path);

        var response = await fileClient.ExistsAsync(cancellationToken).ConfigureAwait(false);

        return new Response<bool>(response.Value, response.GetRawResponse());
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

        var fileClient = _fileSystemClient!.GetFileClient(path);

        var response = await fileClient.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        return new Response<PathProperties>(response.Value, response.GetRawResponse());
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

        var fileClient = _fileSystemClient!.GetFileClient(sourcePath);

        DataLakeRequestConditions? destConditions = null;
        if (!overwrite)
        {
            destConditions = new DataLakeRequestConditions { IfNoneMatch = new ETag("*") };
        }

        var response = await fileClient.RenameAsync(
            destinationPath,
            destinationConditions: destConditions,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        return new Response<StorageResult>(
            new StorageResult
            {
                Path = response.Value.Path,
                ETag = null,
                LastModified = null,
                ContentLength = null
            },
            response.GetRawResponse());
    }
}
