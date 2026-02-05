using Azure.Storage.Files.DataLake;

namespace LakeIO;

/// <summary>
/// Client for directory operations in Azure Data Lake.
/// </summary>
public class DirectoryClient
{
    private readonly DataLakeDirectoryClient? _azureClient;
    private readonly LakeClientOptions? _options;

    /// <summary>Protected constructor for mocking.</summary>
    protected DirectoryClient()
    {
    }

    /// <summary>Internal constructor used by <see cref="FileSystemClient"/>.</summary>
    internal DirectoryClient(DataLakeDirectoryClient azureClient, LakeClientOptions options)
    {
        _azureClient = azureClient ?? throw new ArgumentNullException(nameof(azureClient));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>Gets the directory path.</summary>
    public virtual string Path => _azureClient!.Path;

    /// <summary>Gets the directory name.</summary>
    public virtual string Name => _azureClient!.Name;

    /// <summary>Gets the directory URI.</summary>
    public virtual Uri Uri => _azureClient!.Uri;

    /// <summary>
    /// Gets a <see cref="FileClient"/> for a file within this directory.
    /// </summary>
    /// <param name="fileName">The file name.</param>
    /// <returns>A new <see cref="FileClient"/>.</returns>
    public virtual FileClient GetFileClient(string fileName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fileName);
        var azureClient = _azureClient!.GetFileClient(fileName);
        return new FileClient(azureClient, _options!);
    }

    /// <summary>
    /// Gets a <see cref="DirectoryClient"/> for a subdirectory.
    /// </summary>
    /// <param name="subdirectoryName">The subdirectory name.</param>
    /// <returns>A new <see cref="DirectoryClient"/>.</returns>
    public virtual DirectoryClient GetSubDirectoryClient(string subdirectoryName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subdirectoryName);
        var azureClient = _azureClient!.GetSubDirectoryClient(subdirectoryName);
        return new DirectoryClient(azureClient, _options!);
    }

    /// <summary>
    /// Checks if the directory exists.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the directory exists.</returns>
    public virtual async Task<Response<bool>> ExistsAsync(
        CancellationToken cancellationToken = default)
    {
        var response = await _azureClient!.ExistsAsync(cancellationToken);
        return new Response<bool>(response.Value, response.GetRawResponse());
    }

    /// <summary>
    /// Creates the directory if it doesn't exist.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public virtual async Task CreateIfNotExistsAsync(
        CancellationToken cancellationToken = default)
    {
        await _azureClient!.CreateIfNotExistsAsync(cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Deletes the directory.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public virtual async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        await _azureClient!.DeleteAsync(cancellationToken: cancellationToken);
    }
}
