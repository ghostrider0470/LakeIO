using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

namespace LakeIO;

/// <summary>
/// Client for file operations in Azure Data Lake.
/// </summary>
public class FileClient
{
    private readonly DataLakeFileClient? _azureClient;
    private readonly LakeClientOptions? _options;

    /// <summary>Protected constructor for mocking.</summary>
    protected FileClient()
    {
    }

    /// <summary>Internal constructor used by <see cref="FileSystemClient"/> and <see cref="DirectoryClient"/>.</summary>
    internal FileClient(DataLakeFileClient azureClient, LakeClientOptions options)
    {
        _azureClient = azureClient ?? throw new ArgumentNullException(nameof(azureClient));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>Gets the file path.</summary>
    public virtual string Path => _azureClient!.Path;

    /// <summary>Gets the file name.</summary>
    public virtual string Name => _azureClient!.Name;

    /// <summary>Gets the file URI.</summary>
    public virtual Uri Uri => _azureClient!.Uri;

    /// <summary>
    /// Checks if the file exists.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the file exists.</returns>
    public virtual async Task<Response<bool>> ExistsAsync(
        CancellationToken cancellationToken = default)
    {
        var response = await _azureClient!.ExistsAsync(cancellationToken);
        return new Response<bool>(response.Value, response.GetRawResponse());
    }

    /// <summary>
    /// Gets file properties including size, content type, and metadata.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A response wrapping the Azure PathProperties.</returns>
    /// <remarks>
    /// Returns Azure SDK <see cref="PathProperties"/> directly wrapped in LakeIO's
    /// <see cref="Response{T}"/>. A dedicated LakeIO properties model may be added later
    /// to decouple from Azure SDK types.
    /// </remarks>
    public virtual async Task<Response<PathProperties>> GetPropertiesAsync(
        CancellationToken cancellationToken = default)
    {
        var response = await _azureClient!.GetPropertiesAsync(
            cancellationToken: cancellationToken);
        return new Response<PathProperties>(response.Value, response.GetRawResponse());
    }

    /// <summary>
    /// Deletes the file.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public virtual async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        await _azureClient!.DeleteAsync(cancellationToken: cancellationToken);
    }
}
