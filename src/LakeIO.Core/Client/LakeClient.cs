using System.Runtime.CompilerServices;
using Azure.Core;
using Azure.Storage.Files.DataLake;

namespace LakeIO;

/// <summary>
/// Top-level client for Azure Data Lake Storage Gen2 operations.
/// </summary>
/// <remarks>
/// <para>Create with a connection string or TokenCredential. Thread-safe and reusable --
/// register as singleton in DI. Does NOT implement IDisposable (Azure SDK clients share
/// a static HttpClient).</para>
/// <para>Use <see cref="GetFileSystemClient"/> to navigate to a specific file system.</para>
/// </remarks>
public class LakeClient
{
    private readonly DataLakeServiceClient _serviceClient;
    private readonly LakeClientOptions _options;

    /// <summary>
    /// Protected parameterless constructor for mocking.
    /// </summary>
    protected LakeClient()
    {
        _serviceClient = null!;
        _options = null!;
    }

    /// <summary>
    /// Creates a new <see cref="LakeClient"/> using a connection string with default options.
    /// </summary>
    /// <param name="connectionString">Azure Storage connection string.</param>
    public LakeClient(string connectionString)
        : this(connectionString, new LakeClientOptions())
    {
    }

    /// <summary>
    /// Creates a new <see cref="LakeClient"/> using a connection string.
    /// </summary>
    /// <param name="connectionString">Azure Storage connection string.</param>
    /// <param name="options">Client configuration options.</param>
    public LakeClient(string connectionString, LakeClientOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentNullException.ThrowIfNull(options);

        _options = options;
        _serviceClient = new DataLakeServiceClient(connectionString, options.ToDataLakeClientOptions());
    }

    /// <summary>
    /// Creates a new <see cref="LakeClient"/> using a TokenCredential with default options.
    /// </summary>
    /// <param name="serviceUri">The Data Lake service URI (e.g., https://accountname.dfs.core.windows.net).</param>
    /// <param name="credential">The token credential for authentication.</param>
    public LakeClient(Uri serviceUri, TokenCredential credential)
        : this(serviceUri, credential, new LakeClientOptions())
    {
    }

    /// <summary>
    /// Creates a new <see cref="LakeClient"/> using a TokenCredential.
    /// </summary>
    /// <param name="serviceUri">The Data Lake service URI (e.g., https://accountname.dfs.core.windows.net).</param>
    /// <param name="credential">The token credential for authentication.</param>
    /// <param name="options">Client configuration options.</param>
    public LakeClient(Uri serviceUri, TokenCredential credential, LakeClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(serviceUri);
        ArgumentNullException.ThrowIfNull(credential);
        ArgumentNullException.ThrowIfNull(options);

        _options = options;
        _serviceClient = new DataLakeServiceClient(serviceUri, credential, options.ToDataLakeClientOptions());
    }

    /// <summary>Gets the Data Lake service URI.</summary>
    public virtual Uri Uri => _serviceClient.Uri;

    /// <summary>
    /// Gets a <see cref="FileSystemClient"/> for the specified file system.
    /// </summary>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>A new <see cref="FileSystemClient"/> for the file system.</returns>
    public virtual FileSystemClient GetFileSystemClient(string fileSystemName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fileSystemName);
        var azureClient = _serviceClient.GetFileSystemClient(fileSystemName);
        return new FileSystemClient(azureClient, _options);
    }

    /// <summary>
    /// Enumerates all file systems in the storage account.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of file system items.</returns>
    public virtual async IAsyncEnumerable<FileSystemItem> GetFileSystemsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in _serviceClient.GetFileSystemsAsync(cancellationToken: cancellationToken))
        {
            yield return FileSystemItem.FromAzure(item);
        }
    }
}
