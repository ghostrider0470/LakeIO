using Azure.Storage.Files.DataLake;

namespace LakeIO;

/// <summary>
/// Client for file system operations. Fully implemented in Plan 03.
/// </summary>
public class FileSystemClient
{
    /// <summary>Protected constructor for mocking.</summary>
    protected FileSystemClient()
    {
    }

    /// <summary>Internal constructor used by LakeClient.</summary>
    internal FileSystemClient(DataLakeFileSystemClient azureClient, LakeClientOptions options)
    {
    }
}
