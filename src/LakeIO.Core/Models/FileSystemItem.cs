using Azure;

namespace LakeIO;

/// <summary>
/// Represents a file system in Azure Data Lake Storage.
/// </summary>
/// <remarks>
/// This is LakeIO's own model, decoupling consumers from the Azure SDK's
/// <c>Azure.Storage.Files.DataLake.Models.FileSystemItem</c>.
/// </remarks>
public class FileSystemItem
{
    /// <summary>The file system name.</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>The ETag of the file system.</summary>
    public ETag? ETag { get; init; }

    /// <summary>The last modified timestamp.</summary>
    public DateTimeOffset? LastModified { get; init; }

    /// <summary>Custom metadata associated with the file system.</summary>
    public IDictionary<string, string>? Metadata { get; init; }

    /// <summary>
    /// Creates a <see cref="FileSystemItem"/> from an Azure SDK FileSystemItem.
    /// </summary>
    internal static FileSystemItem FromAzure(Azure.Storage.Files.DataLake.Models.FileSystemItem azureItem)
    {
        return new FileSystemItem
        {
            Name = azureItem.Name,
            ETag = azureItem.Properties?.ETag,
            LastModified = azureItem.Properties?.LastModified,
            Metadata = azureItem.Properties?.Metadata
        };
    }
}
