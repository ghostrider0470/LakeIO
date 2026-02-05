using Azure;

namespace LakeIO;

/// <summary>
/// Represents a path (file or directory) in Azure Data Lake Storage.
/// </summary>
/// <remarks>
/// This is LakeIO's own model, decoupling consumers from the Azure SDK's
/// <c>Azure.Storage.Files.DataLake.Models.PathItem</c>.
/// </remarks>
public class PathItem
{
    /// <summary>The name (path) of the item.</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>Whether this item is a directory. Defaults to <c>false</c> (file) if unknown.</summary>
    public bool IsDirectory { get; init; }

    /// <summary>The last modified timestamp, if available.</summary>
    public DateTimeOffset? LastModified { get; init; }

    /// <summary>The creation timestamp, if available.</summary>
    public DateTimeOffset? CreatedOn { get; init; }

    /// <summary>The content length in bytes, if available. Null for directories.</summary>
    public long? ContentLength { get; init; }

    /// <summary>The ETag of the item, if available.</summary>
    public ETag? ETag { get; init; }

    /// <summary>The owner of the item, if available.</summary>
    public string? Owner { get; init; }

    /// <summary>The owning group of the item, if available.</summary>
    public string? Group { get; init; }

    /// <summary>The POSIX permissions string of the item, if available.</summary>
    public string? Permissions { get; init; }

    /// <summary>The expiration timestamp of the item, if set.</summary>
    public DateTimeOffset? ExpiresOn { get; init; }

    /// <summary>
    /// Creates a <see cref="PathItem"/> from an Azure SDK PathItem.
    /// </summary>
    /// <param name="azureItem">The Azure SDK path item to convert.</param>
    /// <returns>A new <see cref="PathItem"/> mapped from the Azure SDK model.</returns>
    internal static PathItem FromAzure(Azure.Storage.Files.DataLake.Models.PathItem azureItem)
    {
        return new PathItem
        {
            Name = azureItem.Name,
            IsDirectory = azureItem.IsDirectory ?? false,
            LastModified = azureItem.LastModified,
            CreatedOn = azureItem.CreatedOn,
            ContentLength = azureItem.ContentLength,
            ETag = azureItem.ETag,
            Owner = azureItem.Owner,
            Group = azureItem.Group,
            Permissions = azureItem.Permissions,
            ExpiresOn = azureItem.ExpiresOn
        };
    }
}
