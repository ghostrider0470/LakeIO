using Azure;

namespace LakeIO;

/// <summary>
/// Detailed properties for a file or directory in Azure Data Lake Storage.
/// </summary>
/// <remarks>
/// This is LakeIO's own model, decoupling consumers from the Azure SDK's
/// <c>Azure.Storage.Files.DataLake.Models.PathProperties</c>.
/// Returned by operations that fetch full metadata (e.g., GetPropertiesAsync).
/// </remarks>
public class FileProperties
{
    /// <summary>The content length in bytes.</summary>
    public long ContentLength { get; init; }

    /// <summary>The content type (MIME type), if available.</summary>
    public string? ContentType { get; init; }

    /// <summary>The last modified timestamp.</summary>
    public DateTimeOffset LastModified { get; init; }

    /// <summary>The creation timestamp.</summary>
    public DateTimeOffset CreatedOn { get; init; }

    /// <summary>The ETag of the resource.</summary>
    public ETag ETag { get; init; }

    /// <summary>Custom metadata associated with the resource, if any.</summary>
    public IDictionary<string, string>? Metadata { get; init; }

    /// <summary>Whether this resource is a directory.</summary>
    public bool IsDirectory { get; init; }

    /// <summary>The owner of the resource, if available.</summary>
    public string? Owner { get; init; }

    /// <summary>The owning group of the resource, if available.</summary>
    public string? Group { get; init; }

    /// <summary>The POSIX permissions string of the resource, if available.</summary>
    public string? Permissions { get; init; }

    /// <summary>The expiration timestamp of the resource, if set.</summary>
    public DateTimeOffset? ExpiresOn { get; init; }

    /// <summary>The content encoding, if set (e.g., gzip).</summary>
    public string? ContentEncoding { get; init; }

    /// <summary>The cache control header value, if set.</summary>
    public string? CacheControl { get; init; }

    /// <summary>
    /// Creates a <see cref="FileProperties"/> from Azure SDK PathProperties.
    /// </summary>
    /// <param name="props">The Azure SDK path properties to convert.</param>
    /// <returns>A new <see cref="FileProperties"/> mapped from the Azure SDK model.</returns>
    internal static FileProperties FromAzure(Azure.Storage.Files.DataLake.Models.PathProperties props)
    {
        return new FileProperties
        {
            ContentLength = props.ContentLength,
            ContentType = props.ContentType,
            LastModified = props.LastModified,
            CreatedOn = props.CreatedOn,
            ETag = props.ETag,
            Metadata = props.Metadata,
            IsDirectory = props.IsDirectory,
            Owner = props.Owner,
            Group = props.Group,
            Permissions = props.Permissions,
            ExpiresOn = props.ExpiresOn,
            ContentEncoding = props.ContentEncoding,
            CacheControl = props.CacheControl
        };
    }
}
