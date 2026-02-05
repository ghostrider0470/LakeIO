namespace LakeIO;

/// <summary>
/// Result of a storage write operation, containing path, ETag, and metadata.
/// </summary>
public class StorageResult
{
    /// <summary>The path of the written resource.</summary>
    public string Path { get; init; } = string.Empty;

    /// <summary>The ETag of the written resource, if available.</summary>
    public Azure.ETag? ETag { get; init; }

    /// <summary>The last modified timestamp, if available.</summary>
    public DateTimeOffset? LastModified { get; init; }

    /// <summary>The content length in bytes, if available.</summary>
    public long? ContentLength { get; init; }

    /// <summary>Custom metadata associated with the resource.</summary>
    public IDictionary<string, string>? Metadata { get; init; }
}
