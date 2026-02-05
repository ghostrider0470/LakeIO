namespace LakeIO;

/// <summary>
/// Options for listing paths in a file system.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Path"/> and <see cref="Recursive"/> are applied server-side by the Azure Data Lake REST API,
/// reducing the number of results returned over the wire.
/// </para>
/// <para>
/// <see cref="Filter"/> is applied client-side after results are received, enabling rich filtering
/// by extension, date range, size, etc. that the REST API does not support natively.
/// </para>
/// </remarks>
public class GetPathsOptions
{
    /// <summary>
    /// Server-side: filters results to paths within this directory prefix.
    /// When <c>null</c>, lists from the root of the file system.
    /// </summary>
    public string? Path { get; set; }

    /// <summary>
    /// Server-side: when <c>true</c>, lists paths recursively through all subdirectories.
    /// Defaults to <c>false</c> (flat listing of immediate children only).
    /// </summary>
    public bool Recursive { get; set; }

    /// <summary>
    /// Client-side: an optional <see cref="PathFilter"/> to apply after results are received.
    /// When <c>null</c>, no client-side filtering is performed.
    /// </summary>
    public PathFilter? Filter { get; set; }
}
