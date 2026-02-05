namespace LakeIO;

/// <summary>
/// A fluent predicate builder for client-side filtering of <see cref="PathItem"/> results.
/// </summary>
/// <remarks>
/// <para>
/// All filter methods return <c>this</c> for chaining. Predicates are combined with AND semantics:
/// an item must match all predicates to pass the filter.
/// </para>
/// <para>
/// Example usage:
/// <code>
/// var filter = new PathFilter()
///     .WithExtension(".json")
///     .LargerThan(1024)
///     .FilesOnly();
/// </code>
/// </para>
/// </remarks>
public class PathFilter
{
    private readonly List<Func<PathItem, bool>> _predicates = new();

    /// <summary>
    /// Filters to items whose name ends with the specified file extension.
    /// </summary>
    /// <param name="extension">
    /// The file extension to match (e.g., <c>".json"</c> or <c>"json"</c>).
    /// A leading dot is added automatically if missing.
    /// </param>
    /// <returns>This <see cref="PathFilter"/> instance for chaining.</returns>
    public PathFilter WithExtension(string extension)
    {
        var normalized = extension.StartsWith('.') ? extension : "." + extension;
        _predicates.Add(item =>
            System.IO.Path.GetExtension(item.Name)
                .Equals(normalized, StringComparison.OrdinalIgnoreCase));
        return this;
    }

    /// <summary>
    /// Filters to items modified after the specified date.
    /// Items with no <see cref="PathItem.LastModified"/> value are excluded.
    /// </summary>
    /// <param name="date">The exclusive lower bound for the last modified date.</param>
    /// <returns>This <see cref="PathFilter"/> instance for chaining.</returns>
    public PathFilter ModifiedAfter(DateTimeOffset date)
    {
        _predicates.Add(item => item.LastModified.HasValue && item.LastModified.Value > date);
        return this;
    }

    /// <summary>
    /// Filters to items modified before the specified date.
    /// Items with no <see cref="PathItem.LastModified"/> value are excluded.
    /// </summary>
    /// <param name="date">The exclusive upper bound for the last modified date.</param>
    /// <returns>This <see cref="PathFilter"/> instance for chaining.</returns>
    public PathFilter ModifiedBefore(DateTimeOffset date)
    {
        _predicates.Add(item => item.LastModified.HasValue && item.LastModified.Value < date);
        return this;
    }

    /// <summary>
    /// Filters to items larger than the specified size in bytes.
    /// Items with no <see cref="PathItem.ContentLength"/> value are excluded.
    /// </summary>
    /// <param name="bytes">The exclusive lower bound for content length.</param>
    /// <returns>This <see cref="PathFilter"/> instance for chaining.</returns>
    public PathFilter LargerThan(long bytes)
    {
        _predicates.Add(item => item.ContentLength.HasValue && item.ContentLength.Value > bytes);
        return this;
    }

    /// <summary>
    /// Filters to items smaller than the specified size in bytes.
    /// Items with no <see cref="PathItem.ContentLength"/> value are excluded.
    /// </summary>
    /// <param name="bytes">The exclusive upper bound for content length.</param>
    /// <returns>This <see cref="PathFilter"/> instance for chaining.</returns>
    public PathFilter SmallerThan(long bytes)
    {
        _predicates.Add(item => item.ContentLength.HasValue && item.ContentLength.Value < bytes);
        return this;
    }

    /// <summary>
    /// Filters to files only (excludes directories).
    /// </summary>
    /// <returns>This <see cref="PathFilter"/> instance for chaining.</returns>
    public PathFilter FilesOnly()
    {
        _predicates.Add(item => !item.IsDirectory);
        return this;
    }

    /// <summary>
    /// Filters to directories only (excludes files).
    /// </summary>
    /// <returns>This <see cref="PathFilter"/> instance for chaining.</returns>
    public PathFilter DirectoriesOnly()
    {
        _predicates.Add(item => item.IsDirectory);
        return this;
    }

    /// <summary>
    /// Filters to items whose name contains the specified substring (case-insensitive).
    /// </summary>
    /// <param name="substring">The substring to search for in the item name.</param>
    /// <returns>This <see cref="PathFilter"/> instance for chaining.</returns>
    public PathFilter NameContains(string substring)
    {
        _predicates.Add(item => item.Name.Contains(substring, StringComparison.OrdinalIgnoreCase));
        return this;
    }

    /// <summary>
    /// Compiles all accumulated predicates into a single predicate function.
    /// </summary>
    /// <returns>
    /// A <see cref="Func{PathItem, Boolean}"/> that returns <c>true</c> when an item matches
    /// all predicates, or always returns <c>true</c> if no predicates were added.
    /// </returns>
    internal Func<PathItem, bool> Build()
    {
        if (_predicates.Count == 0)
        {
            return _ => true;
        }

        // Capture a snapshot of the predicates list to avoid mutation issues
        var predicates = _predicates.ToList();
        return item => predicates.All(p => p(item));
    }
}
