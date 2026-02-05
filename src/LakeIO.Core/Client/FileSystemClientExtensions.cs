namespace LakeIO;

/// <summary>
/// Extension methods for <see cref="FileSystemClient"/> providing format-specific operations.
/// </summary>
/// <remarks>
/// Operations use extension methods (not properties) because:
/// 1. C# stable versions do not support extension properties.
/// 2. LakeIO.Parquet adds <c>.Parquet()</c> from a separate assembly using the same pattern.
/// 3. Thread-safe lazy initialization via <see cref="FileSystemClient.GetOrCreateOperations{T}"/>.
/// </remarks>
public static class FileSystemClientExtensions
{
    /// <summary>
    /// Gets JSON storage operations for this file system.
    /// </summary>
    /// <param name="client">The file system client.</param>
    /// <returns>JSON operations instance (cached, thread-safe).</returns>
    public static JsonOperations Json(this FileSystemClient client)
    {
        return client.GetOrCreateOperations<JsonOperations>(
            (azure, opts) => new JsonOperations(azure, opts));
    }

    /// <summary>
    /// Gets CSV storage operations for this file system.
    /// </summary>
    /// <param name="client">The file system client.</param>
    /// <returns>CSV operations instance (cached, thread-safe).</returns>
    public static CsvOperations Csv(this FileSystemClient client)
    {
        return client.GetOrCreateOperations<CsvOperations>(
            (azure, opts) => new CsvOperations(azure, opts));
    }

    /// <summary>
    /// Gets raw file operations for this file system.
    /// </summary>
    /// <param name="client">The file system client.</param>
    /// <returns>File operations instance (cached, thread-safe).</returns>
    public static FileOperations Files(this FileSystemClient client)
    {
        return client.GetOrCreateOperations<FileOperations>(
            (azure, opts) => new FileOperations(azure, opts));
    }

    /// <summary>
    /// Gets batch operations for this file system.
    /// </summary>
    /// <param name="client">The file system client.</param>
    /// <returns>Batch operations instance (cached, thread-safe).</returns>
    public static BatchOperations Batch(this FileSystemClient client)
    {
        return client.GetOrCreateOperations<BatchOperations>(
            (azure, opts) => new BatchOperations(azure, opts));
    }

    /// <summary>
    /// Gets directory listing and search operations for this file system.
    /// </summary>
    /// <param name="client">The file system client.</param>
    /// <returns>Directory operations instance (cached, thread-safe).</returns>
    public static DirectoryOperations Directory(this FileSystemClient client)
    {
        return client.GetOrCreateOperations<DirectoryOperations>(
            (azure, opts) => new DirectoryOperations(azure, opts));
    }
}
