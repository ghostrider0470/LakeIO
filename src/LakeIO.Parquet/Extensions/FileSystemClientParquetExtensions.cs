using LakeIO.Parquet;

namespace LakeIO;

/// <summary>
/// Extension methods for <see cref="FileSystemClient"/> providing Parquet operations.
/// </summary>
/// <remarks>
/// This class is in the <c>LakeIO</c> namespace so that <c>.Parquet()</c> is discoverable
/// when users have <c>using LakeIO;</c> without needing <c>using LakeIO.Parquet;</c>.
/// </remarks>
public static class FileSystemClientParquetExtensions
{
    /// <summary>
    /// Gets Parquet storage operations for this file system.
    /// </summary>
    /// <param name="client">The file system client.</param>
    /// <returns>Parquet operations instance (cached, thread-safe).</returns>
    public static ParquetOperations Parquet(this FileSystemClient client)
    {
        return client.GetOrCreateOperations<ParquetOperations>(
            (azure, opts) => new ParquetOperations(azure, opts));
    }
}
