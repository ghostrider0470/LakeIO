namespace LakeIO.Services;

/// <summary>
/// Interface for managing file size thresholds and automatic rotation of large files.
/// </summary>
public interface IFileSizeManager
{
    /// <summary>
    /// Checks if a file exceeds the warning threshold.
    /// </summary>
    /// <param name="sizeInBytes">The file size in bytes.</param>
    /// <returns>True if the file size exceeds the warning threshold.</returns>
    bool IsWarningThreshold(long sizeInBytes);

    /// <summary>
    /// Checks if a file exceeds the critical threshold and should be rotated.
    /// </summary>
    /// <param name="sizeInBytes">The file size in bytes.</param>
    /// <returns>True if the file size exceeds the critical threshold.</returns>
    bool IsCriticalThreshold(long sizeInBytes);

    /// <summary>
    /// Generates a new file name with rotation suffix.
    /// </summary>
    /// <param name="originalPath">The original file path.</param>
    /// <param name="timestamp">Optional timestamp for the rotation. If null, uses current UTC time.</param>
    /// <returns>A new file path with rotation suffix.</returns>
    string GenerateRotatedFileName(string originalPath, DateTimeOffset? timestamp = null);

    /// <summary>
    /// Determines if automatic rotation should be performed based on file size.
    /// </summary>
    /// <param name="sizeInBytes">The file size in bytes.</param>
    /// <returns>True if the file should be automatically rotated.</returns>
    bool ShouldRotate(long sizeInBytes);

    /// <summary>
    /// Gets the configured maximum file size in bytes.
    /// </summary>
    long MaxFileSizeBytes { get; }

    /// <summary>
    /// Gets the configured warning threshold in bytes (typically 80% of max).
    /// </summary>
    long WarningThresholdBytes { get; }
}
