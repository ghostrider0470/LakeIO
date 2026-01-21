using LakeIO.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LakeIO.Services;

/// <summary>
/// Implementation of IFileSizeManager for managing file size thresholds and automatic rotation.
/// Prevents Parquet files from exceeding optimal sizes for query performance.
/// </summary>
public class FileSizeManager : IFileSizeManager
{
    private readonly ILogger<FileSizeManager> _logger;
    private readonly ParquetOptions _options;

    /// <summary>
    /// Initializes a new instance of the FileSizeManager class.
    /// </summary>
    /// <param name="logger">Logger for diagnostics.</param>
    /// <param name="options">Parquet configuration options containing size thresholds.</param>
    public FileSizeManager(
        ILogger<FileSizeManager> logger,
        IOptions<ParquetOptions> options)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public long MaxFileSizeBytes => _options.MaxFileSize;

    /// <inheritdoc />
    public long WarningThresholdBytes => (long)(_options.MaxFileSize * _options.FileSizeWarningThreshold);

    /// <inheritdoc />
    public bool IsWarningThreshold(long sizeInBytes)
    {
        var isWarning = sizeInBytes >= WarningThresholdBytes;

        if (isWarning)
        {
            _logger.LogWarning(
                "File size {SizeBytes} bytes ({SizeMB:F2} MB) exceeds warning threshold {ThresholdBytes} bytes ({ThresholdMB:F2} MB)",
                sizeInBytes,
                sizeInBytes / 1_048_576.0,
                WarningThresholdBytes,
                WarningThresholdBytes / 1_048_576.0);
        }

        return isWarning;
    }

    /// <inheritdoc />
    public bool IsCriticalThreshold(long sizeInBytes)
    {
        var isCritical = sizeInBytes >= MaxFileSizeBytes;

        if (isCritical)
        {
            _logger.LogError(
                "File size {SizeBytes} bytes ({SizeMB:F2} MB) exceeds critical threshold {MaxBytes} bytes ({MaxMB:F2} MB)",
                sizeInBytes,
                sizeInBytes / 1_048_576.0,
                MaxFileSizeBytes,
                MaxFileSizeBytes / 1_048_576.0);
        }

        return isCritical;
    }

    /// <inheritdoc />
    public bool ShouldRotate(long sizeInBytes)
    {
        return IsCriticalThreshold(sizeInBytes);
    }

    /// <inheritdoc />
    public string GenerateRotatedFileName(string originalPath, DateTimeOffset? timestamp = null)
    {
        if (string.IsNullOrWhiteSpace(originalPath))
        {
            throw new ArgumentException("Original path cannot be null or empty.", nameof(originalPath));
        }

        var time = timestamp ?? DateTimeOffset.UtcNow;
        var directory = Path.GetDirectoryName(originalPath) ?? string.Empty;
        var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(originalPath);
        var extension = Path.GetExtension(originalPath);

        // Format: original_yyyyMMdd_HHmmss_fff.ext
        var rotatedFileName = $"{fileNameWithoutExtension}_{time:yyyyMMdd_HHmmss_fff}{extension}";
        var rotatedPath = Path.Combine(directory, rotatedFileName);

        _logger.LogInformation(
            "Generated rotated file name: {OriginalPath} -> {RotatedPath}",
            originalPath,
            rotatedPath);

        return rotatedPath;
    }
}
