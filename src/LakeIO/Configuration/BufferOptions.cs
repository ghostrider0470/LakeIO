using System.ComponentModel.DataAnnotations;

namespace LakeIO.Configuration;

/// <summary>
/// Configuration options for buffer file operations (NDJSON strategy).
/// Supports Options Pattern for .NET configuration binding.
/// </summary>
public class BufferOptions
{
    /// <summary>
    /// Configuration section name for binding from appsettings.json
    /// </summary>
    public const string SectionName = "Buffer";

    /// <summary>
    /// Buffer file name suffix.
    /// Default: "_buffer.ndjson"
    /// </summary>
    [Required(ErrorMessage = "BufferFileSuffix is required")]
    public string BufferFileSuffix { get; set; } = "_buffer.ndjson";

    /// <summary>
    /// Enable buffer strategy for high-throughput write operations.
    /// Default: true
    /// </summary>
    public bool EnableBufferStrategy { get; set; } = true;

    /// <summary>
    /// Automatically compact buffers to Parquet when buffer size exceeds threshold.
    /// Default: false (use timer-based compaction instead)
    /// </summary>
    public bool AutoCompactOnSizeThreshold { get; set; } = false;

    /// <summary>
    /// Buffer size threshold in bytes before triggering auto-compaction.
    /// Default: 104857600 (100 MB)
    /// Only used if AutoCompactOnSizeThreshold is true.
    /// </summary>
    [Range(10485760, 1073741824, ErrorMessage = "BufferSizeThreshold must be between 10 MB and 1 GB")]
    public long BufferSizeThreshold { get; set; } = 104857600; // 100 MB

    /// <summary>
    /// Default compaction schedule (CRON expression).
    /// Default: "0 0 * * * *" (hourly)
    /// Examples:
    /// - Every 15 min: "0 */15 * * * *"
    /// - Every 30 min: "0 */30 * * * *"
    /// - Every hour: "0 0 * * * *"
    /// - Daily: "0 0 0 * * *"
    /// </summary>
    public string DefaultCompactionSchedule { get; set; } = "0 0 * * * *";

    /// <summary>
    /// Delete buffer files after successful compaction.
    /// Default: true
    /// </summary>
    public bool DeleteBufferAfterCompaction { get; set; } = true;

    /// <summary>
    /// Maximum number of buffer items to batch in memory during compaction.
    /// Default: 10000
    /// Helps control memory usage during compaction.
    /// </summary>
    [Range(1000, 100000, ErrorMessage = "MaxBatchSize must be between 1000 and 100000")]
    public int MaxBatchSize { get; set; } = 10000;

    /// <summary>
    /// Enable streaming compaction for very large buffer files.
    /// Default: false (load entire buffer into memory)
    /// When true, processes buffer in chunks to minimize memory usage.
    /// </summary>
    public bool EnableStreamingCompaction { get; set; } = false;

    /// <summary>
    /// Use hourly partitioning for buffer files (year/month/day/hour/).
    /// Default: false (use daily partitioning)
    /// Recommended for high-volume scenarios (> 1M messages/day).
    /// </summary>
    public bool UseHourlyPartitioning { get; set; } = false;

    /// <summary>
    /// Number of concurrent compaction operations allowed.
    /// Default: 4
    /// Higher values = faster compaction but more memory usage.
    /// </summary>
    [Range(1, 16, ErrorMessage = "MaxConcurrentCompactions must be between 1 and 16")]
    public int MaxConcurrentCompactions { get; set; } = 4;

    /// <summary>
    /// Suffix added to buffer files during compaction to prevent race conditions.
    /// Default: ".compacting"
    /// Files with this suffix are atomically renamed during compaction.
    /// </summary>
    public string CompactingSuffix { get; set; } = ".compacting";

    /// <summary>
    /// Suffix added to buffer files that failed compaction.
    /// Default: ".failed"
    /// Failed buffers can be manually investigated or retried.
    /// </summary>
    public string FailedCompactionSuffix { get; set; } = ".failed";

    /// <summary>
    /// Automatically retry failed compactions on next run.
    /// Default: true
    /// If false, failed buffers remain with .failed suffix until manually handled.
    /// </summary>
    public bool RetryFailedCompactions { get; set; } = true;

    /// <summary>
    /// Continue compacting other buffers if one fails.
    /// Default: true
    /// If false, stops entire compaction process on first failure.
    /// </summary>
    public bool ContinueOnCompactionError { get; set; } = true;

    /// <summary>
    /// Force compaction on timer trigger regardless of buffer size threshold.
    /// Default: false
    /// If false, timer-based compaction will only compact buffers exceeding BufferSizeThreshold.
    /// If true, timer-based compaction will compact ALL buffers regardless of size.
    /// </summary>
    public bool ForceTimerCompaction { get; set; } = false;
}
