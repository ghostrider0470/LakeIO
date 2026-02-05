namespace LakeIO;

/// <summary>
/// Result of a batch operation containing per-item success/failure details.
/// </summary>
public class BatchResult
{
    /// <summary>Total number of items in the batch.</summary>
    public int TotalCount { get; init; }

    /// <summary>Number of items that completed successfully.</summary>
    public int SucceededCount { get; init; }

    /// <summary>Number of items that failed.</summary>
    public int FailedCount { get; init; }

    /// <summary>Whether all items in the batch completed successfully.</summary>
    public bool IsFullySuccessful => FailedCount == 0;

    /// <summary>Per-item results with individual success/failure details.</summary>
    public IReadOnlyList<BatchItemResult> Items { get; init; } = [];
}

/// <summary>
/// Result for an individual item within a batch operation.
/// </summary>
public class BatchItemResult
{
    /// <summary>The path of the item that was operated on.</summary>
    public string Path { get; init; } = string.Empty;

    /// <summary>Whether the operation on this item succeeded.</summary>
    public bool Succeeded { get; init; }

    /// <summary>Error message if the operation failed, otherwise <see langword="null"/>.</summary>
    public string? Error { get; init; }

    /// <summary>Exception that caused the failure, otherwise <see langword="null"/>.</summary>
    public Exception? Exception { get; init; }
}

/// <summary>
/// Progress report for batch operations, compatible with <see cref="IProgress{T}"/>.
/// </summary>
public class BatchProgress
{
    /// <summary>Number of items completed so far.</summary>
    public int Completed { get; init; }

    /// <summary>Total number of items in the batch.</summary>
    public int Total { get; init; }

    /// <summary>The path of the item currently being processed.</summary>
    public string CurrentPath { get; init; } = string.Empty;
}

/// <summary>
/// Specifies a source and destination path for a batch move operation.
/// </summary>
public class BatchMoveItem
{
    /// <summary>The source file path to move from.</summary>
    public required string SourcePath { get; init; }

    /// <summary>The destination file path to move to.</summary>
    public required string DestinationPath { get; init; }

    /// <summary>Whether to overwrite an existing file at the destination. Default is <see langword="false"/>.</summary>
    public bool Overwrite { get; init; }
}

/// <summary>
/// Specifies a source and destination path for a batch copy operation.
/// </summary>
public class BatchCopyItem
{
    /// <summary>The source file path to copy from.</summary>
    public required string SourcePath { get; init; }

    /// <summary>The destination file path to copy to.</summary>
    public required string DestinationPath { get; init; }

    /// <summary>Whether to overwrite an existing file at the destination. Default is <see langword="false"/>.</summary>
    public bool Overwrite { get; init; }
}
