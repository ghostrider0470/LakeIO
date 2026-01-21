using Polly;
using Polly.Retry;

namespace LakeIO.Resilience;

/// <summary>
/// Factory for creating resilience policies (retry, circuit breaker) for Azure Data Lake operations.
/// </summary>
public interface IRetryPolicyFactory
{
    /// <summary>
    /// Gets the retry policy for Azure Storage operations.
    /// Handles transient failures with exponential backoff.
    /// </summary>
    ResiliencePipeline<T> GetStorageRetryPolicy<T>();

    /// <summary>
    /// Gets the retry policy for file operations (read, write, delete).
    /// </summary>
    ResiliencePipeline GetFileOperationPolicy();

    /// <summary>
    /// Gets the retry policy for metadata operations (list, get properties).
    /// </summary>
    ResiliencePipeline GetMetadataOperationPolicy();
}
