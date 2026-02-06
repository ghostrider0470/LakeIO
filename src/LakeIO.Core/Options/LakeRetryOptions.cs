using Azure.Core;

namespace LakeIO;

/// <summary>
/// Retry options for LakeIO operations. Maps to <c>Azure.Core.RetryOptions</c> internally.
/// </summary>
public class LakeRetryOptions
{
    /// <summary>Maximum number of retry attempts. Default: 3.</summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>Delay between retries. Default: 800ms.</summary>
    public TimeSpan Delay { get; set; } = TimeSpan.FromMilliseconds(800);

    /// <summary>Maximum delay between retries. Default: 1 minute.</summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Retry mode. Default: Exponential.</summary>
    public RetryMode RetryMode { get; set; } = RetryMode.Exponential;

    /// <summary>
    /// Additional HTTP status codes to retry at the application level.
    /// These are retried by LakeIO's Polly-based <c>RetryHelper</c>, in addition to the
    /// transport-level codes retried by the Azure SDK (408, 429, 500, 502, 503, 504).
    /// Default is empty (no additional retries). Common use: add 409 for Conflict.
    /// </summary>
    public IList<int> AdditionalRetryStatusCodes { get; set; } = new List<int>();
}
