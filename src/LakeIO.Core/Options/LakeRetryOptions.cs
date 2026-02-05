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
}
