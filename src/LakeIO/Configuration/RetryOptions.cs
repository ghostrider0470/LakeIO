using System.ComponentModel.DataAnnotations;

namespace LakeIO.Configuration;

/// <summary>
/// Configuration options for retry policies and resilience patterns.
/// Supports Options Pattern for .NET configuration binding.
/// Used with Polly for resilient operations.
/// </summary>
public class RetryOptions
{
    /// <summary>
    /// Configuration section name for binding from appsettings.json
    /// </summary>
    public const string SectionName = "Retry";

    /// <summary>
    /// Maximum number of retry attempts for transient failures.
    /// Default: 3
    /// </summary>
    [Range(0, 10, ErrorMessage = "MaxRetryAttempts must be between 0 and 10")]
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>
    /// Initial delay between retries in milliseconds.
    /// Default: 1000 (1 second)
    /// Used as base for exponential backoff.
    /// </summary>
    [Range(100, 10000, ErrorMessage = "InitialDelayMs must be between 100 and 10000")]
    public int InitialDelayMs { get; set; } = 1000;

    /// <summary>
    /// Maximum delay between retries in milliseconds.
    /// Default: 30000 (30 seconds)
    /// Caps the exponential backoff delay.
    /// </summary>
    [Range(1000, 60000, ErrorMessage = "MaxDelayMs must be between 1000 and 60000")]
    public int MaxDelayMs { get; set; } = 30000;

    /// <summary>
    /// Retry strategy: Fixed (constant delay) or Exponential (increasing delay).
    /// Default: Exponential
    /// </summary>
    public RetryStrategy Strategy { get; set; } = RetryStrategy.Exponential;

    /// <summary>
    /// Enable circuit breaker pattern to prevent cascading failures.
    /// Default: true
    /// </summary>
    public bool EnableCircuitBreaker { get; set; } = true;

    /// <summary>
    /// Number of consecutive failures before opening the circuit.
    /// Default: 5
    /// </summary>
    [Range(2, 20, ErrorMessage = "CircuitBreakerThreshold must be between 2 and 20")]
    public int CircuitBreakerThreshold { get; set; } = 5;

    /// <summary>
    /// Duration in seconds the circuit stays open before attempting to close.
    /// Default: 60 seconds
    /// </summary>
    [Range(10, 300, ErrorMessage = "CircuitBreakerDurationSeconds must be between 10 and 300")]
    public int CircuitBreakerDurationSeconds { get; set; } = 60;

    /// <summary>
    /// Add jitter to retry delays to prevent thundering herd.
    /// Default: true
    /// </summary>
    public bool UseJitter { get; set; } = true;
}

/// <summary>
/// Retry strategy enumeration
/// </summary>
public enum RetryStrategy
{
    /// <summary>
    /// Fixed delay between retries (constant backoff)
    /// </summary>
    Fixed,

    /// <summary>
    /// Exponentially increasing delay between retries
    /// </summary>
    Exponential
}
