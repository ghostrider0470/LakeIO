namespace LakeIO.Telemetry;

/// <summary>
/// Configuration options for LakeIO observability features including
/// cost estimation and storage tier selection.
/// </summary>
public class ObservabilityOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether cost estimation is enabled
    /// for storage operations. When enabled, the <see cref="CostEstimator"/>
    /// can be used to approximate per-operation costs based on the configured
    /// <see cref="StorageTier"/>.
    /// </summary>
    /// <value><see langword="true"/> to enable cost estimation; otherwise, <see langword="false"/>. Default is <see langword="false"/>.</value>
    public bool EnableCostEstimation { get; set; }

    /// <summary>
    /// Gets or sets the Azure storage tier used for cost calculations.
    /// Different tiers have different per-operation pricing.
    /// </summary>
    /// <value>The storage tier. Default is <see cref="Telemetry.StorageTier.Hot"/>.</value>
    public StorageTier StorageTier { get; set; } = StorageTier.Hot;
}

/// <summary>
/// Represents the Azure Data Lake Storage Gen2 access tiers, each with different
/// storage and operation costs.
/// </summary>
public enum StorageTier
{
    /// <summary>
    /// Hot tier: optimized for frequent access. Lowest operation costs, highest storage costs.
    /// </summary>
    Hot,

    /// <summary>
    /// Cool tier: optimized for infrequent access (30+ days). Lower storage costs, higher operation costs.
    /// </summary>
    Cool,

    /// <summary>
    /// Cold tier: optimized for rare access (90+ days). Lower storage costs than Cool, higher operation costs.
    /// </summary>
    Cold,

    /// <summary>
    /// Archive tier: optimized for long-term retention (180+ days). Lowest storage costs, highest operation costs.
    /// Read operations require rehydration.
    /// </summary>
    Archive
}
