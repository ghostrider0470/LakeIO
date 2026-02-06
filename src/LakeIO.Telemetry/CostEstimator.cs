namespace LakeIO.Telemetry;

/// <summary>
/// Provides approximate per-operation cost estimation for Azure Data Lake Storage Gen2 operations
/// across different storage tiers.
/// </summary>
/// <remarks>
/// <para>
/// Pricing figures are approximate and based on publicly available Azure ADLS Gen2 pricing
/// as of 2025. Actual costs may vary by region, reservation, and pricing changes.
/// Use these estimates for relative cost awareness and optimization, not for billing purposes.
/// </para>
/// <para>
/// This class is stateless and thread-safe. All methods can be called concurrently.
/// </para>
/// </remarks>
public class CostEstimator
{
    /// <summary>
    /// Approximate costs per 10,000 operations for each storage tier (USD).
    /// </summary>
    /// <param name="WritePer10K">Cost per 10,000 write operations.</param>
    /// <param name="ReadPer10K">Cost per 10,000 read operations.</param>
    /// <param name="ListPer10K">Cost per 10,000 list operations.</param>
    /// <param name="DeletePer10K">Cost per 10,000 delete operations.</param>
    private record OperationCosts(decimal WritePer10K, decimal ReadPer10K, decimal ListPer10K, decimal DeletePer10K);

    /// <summary>
    /// Azure ADLS Gen2 approximate pricing per 10,000 operations by storage tier (USD).
    /// Prices are based on publicly available Azure pricing and may vary by region.
    /// </summary>
    private static readonly Dictionary<StorageTier, OperationCosts> CostTable = new()
    {
        [StorageTier.Hot] = new(WritePer10K: 0.065m, ReadPer10K: 0.005m, ListPer10K: 0.065m, DeletePer10K: 0m),
        [StorageTier.Cool] = new(WritePer10K: 0.13m, ReadPer10K: 0.013m, ListPer10K: 0.065m, DeletePer10K: 0m),
        [StorageTier.Cold] = new(WritePer10K: 0.195m, ReadPer10K: 0.065m, ListPer10K: 0.065m, DeletePer10K: 0m),
        [StorageTier.Archive] = new(WritePer10K: 0.26m, ReadPer10K: 6.50m, ListPer10K: 0.065m, DeletePer10K: 0m),
    };

    /// <summary>
    /// Estimates the approximate cost (in USD) of a single storage operation for the given
    /// category and tier.
    /// </summary>
    /// <param name="category">The operation category (Read, Write, List, or Delete).</param>
    /// <param name="tier">
    /// The storage tier to price against. Default is <see cref="StorageTier.Hot"/>.
    /// </param>
    /// <returns>
    /// The estimated cost in USD for a single operation. Returns <c>0</c> for
    /// <see cref="OperationCategory.Other"/> or unrecognized tiers.
    /// </returns>
    /// <remarks>
    /// Pricing is approximate and based on published Azure ADLS Gen2 rates.
    /// Actual costs may vary by region, negotiated pricing, and Azure pricing updates.
    /// </remarks>
    public decimal EstimateCost(OperationCategory category, StorageTier tier = StorageTier.Hot)
    {
        if (!CostTable.TryGetValue(tier, out var costs))
            return 0m;

        return category switch
        {
            OperationCategory.Write => costs.WritePer10K / 10_000m,
            OperationCategory.Read => costs.ReadPer10K / 10_000m,
            OperationCategory.List => costs.ListPer10K / 10_000m,
            OperationCategory.Delete => costs.DeletePer10K / 10_000m,
            _ => 0m
        };
    }

    /// <summary>
    /// Maps a LakeIO operation type name (e.g., <c>"json.write"</c>, <c>"file.delete"</c>)
    /// to its corresponding <see cref="OperationCategory"/> for cost estimation.
    /// </summary>
    /// <param name="operationType">
    /// The operation type name as used in metrics tags (e.g., <c>"json.read"</c>,
    /// <c>"parquet.write"</c>, <c>"directory.list"</c>).
    /// </param>
    /// <returns>
    /// The <see cref="OperationCategory"/> for the operation, or
    /// <see cref="OperationCategory.Other"/> if the operation type is not recognized.
    /// </returns>
    public static OperationCategory GetCategory(string operationType)
    {
        return operationType switch
        {
            // Write operations
            "json.write" => OperationCategory.Write,
            "json.append_ndjson" => OperationCategory.Write,
            "csv.write" => OperationCategory.Write,
            "file.upload" => OperationCategory.Write,
            "file.move" => OperationCategory.Write,
            "batch.move" => OperationCategory.Write,
            "batch.copy" => OperationCategory.Write,
            "parquet.write" => OperationCategory.Write,
            "parquet.write_stream" => OperationCategory.Write,
            "parquet.merge" => OperationCategory.Write,
            "parquet.compact_ndjson" => OperationCategory.Write,

            // Read operations
            "json.read" => OperationCategory.Read,
            "json.read_ndjson" => OperationCategory.Read,
            "csv.read" => OperationCategory.Read,
            "csv.read_stream" => OperationCategory.Read,
            "file.download" => OperationCategory.Read,
            "file.download_stream" => OperationCategory.Read,
            "file.exists" => OperationCategory.Read,
            "file.get_properties" => OperationCategory.Read,
            "directory.get_properties" => OperationCategory.Read,
            "parquet.read_stream" => OperationCategory.Read,
            "parquet.get_schema" => OperationCategory.Read,

            // List operations
            "directory.list" => OperationCategory.List,
            "directory.count" => OperationCategory.List,

            // Delete operations
            "file.delete" => OperationCategory.Delete,
            "batch.delete" => OperationCategory.Delete,

            _ => OperationCategory.Other
        };
    }
}

/// <summary>
/// Categorizes LakeIO storage operations for cost estimation purposes.
/// </summary>
public enum OperationCategory
{
    /// <summary>
    /// Read operations: downloading data, checking existence, reading properties or schemas.
    /// </summary>
    Read,

    /// <summary>
    /// Write operations: uploading, appending, moving, copying, merging, or compacting data.
    /// </summary>
    Write,

    /// <summary>
    /// List operations: enumerating directory contents or counting items.
    /// </summary>
    List,

    /// <summary>
    /// Delete operations: removing files or batch-deleting paths.
    /// </summary>
    Delete,

    /// <summary>
    /// Unrecognized operation type. Cost estimation returns zero.
    /// </summary>
    Other
}
