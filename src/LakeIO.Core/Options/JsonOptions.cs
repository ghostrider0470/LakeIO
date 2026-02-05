using System.Text.Json;

namespace LakeIO;

/// <summary>
/// Per-operation configuration for JSON operations.
/// </summary>
/// <remarks>
/// When <see cref="SerializerOptions"/> is null, the operation falls back to
/// <see cref="LakeClientOptions.JsonSerializerOptions"/>, then to
/// <see cref="System.Text.Json"/> defaults.
/// </remarks>
public class JsonOptions
{
    /// <summary>
    /// JSON serializer options for this specific operation.
    /// When null, falls back to <see cref="LakeClientOptions.JsonSerializerOptions"/>,
    /// then to <see cref="System.Text.Json"/> defaults.
    /// </summary>
    public JsonSerializerOptions? SerializerOptions { get; set; }
}
