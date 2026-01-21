using System.Text.Json;
using System.Text.Json.Serialization;

namespace LakeIO.Serialization;

/// <summary>
/// Extension methods for creating and configuring JsonSerializerOptions for Azure Data Lake storage.
/// </summary>
public static class JsonSerializerOptionsExtensions
{
    /// <summary>
    /// Creates default JsonSerializerOptions optimized for Azure Data Lake storage.
    /// </summary>
    public static JsonSerializerOptions CreateDefaultOptions()
    {
        return new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false,
            PropertyNameCaseInsensitive = true,
            Converters =
            {
                new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)
            }
        };
    }

    /// <summary>
    /// Creates JsonSerializerOptions for compact JSON output (no whitespace).
    /// </summary>
    public static JsonSerializerOptions CreateCompactOptions()
    {
        var options = CreateDefaultOptions();
        options.WriteIndented = false;
        return options;
    }

    /// <summary>
    /// Creates JsonSerializerOptions for pretty-printed JSON output.
    /// </summary>
    public static JsonSerializerOptions CreateIndentedOptions()
    {
        var options = CreateDefaultOptions();
        options.WriteIndented = true;
        return options;
    }
}
