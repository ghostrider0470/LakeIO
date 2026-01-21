using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using LakeIO.Formatters.Interfaces;
using LakeIO.Serialization;

namespace LakeIO.Formatters.Json;

/// <summary>
/// Modern JSON formatter implementation using System.Text.Json for better performance.
/// Recommended for new code. Use JsonFileFormatter for backward compatibility with Newtonsoft.Json.
/// </summary>
public class SystemTextJsonFormatter : IFileFormatter
{
    private readonly JsonSerializerOptions _options;

    /// <summary>
    /// Initializes a new instance of the SystemTextJsonFormatter class.
    /// </summary>
    /// <param name="options">Optional JSON serializer options. If null, default options are used.</param>
    public SystemTextJsonFormatter(JsonSerializerOptions? options = null)
    {
        _options = options ?? JsonSerializerOptionsExtensions.CreateDefaultOptions();
    }

    /// <inheritdoc />
    public string FileExtension => ".json";

    /// <inheritdoc />
    public string ContentType => "application/json";

    /// <inheritdoc />
    public async Task<Stream> SerializeAsync<T>(T item)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        return await SerializeItemsAsync(new[] { item });
    }

    /// <inheritdoc />
    public async Task<Stream> SerializeItemsAsync<T>(IEnumerable<T> items)
    {
        if (items == null)
        {
            throw new ArgumentNullException(nameof(items));
        }

        var itemsList = items.ToList();
        var stream = new MemoryStream();

        // Serialize single item directly, or array for multiple items
        if (itemsList.Count == 1)
        {
            await JsonSerializer.SerializeAsync(stream, itemsList[0], _options);
        }
        else
        {
            await JsonSerializer.SerializeAsync(stream, itemsList, _options);
        }

        stream.Position = 0;
        return stream;
    }

    /// <inheritdoc />
    public async Task<T> DeserializeAsync<T>(Stream stream)
    {
        var items = await DeserializeItemsAsync<T>(stream);
        return items.FirstOrDefault()!;
    }

    /// <inheritdoc />
    public async Task<IEnumerable<T>> DeserializeItemsAsync<T>(Stream stream)
    {
        if (stream == null)
        {
            throw new ArgumentNullException(nameof(stream));
        }

        // Try to deserialize as array first
        try
        {
            var items = await JsonSerializer.DeserializeAsync<List<T>>(stream, _options);
            return items ?? (IEnumerable<T>)Array.Empty<T>();
        }
        catch (JsonException)
        {
            // If array deserialization fails, try single item
            stream.Position = 0;
            var item = await JsonSerializer.DeserializeAsync<T>(stream, _options);
            return item != null ? new[] { item } : (IEnumerable<T>)Array.Empty<T>();
        }
    }
}
