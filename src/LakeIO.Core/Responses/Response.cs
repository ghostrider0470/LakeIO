namespace LakeIO;

/// <summary>
/// Represents a response from a LakeIO operation with a value and raw HTTP response.
/// </summary>
/// <typeparam name="T">The type of the response value.</typeparam>
public class Response<T>
{
    /// <summary>
    /// Creates a new Response wrapping a value and raw HTTP response.
    /// </summary>
    /// <param name="value">The deserialized value.</param>
    /// <param name="rawResponse">The raw Azure HTTP response.</param>
    public Response(T value, Azure.Response rawResponse)
    {
        Value = value;
        RawResponse = rawResponse ?? throw new ArgumentNullException(nameof(rawResponse));
    }

    /// <summary>The deserialized value of the response.</summary>
    public T Value { get; }

    /// <summary>The raw HTTP response from the Azure SDK.</summary>
    public Azure.Response RawResponse { get; }

    /// <summary>
    /// Gets the raw HTTP response. Alias for <see cref="RawResponse"/>, matching Azure SDK convention.
    /// </summary>
    public Azure.Response GetRawResponse() => RawResponse;

    /// <summary>
    /// Implicit conversion to the value type for convenience.
    /// Allows <c>StorageResult result = await operation;</c> without accessing .Value.
    /// </summary>
    public static implicit operator T(Response<T> response)
    {
        ArgumentNullException.ThrowIfNull(response);
        return response.Value;
    }
}
