using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace LakeIO.Validation;

/// <summary>
/// Extension methods for common validation scenarios.
/// Reduces code duplication and provides consistent validation behavior.
/// </summary>
public static class ValidationExtensions
{
    /// <summary>
    /// Throws ArgumentNullException if the value is null.
    /// </summary>
    /// <typeparam name="T">The type of the value to validate.</typeparam>
    /// <param name="value">The value to validate.</param>
    /// <param name="parameterName">The name of the parameter (automatically captured).</param>
    /// <returns>The non-null value.</returns>
    /// <exception cref="ArgumentNullException">Thrown when value is null.</exception>
    public static T ThrowIfNull<T>(
        [NotNull] this T? value,
        [CallerArgumentExpression(nameof(value))] string? parameterName = null)
        where T : class
    {
        if (value is null)
        {
            throw new ArgumentNullException(parameterName);
        }

        return value;
    }

    /// <summary>
    /// Throws ArgumentException if the string is null, empty, or whitespace.
    /// </summary>
    /// <param name="value">The string to validate.</param>
    /// <param name="parameterName">The name of the parameter (automatically captured).</param>
    /// <returns>The non-null, non-empty string.</returns>
    /// <exception cref="ArgumentException">Thrown when value is null, empty, or whitespace.</exception>
    public static string ThrowIfNullOrWhiteSpace(
        [NotNull] this string? value,
        [CallerArgumentExpression(nameof(value))] string? parameterName = null)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException(
                $"{parameterName} cannot be null, empty, or whitespace.",
                parameterName);
        }

        return value;
    }

    /// <summary>
    /// Throws ArgumentException if the string is null or empty.
    /// </summary>
    /// <param name="value">The string to validate.</param>
    /// <param name="parameterName">The name of the parameter (automatically captured).</param>
    /// <returns>The non-null, non-empty string.</returns>
    /// <exception cref="ArgumentException">Thrown when value is null or empty.</exception>
    public static string ThrowIfNullOrEmpty(
        [NotNull] this string? value,
        [CallerArgumentExpression(nameof(value))] string? parameterName = null)
    {
        if (string.IsNullOrEmpty(value))
        {
            throw new ArgumentException(
                $"{parameterName} cannot be null or empty.",
                parameterName);
        }

        return value;
    }

    /// <summary>
    /// Throws ArgumentOutOfRangeException if the value is not within the specified range.
    /// </summary>
    /// <typeparam name="T">The type of the value (must be comparable).</typeparam>
    /// <param name="value">The value to validate.</param>
    /// <param name="min">The minimum allowed value (inclusive).</param>
    /// <param name="max">The maximum allowed value (inclusive).</param>
    /// <param name="parameterName">The name of the parameter (automatically captured).</param>
    /// <returns>The value if within range.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when value is outside the range.</exception>
    public static T ThrowIfOutOfRange<T>(
        this T value,
        T min,
        T max,
        [CallerArgumentExpression(nameof(value))] string? parameterName = null)
        where T : IComparable<T>
    {
        if (value.CompareTo(min) < 0 || value.CompareTo(max) > 0)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                value,
                $"{parameterName} must be between {min} and {max}.");
        }

        return value;
    }

    /// <summary>
    /// Throws ArgumentOutOfRangeException if the value is negative.
    /// </summary>
    /// <typeparam name="T">The numeric type.</typeparam>
    /// <param name="value">The value to validate.</param>
    /// <param name="parameterName">The name of the parameter (automatically captured).</param>
    /// <returns>The non-negative value.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when value is negative.</exception>
    public static T ThrowIfNegative<T>(
        this T value,
        [CallerArgumentExpression(nameof(value))] string? parameterName = null)
        where T : IComparable<T>, IConvertible
    {
        var zero = (T)Convert.ChangeType(0, typeof(T));
        if (value.CompareTo(zero) < 0)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                value,
                $"{parameterName} cannot be negative.");
        }

        return value;
    }

    /// <summary>
    /// Throws ArgumentException if the collection is null or empty.
    /// </summary>
    /// <typeparam name="T">The type of elements in the collection.</typeparam>
    /// <param name="collection">The collection to validate.</param>
    /// <param name="parameterName">The name of the parameter (automatically captured).</param>
    /// <returns>The non-null, non-empty collection.</returns>
    /// <exception cref="ArgumentException">Thrown when collection is null or empty.</exception>
    public static IEnumerable<T> ThrowIfNullOrEmpty<T>(
        [NotNull] this IEnumerable<T>? collection,
        [CallerArgumentExpression(nameof(collection))] string? parameterName = null)
    {
        if (collection is null)
        {
            throw new ArgumentNullException(parameterName);
        }

        // Check if empty (avoid multiple enumeration by using Any())
        if (!collection.Any())
        {
            throw new ArgumentException(
                $"{parameterName} cannot be empty.",
                parameterName);
        }

        return collection;
    }

    /// <summary>
    /// Validates that a file path is valid for Azure Data Lake Storage.
    /// </summary>
    /// <param name="filePath">The file path to validate.</param>
    /// <param name="parameterName">The name of the parameter (automatically captured).</param>
    /// <returns>The validated file path.</returns>
    /// <exception cref="ArgumentException">Thrown when path contains invalid characters.</exception>
    public static string ValidateFilePath(
        this string filePath,
        [CallerArgumentExpression(nameof(filePath))] string? parameterName = null)
    {
        filePath.ThrowIfNullOrWhiteSpace(parameterName);

        // Check for invalid characters (basic validation)
        var invalidChars = new[] { '<', '>', '"', '|', '\0', '\r', '\n' };
        if (filePath.IndexOfAny(invalidChars) >= 0)
        {
            throw new ArgumentException(
                $"{parameterName} contains invalid characters.",
                parameterName);
        }

        return filePath;
    }

    /// <summary>
    /// Validates that a file system name is valid for Azure Data Lake Storage.
    /// Must be lowercase, 3-63 characters, alphanumeric and hyphens only.
    /// </summary>
    /// <param name="fileSystemName">The file system name to validate.</param>
    /// <param name="parameterName">The name of the parameter (automatically captured).</param>
    /// <returns>The validated file system name.</returns>
    /// <exception cref="ArgumentException">Thrown when name is invalid.</exception>
    public static string ValidateFileSystemName(
        this string fileSystemName,
        [CallerArgumentExpression(nameof(fileSystemName))] string? parameterName = null)
    {
        fileSystemName.ThrowIfNullOrWhiteSpace(parameterName);

        // Azure file system naming rules:
        // - Lowercase only
        // - 3-63 characters
        // - Alphanumeric and hyphens only
        // - Cannot start or end with hyphen
        // - No consecutive hyphens

        if (fileSystemName.Length < 3 || fileSystemName.Length > 63)
        {
            throw new ArgumentException(
                $"{parameterName} must be between 3 and 63 characters.",
                parameterName);
        }

        if (!fileSystemName.All(c => char.IsLetterOrDigit(c) || c == '-'))
        {
            throw new ArgumentException(
                $"{parameterName} can only contain lowercase letters, numbers, and hyphens.",
                parameterName);
        }

        if (fileSystemName.StartsWith('-') || fileSystemName.EndsWith('-'))
        {
            throw new ArgumentException(
                $"{parameterName} cannot start or end with a hyphen.",
                parameterName);
        }

        if (fileSystemName.Contains("--"))
        {
            throw new ArgumentException(
                $"{parameterName} cannot contain consecutive hyphens.",
                parameterName);
        }

        if (fileSystemName != fileSystemName.ToLowerInvariant())
        {
            throw new ArgumentException(
                $"{parameterName} must be lowercase.",
                parameterName);
        }

        return fileSystemName;
    }
}
