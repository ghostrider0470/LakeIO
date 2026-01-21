using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace LakeIO.Formatters.Interfaces
{
    /// <summary>
    /// Defines the contract for CSV file formatters that handle CSV-specific operations.
    /// Extends the base IFileFormatter interface with CSV-specific configuration.
    /// </summary>
    public interface ICsvFileFormatter : IFileFormatter
    {
        /// <summary>
        /// Gets or sets the delimiter used to separate fields in the CSV file.
        /// </summary>
        string Delimiter { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the CSV file should include a header row.
        /// </summary>
        bool HasHeader { get; set; }

        /// <summary>
        /// Gets or sets the quote character used to enclose fields that contain special characters.
        /// </summary>
        char Quote { get; set; }

        /// <summary>
        /// Gets or sets the escape character used to escape special characters within fields.
        /// </summary>
        char Escape { get; set; }

        /// <summary>
        /// Serializes a collection of objects to a CSV stream with custom column mapping.
        /// </summary>
        /// <typeparam name="T">The type of the objects to serialize.</typeparam>
        /// <param name="items">The collection of objects to serialize.</param>
        /// <param name="columnMapping">Optional dictionary mapping property names to CSV column headers.</param>
        /// <returns>A stream containing the CSV data.</returns>
        Task<Stream> SerializeItemsAsync<T>(IEnumerable<T> items, Dictionary<string, string> columnMapping = null);
    }
}