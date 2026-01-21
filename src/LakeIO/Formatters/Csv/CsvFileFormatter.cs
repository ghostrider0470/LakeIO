using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using LakeIO.Formatters.Interfaces;
using CsvHelper;
using CsvHelper.Configuration;

namespace LakeIO.Formatters.Csv
{
    /// <summary>
    /// CSV implementation of the IFileFormatter and ICsvFileFormatter interfaces.
    /// </summary>
    public class CsvFileFormatter : ICsvFileFormatter
    {
        private readonly CsvConfiguration _csvConfiguration;

        /// <summary>
        /// Initializes a new instance of the <see cref="CsvFileFormatter"/> class.
        /// </summary>
        /// <param name="csvConfiguration">Optional CSV configuration settings.</param>
        public CsvFileFormatter(CsvConfiguration csvConfiguration = null)
        {
            _csvConfiguration = csvConfiguration ?? new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = ",",
                HasHeaderRecord = true,
                Quote = '"',
                Escape = '"'
            };
        }

        /// <inheritdoc />
        public string FileExtension => ".csv";

        /// <inheritdoc />
        public string ContentType => "text/csv";

        /// <inheritdoc />
        public string Delimiter
        {
            get => _csvConfiguration.Delimiter;
            set => _csvConfiguration.Delimiter = value;
        }

        /// <inheritdoc />
        public bool HasHeader
        {
            get => _csvConfiguration.HasHeaderRecord;
            set => _csvConfiguration.HasHeaderRecord = value;
        }

        /// <inheritdoc />
        public char Quote
        {
            get => _csvConfiguration.Quote;
            set => _csvConfiguration.Quote = value;
        }

        /// <inheritdoc />
        public char Escape
        {
            get => _csvConfiguration.Escape;
            set => _csvConfiguration.Escape = value;
        }

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
            return await SerializeItemsAsync(items, null);
        }

        /// <inheritdoc />
        public async Task<Stream> SerializeItemsAsync<T>(IEnumerable<T> items, Dictionary<string, string> columnMapping = null)
        {
            if (items == null)
            {
                throw new ArgumentNullException(nameof(items));
            }

            var stream = new MemoryStream();
            await using var writer = new StreamWriter(stream, Encoding.UTF8, 8192, leaveOpen: true);
            await using var csv = new CsvWriter(writer, _csvConfiguration);

            // If column mapping is provided, use manual header writing
            if (columnMapping != null && columnMapping.Any())
            {
                // Write custom headers
                if (_csvConfiguration.HasHeaderRecord)
                {
                    var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
                    foreach (var property in properties)
                    {
                        var columnName = columnMapping.TryGetValue(property.Name, out var mappedName) ? mappedName : property.Name;
                        csv.WriteField(columnName);
                    }
                    await csv.NextRecordAsync();
                }

                // Write data rows
                foreach (var item in items)
                {
                    var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
                    foreach (var property in properties)
                    {
                        var value = property.GetValue(item);
                        csv.WriteField(value?.ToString() ?? string.Empty);
                    }
                    await csv.NextRecordAsync();
                }
            }
            else
            {
                // Use default CsvHelper behavior
                await csv.WriteRecordsAsync(items);
            }

            await csv.FlushAsync();
            await writer.FlushAsync();
            
            stream.Position = 0;
            return stream;
        }

        /// <inheritdoc />
        public async Task<T> DeserializeAsync<T>(Stream stream)
        {
            var items = await DeserializeItemsAsync<T>(stream);
            return items.FirstOrDefault();
        }

        /// <inheritdoc />
        public async Task<IEnumerable<T>> DeserializeItemsAsync<T>(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            using var reader = new StreamReader(stream, Encoding.UTF8, true, 8192, leaveOpen: true);
            using var csv = new CsvReader(reader, _csvConfiguration);
            
            var records = new List<T>();
            await foreach (var record in csv.GetRecordsAsync<T>())
            {
                records.Add(record);
            }
            
            return records;
        }

    }
}