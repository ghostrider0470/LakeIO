using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using LakeIO.Annotations;
using LakeIO.Formatters.Interfaces;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace LakeIO.Formatters.Parquet
{
    /// <summary>
    /// Implementation of IParquetFileFormatter for handling Parquet file operations.
    /// Note: Parquet.Net v5 doesn't support writer-level compression/row group configuration.
    /// These settings are schema-level and will be implemented in future versions.
    /// </summary>
    public class ParquetFileFormatter : IParquetFileFormatter
    {
        /// <summary>
        /// Serializes a single item to a Parquet stream.
        /// </summary>
        /// <typeparam name="T">The type of the item to serialize, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
        /// <param name="item">The item to serialize.</param>
        /// <returns>A stream containing the serialized Parquet data.</returns>
        /// <exception cref="ArgumentNullException">Thrown when item is null.</exception>
        public async Task<Stream> SerializeAsync<T>(T item) where T : IParquetSerializable<T>
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            var items = new[] { item };
            return await SerializeItemsAsync(items);
        }

        /// <summary>
        /// Serializes a collection of items to a Parquet stream.
        /// </summary>
        /// <typeparam name="T">The type of the items to serialize, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
        /// <param name="items">The collection of items to serialize.</param>
        /// <returns>A stream containing the serialized Parquet data.</returns>
        /// <exception cref="ArgumentNullException">Thrown when items is null.</exception>
        /// <exception cref="ArgumentException">Thrown when items collection is empty.</exception>
        public async Task<Stream> SerializeItemsAsync<T>(IEnumerable<T> items) where T : IParquetSerializable<T>
        {
            if (items == null)
            {
                throw new ArgumentNullException(nameof(items));
            }

            var itemList = items.ToList();
            if (!itemList.Any())
            {
                throw new ArgumentException("At least one item is required", nameof(items));
            }

            var schema = GetSchema(itemList.First()!);
            var dataColumns = CreateDataColumns(itemList, schema);

            var stream = new MemoryStream();

            // Write to a temporary stream first to ensure proper disposal
            // Note: Parquet.Net v5 doesn't support ParquetOptions parameter - compression is schema-level
            using (var tempStream = new MemoryStream())
            {
                using (var writer = await ParquetWriter.CreateAsync(schema, tempStream))
                {
                    // Create a new row group in the parquet file
                    using (ParquetRowGroupWriter groupWriter = writer.CreateRowGroup())
                    {
                        foreach (var column in dataColumns)
                        {
                            await groupWriter.WriteColumnAsync(column);
                        }
                    }
                    // Writer disposes here, ensuring footer is written
                }

                // Copy the complete parquet file to the return stream
                tempStream.Position = 0;
                await tempStream.CopyToAsync(stream);
            }

            stream.Position = 0;
            return stream;
        }

        private static ParquetSchema GetSchema<T>(T item) where T : IParquetSerializable<T>
        {
            if (item is IParquetSerializable<T> serializable)
            {
                return serializable.BuildSchema();
            }

            // Fallback to reflection-based schema generation
            var type = typeof(T);
            var fields = new List<DataField>();
            var properties = type.GetProperties()
                .Where(p => p.CanRead && p.CanWrite && !p.GetIndexParameters().Any());

            foreach (var prop in properties)
            {
                // Check for ParquetColumn attribute first
                var attr = prop.GetCustomAttributes(typeof(ParquetColumnAttribute), true)
                    .FirstOrDefault() as ParquetColumnAttribute;

                if (attr != null)
                {
                    // Use the attribute to create the field
                    fields.Add(attr.CreateDataField(prop.PropertyType));
                }
                else
                {
                    // Create field based on property type
                    Type dataType = GetParquetDataType(prop.PropertyType);
                    bool isNullable = IsNullable(prop.PropertyType);
                    
                    // Create the appropriate DataField based on the type
                    DataField field;
                    
                    if (dataType == typeof(int) || dataType == typeof(int?))
                        field = new DataField<int>(prop.Name, isNullable);
                    else if (dataType == typeof(long) || dataType == typeof(long?))
                        field = new DataField<long>(prop.Name, isNullable);
                    else if (dataType == typeof(float) || dataType == typeof(float?))
                        field = new DataField<float>(prop.Name, isNullable);
                    else if (dataType == typeof(double) || dataType == typeof(double?))
                        field = new DataField<double>(prop.Name, isNullable);
                    else if (dataType == typeof(bool) || dataType == typeof(bool?))
                        field = new DataField<bool>(prop.Name, isNullable);
                    else if (dataType == typeof(DateTime) || dataType == typeof(DateTime?))
                        field = new DataField<DateTime>(prop.Name, isNullable);
                    else if (dataType == typeof(DateTimeOffset) || dataType == typeof(DateTimeOffset?))
                        field = new DataField<DateTimeOffset>(prop.Name, isNullable);
                    else if (dataType == typeof(decimal) || dataType == typeof(decimal?))
                        field = new DataField<decimal>(prop.Name, isNullable);
                    else if (dataType == typeof(Guid) || dataType == typeof(Guid?))
                        field = new DataField<Guid>(prop.Name, isNullable);
                    else
                        field = new DataField<string>(prop.Name, isNullable);
                    
                    fields.Add(field);
                }
            }

            if (fields.Count == 0)
            {
                throw new InvalidOperationException($"No serializable properties found on type {type.Name}");
            }

            return new ParquetSchema(fields);
        }

        private static List<DataColumn> CreateDataColumns<T>(IEnumerable<T> items, ParquetSchema schema) where T : IParquetSerializable<T>
        {
            var columns = new List<DataColumn>();
            var itemList = items.ToList();

            // Build a dictionary that maps COLUMN NAMES (from ParquetColumn attribute) to properties
            var properties = typeof(T).GetProperties()
                .Where(p => p.CanRead && p.CanWrite && !p.GetIndexParameters().Any())
                .ToDictionary(p =>
                {
                    // Check for ParquetColumn attribute to get the actual column name
                    var attr = p.GetCustomAttributes(typeof(ParquetColumnAttribute), true)
                        .FirstOrDefault() as ParquetColumnAttribute;
                    return attr?.Name ?? p.Name;
                }, p => p, StringComparer.OrdinalIgnoreCase);

            foreach (DataField field in schema.GetDataFields())
            {
                if (!properties.TryGetValue(field.Name, out var property))
                    continue;

                // Create the array with the correct type based on the field definition
                // This is critical for handling nullable types correctly
                Type arrayType;
                
                // Handle specific field types to ensure correct array creation
                if (field is DataField<int> intField)
                    arrayType = intField.HasNulls ? typeof(int?) : typeof(int);
                else if (field is DataField<long> longField)
                    arrayType = longField.HasNulls ? typeof(long?) : typeof(long);
                else if (field is DataField<float> floatField)
                    arrayType = floatField.HasNulls ? typeof(float?) : typeof(float);
                else if (field is DataField<double> doubleField)
                    arrayType = doubleField.HasNulls ? typeof(double?) : typeof(double);
                else if (field is DataField<bool> boolField)
                    arrayType = boolField.HasNulls ? typeof(bool?) : typeof(bool);
                else if (field is DataField<DateTime> dateTimeField)
                    arrayType = dateTimeField.HasNulls ? typeof(DateTime?) : typeof(DateTime);
                else if (field is DataField<DateTimeOffset> dateTimeOffsetField)
                    arrayType = dateTimeOffsetField.HasNulls ? typeof(DateTimeOffset?) : typeof(DateTimeOffset);
                else if (field is DataField<decimal> decimalField)
                    arrayType = decimalField.HasNulls ? typeof(decimal?) : typeof(decimal);
                else if (field is DataField<Guid> guidField)
                    arrayType = guidField.HasNulls ? typeof(Guid?) : typeof(Guid);
                else
                    arrayType = field.ClrType; // Default to field's CLR type for other types
                
                var data = Array.CreateInstance(arrayType, itemList.Count);

                for (int i = 0; i < itemList.Count; i++)
                {
                    var value = property.GetValue(itemList[i]);
                    
                    // Handle null values
                    if (value == null)
                    {
                        data.SetValue(GetDefaultValue(property.PropertyType), i);
                        continue;
                    }
                    
                    // Handle enum values by converting them to strings
                    if (value.GetType().IsEnum)
                    {
                        // If the field expects a string (which it should for enums), convert the enum to string
                        if (field.ClrType == typeof(string))
                        {
                            data.SetValue(value.ToString(), i);
                            continue;
                        }
                    }
                    
                    // For all other types, try to convert and set the value
                    try
                    {
                        // If the value type doesn't match the field type, try to convert it
                        if (value.GetType() != field.ClrType)
                        {
                            if (field.ClrType == typeof(string))
                            {
                                // Convert to string for string fields
                                data.SetValue(value.ToString(), i);
                            }
                            else if (field.ClrType == typeof(int) && value is long longValue)
                            {
                                // Handle common numeric conversions
                                data.SetValue((int)longValue, i);
                            }
                            else if (field.ClrType == typeof(long) && value is int intValue)
                            {
                                data.SetValue((long)intValue, i);
                            }
                            else if (field.ClrType == typeof(double) && value is float floatValue)
                            {
                                data.SetValue((double)floatValue, i);
                            }
                            else if (field.ClrType == typeof(float) && value is double doubleValue)
                            {
                                data.SetValue((float)doubleValue, i);
                            }
                            else
                            {
                                // Try general conversion
                                try
                                {
                                    var convertedValue = Convert.ChangeType(value, field.ClrType);
                                    data.SetValue(convertedValue, i);
                                }
                                catch
                                {
                                    // If conversion fails, use default value
                                    data.SetValue(GetDefaultValue(field.ClrType), i);
                                }
                            }
                        }
                        else
                        {
                            // Types match, set directly
                            data.SetValue(value, i);
                        }
                    }
                    catch (Exception)
                    {
                        // If all else fails, use the default value for the field type
                        data.SetValue(GetDefaultValue(field.ClrType), i);
                    }
                }

                columns.Add(new DataColumn(field, data));
            }

            return columns;
        }

        private static Type GetParquetDataType(Type type)
        {
            // Check if the type is nullable
            bool isNullable = IsNullable(type);
            var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

            // Map .NET types to Parquet types, preserving nullability
            if (underlyingType == typeof(int))
                return isNullable ? typeof(int?) : typeof(int);
            if (underlyingType == typeof(long))
                return isNullable ? typeof(long?) : typeof(long);
            if (underlyingType == typeof(float))
                return isNullable ? typeof(float?) : typeof(float);
            if (underlyingType == typeof(double))
                return isNullable ? typeof(double?) : typeof(double);
            if (underlyingType == typeof(decimal))
                return isNullable ? typeof(decimal?) : typeof(decimal);
            if (underlyingType == typeof(bool))
                return isNullable ? typeof(bool?) : typeof(bool);
            if (underlyingType == typeof(DateTime))
                return isNullable ? typeof(DateTime?) : typeof(DateTime);
            if (underlyingType == typeof(DateTimeOffset))
                return isNullable ? typeof(DateTimeOffset?) : typeof(DateTimeOffset);
            if (underlyingType == typeof(Guid))
                return isNullable ? typeof(Guid?) : typeof(Guid);
                
            // Handle byte arrays (not nullable in the same way as value types)
            if (underlyingType == typeof(byte[]))
                return typeof(byte[]);
            
            // Handle enum types by converting them to strings
            if (underlyingType.IsEnum)
                return typeof(string);
            
            // Default to string for all other types
            return typeof(string);
        }

        private static bool IsNullable(Type type)
        {
            if (!type.IsValueType) return true; // Reference types are nullable
            return Nullable.GetUnderlyingType(type) != null; // Nullable<T> types
        }
        
        /// <summary>
        /// Creates a nullable version of a value type if it's not already nullable.
        /// </summary>
        /// <param name="type">The type to make nullable.</param>
        /// <returns>A nullable version of the type if it's a value type, otherwise the original type.</returns>
        private static Type MakeNullableType(Type type)
        {
            // If it's already a nullable type or not a value type, return it as is
            if (!type.IsValueType || Nullable.GetUnderlyingType(type) != null)
                return type;
                
            // Create a nullable version of the value type
            return typeof(Nullable<>).MakeGenericType(type);
        }

        private static Type GetUnderlyingType(Type type)
        {
            return Nullable.GetUnderlyingType(type) ?? type;
        }

        private static object? GetDefaultValue(Type type)
        {
            return type.IsValueType ? Activator.CreateInstance(type) : null;
        }

        /// <summary>
        /// Deserializes a Parquet stream to a single item of the specified type.
        /// </summary>
        /// <typeparam name="T">The type to deserialize to, which must implement IParquetSerializable&lt;T&gt; and have a parameterless constructor.</typeparam>
        /// <param name="stream">The stream containing the Parquet data.</param>
        /// <returns>The deserialized item.</returns>
        public async Task<T> DeserializeAsync<T>(Stream stream) where T : IParquetSerializable<T>, new()
        {
            var items = await DeserializeItemsAsync<T>(stream);
            return items.FirstOrDefault() ?? throw new InvalidOperationException("No items found in the Parquet file");
        }

        /// <summary>
        /// Deserializes a Parquet stream to a collection of items of the specified type.
        /// </summary>
        /// <typeparam name="T">The type to deserialize to, which must implement IParquetSerializable&lt;T&gt; and have a parameterless constructor.</typeparam>
        /// <param name="stream">The stream containing the Parquet data.</param>
        /// <returns>A collection of deserialized items.</returns>
        /// <exception cref="ArgumentNullException">Thrown when stream is null.</exception>
        public async Task<IEnumerable<T>> DeserializeItemsAsync<T>(Stream stream) where T : IParquetSerializable<T>, new()
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            // Check if stream is seekable and has sufficient size
            if (stream.CanSeek)
            {
                var length = stream.Length - stream.Position;
                if (length < 12) // Minimum Parquet file size (PAR1 header + PAR1 footer)
                {
                    // Return empty collection for files too small to be valid Parquet
                    return Enumerable.Empty<T>();
                }
            }

            var result = new List<T>();

            try
            {
                using (var parquetReader = await ParquetReader.CreateAsync(stream, leaveStreamOpen: true))
            {
                // Read the first row group
                using (ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(0))
                {
                    var schema = parquetReader.Schema;

                    // Build a dictionary that maps COLUMN NAMES (from ParquetColumn attribute) to properties
                    // This mirrors the serialization logic in CreateDataColumns for consistency
                    var properties = typeof(T).GetProperties()
                        .Where(p => p.CanRead && p.CanWrite && !p.GetIndexParameters().Any())
                        .ToDictionary(p =>
                        {
                            // Check for ParquetColumn attribute to get the actual column name
                            var attr = p.GetCustomAttributes(typeof(ParquetColumnAttribute), true)
                                .FirstOrDefault() as ParquetColumnAttribute;
                            return attr?.Name ?? p.Name;
                        }, p => p, StringComparer.OrdinalIgnoreCase);

                    var items = new List<Dictionary<string, object>>();

                    // Read each field from the parquet file
                    foreach (DataField field in schema.GetDataFields())
                    {
                        if (!properties.TryGetValue(field.Name, out var property)) continue;

                        var data = (await groupReader.ReadColumnAsync(field)).Data;

                        for (int i = 0; i < data.Length; i++)
                        {
                            if (i >= items.Count)
                            {
                                items.Add(new Dictionary<string, object>());
                            }

                            items[i][property.Name] = data.GetValue(i) ?? DBNull.Value;
                        }
                    }

                    // Convert the dictionaries to strongly-typed objects
                    foreach (var item in items)
                    {
                        var obj = Activator.CreateInstance<T>();
                        foreach (var prop in properties.Values)
                        {
                            if (item.TryGetValue(prop.Name, out var value) && value != DBNull.Value)
                            {
                                try
                                {
                                    var targetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

                                    if (targetType.IsEnum && value is string strValue)
                                    {
                                        // Enum stored as string, parse it
                                        var enumValue = Enum.Parse(targetType, strValue);
                                        prop.SetValue(obj, enumValue);
                                    }
                                    else if (targetType.IsEnum && value is int intValue)
                                    {
                                        // Enum stored as int, cast it
                                        var enumValue = Enum.ToObject(targetType, intValue);
                                        prop.SetValue(obj, enumValue);
                                    }
                                    else if (value != null)
                                    {
                                        // Handle nullable type conversions
                                        if (prop.PropertyType.IsGenericType &&
                                            prop.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                                        {
                                            // Property is nullable, convert the value to the underlying type
                                            var convertedValue = Convert.ChangeType(value, targetType);
                                            prop.SetValue(obj, convertedValue);
                                        }
                                        else
                                        {
                                            // Property is not nullable, direct conversion
                                            prop.SetValue(obj, Convert.ChangeType(value, prop.PropertyType));
                                        }
                                    }
                                }
                                catch (InvalidCastException)
                                {
                                    // Handle specific type conversions that might fail
                                    // This commonly happens when Parquet has int32 but C# expects int32?
                                    if (value != null)
                                    {
                                        // Try to set the value directly, which should work for nullable types
                                        prop.SetValue(obj, value);
                                    }
                                }
                            }
                        }
                        result.Add(obj);
                    }
                }
            }

            return result;
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("not a Parquet file"))
            {
                // Handle files that are too small or invalid
                return Enumerable.Empty<T>();
            }
            catch (Exception ex) when (ex.Message.Contains("size too small") || ex.Message.Contains("PAR1"))
            {
                // Handle other Parquet format errors
                return Enumerable.Empty<T>();
            }
        }
    }
}
