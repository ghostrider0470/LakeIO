using Parquet.Schema;

namespace LakeIO.Parquet;

/// <summary>
/// Implements schema evolution by merging existing and incoming Parquet schemas
/// using the AddNewColumnsAsNullable strategy.
/// </summary>
/// <remarks>
/// <para>This evolver preserves all existing columns in their original order and appends
/// any new columns from the incoming schema as nullable fields. This ensures that existing
/// row groups (which have no data for new columns) remain valid — readers return null for
/// the missing values.</para>
/// <para>When both schemas contain a column with the same name (case-insensitive),
/// the existing column definition takes precedence and the incoming definition is ignored.</para>
/// </remarks>
public class SchemaEvolver
{
    /// <summary>
    /// Evolves a schema by keeping all existing columns in order and appending new columns
    /// from the incoming schema as nullable.
    /// </summary>
    /// <param name="existing">
    /// The schema of the existing Parquet file. All columns are preserved in their original order.
    /// </param>
    /// <param name="incoming">
    /// The schema derived from the incoming data type. New columns not present in
    /// <paramref name="existing"/> are appended as nullable fields.
    /// </param>
    /// <returns>
    /// A merged <see cref="ParquetSchema"/> containing all existing columns followed by any
    /// new columns from the incoming schema (made nullable).
    /// </returns>
    /// <remarks>
    /// <para>Existing column definitions always take precedence over incoming ones with the same name.
    /// Column name matching is case-insensitive (<see cref="StringComparison.OrdinalIgnoreCase"/>).</para>
    /// <para>New columns are forced nullable via <see cref="DataField"/> reconstruction with
    /// <c>hasNulls: true</c>, because prior row groups contain no data for these columns.</para>
    /// </remarks>
    public ParquetSchema Evolve(ParquetSchema existing, ParquetSchema incoming)
    {
        ArgumentNullException.ThrowIfNull(existing);
        ArgumentNullException.ThrowIfNull(incoming);

        var mergedFields = new List<Field>(existing.Fields);

        var existingNames = new HashSet<string>(
            existing.GetDataFields().Select(f => f.Name),
            StringComparer.OrdinalIgnoreCase);

        foreach (var incomingField in incoming.GetDataFields())
        {
            if (!existingNames.Contains(incomingField.Name))
            {
                mergedFields.Add(MakeNullable(incomingField));
            }
        }

        return new ParquetSchema(mergedFields);
    }

    /// <summary>
    /// Returns a nullable copy of the field if it is not already nullable.
    /// </summary>
    private static DataField MakeNullable(DataField field)
    {
        if (field.IsNullable) return field;
        return new DataField(field.Name, field.ClrType, isNullable: true);
    }
}
