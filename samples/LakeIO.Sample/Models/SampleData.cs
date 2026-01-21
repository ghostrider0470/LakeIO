using LakeIO.Annotations;

namespace LakeIO.Sample.Models;

public class SampleData : IParquetSerializable<SampleData>
{
    [ParquetColumn("id")]
    public int Id { get; set; }

    [ParquetColumn("name")]
    public string? Name { get; set; }

    [ParquetColumn("timestamp")]
    public DateTime Timestamp { get; set; }

    [ParquetColumn("value")]
    public double Value { get; set; }

    [ParquetColumn("is_active")]
    public bool IsActive { get; set; }

    // Note: Dictionary types are not directly supported in Parquet,
    // so we'll exclude Metadata from Parquet serialization
}
