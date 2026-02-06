using FluentAssertions;
using Parquet.Schema;
using Xunit;

namespace LakeIO.Parquet.Tests.Schema;

/// <summary>
/// Tests for <see cref="SchemaEvolver"/> pure schema merge logic.
/// All tests are self-contained with no I/O or mocks -- SchemaEvolver is pure logic.
/// </summary>
public class SchemaEvolverTests
{
    private readonly SchemaEvolver _sut = new();

    [Fact]
    public void Evolve_IdenticalSchemas_ReturnsSameFields()
    {
        var existing = new ParquetSchema(
            new DataField<int>("Id"),
            new DataField<string>("Name"));

        var incoming = new ParquetSchema(
            new DataField<int>("Id"),
            new DataField<string>("Name"));

        var merged = _sut.Evolve(existing, incoming);

        merged.GetDataFields().Should().HaveCount(2);
        merged.GetDataFields().Select(f => f.Name).Should().ContainInOrder("Id", "Name");
    }

    [Fact]
    public void Evolve_IncomingHasNewColumn_AppendsAsNullable()
    {
        var existing = new ParquetSchema(
            new DataField<int>("Id"),
            new DataField<string>("Name"));

        var incoming = new ParquetSchema(
            new DataField<int>("Id"),
            new DataField<string>("Name"),
            new DataField<double>("Score"));

        var merged = _sut.Evolve(existing, incoming);

        merged.GetDataFields().Should().HaveCount(3);

        var scoreField = merged.GetDataFields().First(f => f.Name == "Score");
        scoreField.IsNullable.Should().BeTrue("new columns must be appended as nullable");
    }

    [Fact]
    public void Evolve_PreservesExistingColumnOrder()
    {
        var existing = new ParquetSchema(
            new DataField<string>("A"),
            new DataField<string>("B"),
            new DataField<string>("C"));

        var incoming = new ParquetSchema(
            new DataField<string>("C"),
            new DataField<string>("D"),
            new DataField<string>("A"));

        var merged = _sut.Evolve(existing, incoming);

        merged.GetDataFields().Select(f => f.Name)
            .Should().ContainInOrder("A", "B", "C", "D");
    }

    [Fact]
    public void Evolve_NewColumnAlreadyNullable_RemainsNullable()
    {
        var existing = new ParquetSchema(
            new DataField<int>("Id"));

        var incoming = new ParquetSchema(
            new DataField<int>("Id"),
            new DataField("Score", typeof(double), isNullable: true));

        var merged = _sut.Evolve(existing, incoming);

        var scoreField = merged.GetDataFields().First(f => f.Name == "Score");
        scoreField.IsNullable.Should().BeTrue("already-nullable fields should remain nullable");
    }

    [Fact]
    public void Evolve_CaseInsensitiveColumnMatching()
    {
        var existing = new ParquetSchema(
            new DataField<int>("Id"),
            new DataField<string>("Name"));

        var incoming = new ParquetSchema(
            new DataField<int>("id"),
            new DataField<string>("name"));

        var merged = _sut.Evolve(existing, incoming);

        merged.GetDataFields().Should().HaveCount(2, "case-insensitive match should not add duplicates");
        merged.GetDataFields().Select(f => f.Name).Should().ContainInOrder("Id", "Name");
    }

    [Fact]
    public void Evolve_NoNewColumns_ReturnsSameAsExisting()
    {
        var existing = new ParquetSchema(
            new DataField<int>("Id"),
            new DataField<string>("Name"),
            new DataField<double>("Score"));

        // Incoming is a subset of existing
        var incoming = new ParquetSchema(
            new DataField<int>("Id"),
            new DataField<string>("Name"));

        var merged = _sut.Evolve(existing, incoming);

        merged.GetDataFields().Should().HaveCount(3);
        merged.GetDataFields().Select(f => f.Name).Should().ContainInOrder("Id", "Name", "Score");
    }

    [Fact]
    public void Evolve_DisjointSchemas_AppendsAllIncomingAsNullable()
    {
        // Existing and incoming share no columns -- all incoming are "new"
        var existing = new ParquetSchema(
            new DataField<int>("Id"));

        var incoming = new ParquetSchema(
            new DataField<string>("Name"),
            new DataField<double>("Score"));

        var merged = _sut.Evolve(existing, incoming);

        merged.GetDataFields().Should().HaveCount(3);
        merged.GetDataFields().Select(f => f.Name).Should().ContainInOrder("Id", "Name", "Score");

        // Id preserves original nullable state; Name and Score are appended as nullable
        merged.GetDataFields().First(f => f.Name == "Name").IsNullable.Should().BeTrue();
        merged.GetDataFields().First(f => f.Name == "Score").IsNullable.Should().BeTrue();
    }

    [Fact]
    public void Evolve_NullExisting_ThrowsArgumentNullException()
    {
        var incoming = new ParquetSchema(
            new DataField<int>("Id"));

        var act = () => _sut.Evolve(null!, incoming);

        act.Should().Throw<ArgumentNullException>()
            .And.ParamName.Should().Be("existing");
    }

    [Fact]
    public void Evolve_NullIncoming_ThrowsArgumentNullException()
    {
        var existing = new ParquetSchema(
            new DataField<int>("Id"));

        var act = () => _sut.Evolve(existing, null!);

        act.Should().Throw<ArgumentNullException>()
            .And.ParamName.Should().Be("incoming");
    }

    [Fact]
    public void Evolve_MultipleNewColumns_AllAppendedAsNullable()
    {
        var existing = new ParquetSchema(
            new DataField<int>("Id"));

        var incoming = new ParquetSchema(
            new DataField<int>("Id"),
            new DataField<string>("Name"),
            new DataField<double>("Score"),
            new DataField<DateTime>("Timestamp"));

        var merged = _sut.Evolve(existing, incoming);

        merged.GetDataFields().Should().HaveCount(4);
        merged.GetDataFields().Select(f => f.Name)
            .Should().ContainInOrder("Id", "Name", "Score", "Timestamp");

        // Existing Id keeps its original nullable state; new fields are all nullable
        var newFields = merged.GetDataFields().Where(f => f.Name != "Id").ToList();
        newFields.Should().AllSatisfy(f => f.IsNullable.Should().BeTrue());
    }
}
