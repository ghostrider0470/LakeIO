using FluentAssertions;
using Xunit;

namespace LakeIO.Tests.Queries;

public class PathFilterTests
{
    [Fact]
    public void WithExtension_MatchesFileWithExtension()
    {
        var filter = new PathFilter().WithExtension(".json");
        var predicate = filter.Build();

        var item = CreateItem("data/test.json");

        predicate(item).Should().BeTrue();
    }

    [Fact]
    public void WithExtension_RejectsFileWithDifferentExtension()
    {
        var filter = new PathFilter().WithExtension(".json");
        var predicate = filter.Build();

        var item = CreateItem("data/test.csv");

        predicate(item).Should().BeFalse();
    }

    [Fact]
    public void WithExtension_AutoAddsDot()
    {
        var filter = new PathFilter().WithExtension("json");
        var predicate = filter.Build();

        var item = CreateItem("data/test.json");

        predicate(item).Should().BeTrue();
    }

    [Theory]
    [InlineData(".JSON")]
    [InlineData(".Json")]
    [InlineData(".json")]
    public void WithExtension_IsCaseInsensitive(string extension)
    {
        var filter = new PathFilter().WithExtension(extension);
        var predicate = filter.Build();

        var item = CreateItem("data/test.json");

        predicate(item).Should().BeTrue();
    }

    [Fact]
    public void FilesOnly_ExcludesDirectories()
    {
        var filter = new PathFilter().FilesOnly();
        var predicate = filter.Build();

        var directory = new PathItem { Name = "folder", IsDirectory = true };
        var file = new PathItem { Name = "file.txt", IsDirectory = false };

        predicate(directory).Should().BeFalse();
        predicate(file).Should().BeTrue();
    }

    [Fact]
    public void DirectoriesOnly_ExcludesFiles()
    {
        var filter = new PathFilter().DirectoriesOnly();
        var predicate = filter.Build();

        var directory = new PathItem { Name = "folder", IsDirectory = true };
        var file = new PathItem { Name = "file.txt", IsDirectory = false };

        predicate(directory).Should().BeTrue();
        predicate(file).Should().BeFalse();
    }

    [Theory]
    [InlineData(2048, true)]
    [InlineData(1024, false)]
    [InlineData(512, false)]
    public void LargerThan_FiltersCorrectly(long contentLength, bool expected)
    {
        var filter = new PathFilter().LargerThan(1024);
        var predicate = filter.Build();

        var item = new PathItem { Name = "file.bin", ContentLength = contentLength };

        predicate(item).Should().Be(expected);
    }

    [Fact]
    public void LargerThan_ExcludesItemsWithNullContentLength()
    {
        var filter = new PathFilter().LargerThan(1024);
        var predicate = filter.Build();

        var item = new PathItem { Name = "file.bin", ContentLength = null };

        predicate(item).Should().BeFalse();
    }

    [Theory]
    [InlineData(512, true)]
    [InlineData(1024, false)]
    [InlineData(2048, false)]
    public void SmallerThan_FiltersCorrectly(long contentLength, bool expected)
    {
        var filter = new PathFilter().SmallerThan(1024);
        var predicate = filter.Build();

        var item = new PathItem { Name = "file.bin", ContentLength = contentLength };

        predicate(item).Should().Be(expected);
    }

    [Fact]
    public void SmallerThan_ExcludesItemsWithNullContentLength()
    {
        var filter = new PathFilter().SmallerThan(1024);
        var predicate = filter.Build();

        var item = new PathItem { Name = "file.bin", ContentLength = null };

        predicate(item).Should().BeFalse();
    }

    [Fact]
    public void ModifiedAfter_FiltersCorrectly()
    {
        var cutoff = new DateTimeOffset(2025, 6, 1, 0, 0, 0, TimeSpan.Zero);
        var filter = new PathFilter().ModifiedAfter(cutoff);
        var predicate = filter.Build();

        var recent = new PathItem { Name = "recent.json", LastModified = cutoff.AddDays(1) };
        var old = new PathItem { Name = "old.json", LastModified = cutoff.AddDays(-1) };
        var exact = new PathItem { Name = "exact.json", LastModified = cutoff };

        predicate(recent).Should().BeTrue();
        predicate(old).Should().BeFalse();
        predicate(exact).Should().BeFalse("boundary value should be excluded (strictly after)");
    }

    [Fact]
    public void ModifiedAfter_ExcludesItemsWithNullLastModified()
    {
        var cutoff = DateTimeOffset.UtcNow;
        var filter = new PathFilter().ModifiedAfter(cutoff);
        var predicate = filter.Build();

        var item = new PathItem { Name = "no-date.json", LastModified = null };

        predicate(item).Should().BeFalse();
    }

    [Fact]
    public void ModifiedBefore_FiltersCorrectly()
    {
        var cutoff = new DateTimeOffset(2025, 6, 1, 0, 0, 0, TimeSpan.Zero);
        var filter = new PathFilter().ModifiedBefore(cutoff);
        var predicate = filter.Build();

        var old = new PathItem { Name = "old.json", LastModified = cutoff.AddDays(-1) };
        var recent = new PathItem { Name = "recent.json", LastModified = cutoff.AddDays(1) };
        var exact = new PathItem { Name = "exact.json", LastModified = cutoff };

        predicate(old).Should().BeTrue();
        predicate(recent).Should().BeFalse();
        predicate(exact).Should().BeFalse("boundary value should be excluded (strictly before)");
    }

    [Fact]
    public void ModifiedBefore_ExcludesItemsWithNullLastModified()
    {
        var cutoff = DateTimeOffset.UtcNow;
        var filter = new PathFilter().ModifiedBefore(cutoff);
        var predicate = filter.Build();

        var item = new PathItem { Name = "no-date.json", LastModified = null };

        predicate(item).Should().BeFalse();
    }

    [Fact]
    public void NameContains_CaseInsensitive()
    {
        var filter = new PathFilter().NameContains("TEST");
        var predicate = filter.Build();

        var matching = CreateItem("test-data.json");
        var notMatching = CreateItem("other-data.json");

        predicate(matching).Should().BeTrue();
        predicate(notMatching).Should().BeFalse();
    }

    [Fact]
    public void NameContains_MatchesSubstring()
    {
        var filter = new PathFilter().NameContains("data");
        var predicate = filter.Build();

        var item = CreateItem("folder/my-data-file.json");

        predicate(item).Should().BeTrue();
    }

    [Fact]
    public void MultiplePredicates_AndSemantics()
    {
        var filter = new PathFilter()
            .WithExtension(".json")
            .FilesOnly()
            .LargerThan(1024);
        var predicate = filter.Build();

        // Matches all three conditions
        var good = new PathItem { Name = "data.json", IsDirectory = false, ContentLength = 2048 };
        predicate(good).Should().BeTrue();

        // Wrong extension
        var wrongExt = new PathItem { Name = "data.csv", IsDirectory = false, ContentLength = 2048 };
        predicate(wrongExt).Should().BeFalse();

        // Is a directory
        var directory = new PathItem { Name = "data.json", IsDirectory = true, ContentLength = 2048 };
        predicate(directory).Should().BeFalse();

        // Too small
        var tooSmall = new PathItem { Name = "data.json", IsDirectory = false, ContentLength = 512 };
        predicate(tooSmall).Should().BeFalse();
    }

    [Fact]
    public void EmptyFilter_MatchesEverything()
    {
        var filter = new PathFilter();
        var predicate = filter.Build();

        var file = new PathItem { Name = "anything.txt", IsDirectory = false, ContentLength = 100 };
        var dir = new PathItem { Name = "folder", IsDirectory = true };

        predicate(file).Should().BeTrue();
        predicate(dir).Should().BeTrue();
    }

    [Fact]
    public void FluentChaining_ReturnsSameInstance()
    {
        var filter = new PathFilter();
        var chained = filter
            .WithExtension(".json")
            .FilesOnly()
            .LargerThan(100)
            .SmallerThan(10000)
            .ModifiedAfter(DateTimeOffset.MinValue)
            .ModifiedBefore(DateTimeOffset.MaxValue)
            .NameContains("test");

        chained.Should().BeSameAs(filter);
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private static PathItem CreateItem(string name, bool isDirectory = false) =>
        new() { Name = name, IsDirectory = isDirectory };
}
