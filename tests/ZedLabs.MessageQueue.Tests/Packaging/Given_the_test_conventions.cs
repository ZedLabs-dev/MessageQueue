using System.Reflection;
using AwesomeAssertions;

namespace ZedLabs.MessageQueue.Tests.Packaging;

// CLAUDE.md "Testing": every spec class is named for its context and carries exactly one Category trait, so
// `--filter Category=Unit` is the complete fast loop and nothing slips out of both the Unit and Integration runs.
[Trait("Category", "Unit")]
public class Given_the_test_conventions
{
    static readonly IReadOnlyList<Type> SpecClasses = typeof(Given_the_test_conventions).Assembly.GetTypes()
        .Where(t => t.GetMethods().Any(m => m.GetCustomAttributes<FactAttribute>(inherit: true).Any()))
        .ToList();

    static IEnumerable<string> Categories(Type type) =>
        type.GetCustomAttributesData()
            .Where(a => a.AttributeType == typeof(TraitAttribute) && (string?)a.ConstructorArguments[0].Value == "Category")
            .Select(a => (string)a.ConstructorArguments[1].Value!);

    [Fact]
    public void When_Listing_The_Spec_Classes() =>
        SpecClasses.Should().HaveCountGreaterThan(50, "it should find the spec classes it checks");

    [Fact]
    public void When_Checking_Their_Category_Traits() =>
        SpecClasses.Where(t => Categories(t).ToList() is not [var category] || category is not ("Unit" or "Integration"))
            .Select(t => t.FullName).Should().BeEmpty("it should give every spec class exactly one Category trait, Unit or Integration");

    [Fact]
    public void When_Checking_Their_Names()
    {
        SpecClasses.Where(t => !t.Name.StartsWith("Given_", StringComparison.Ordinal))
            .Select(t => t.FullName).Should().BeEmpty("it should name every spec class Given_<context>");
        SpecClasses.SelectMany(t => t.GetMethods().Where(m => m.GetCustomAttributes<FactAttribute>(inherit: true).Any()))
            .Where(m => !m.Name.StartsWith("When_", StringComparison.Ordinal))
            .Select(m => $"{m.DeclaringType!.Name}.{m.Name}").Should().BeEmpty("it should name every spec When_<Condition_Or_Action>");
    }
}
