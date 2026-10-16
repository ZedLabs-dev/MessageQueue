# Instructions for MessageQueue Project Refactoring

## Coding Standards to Apply

### 2. Test Method Naming Convention
Rename all test methods to use the `When_<Condition_Or_Action>` pattern:

**Before:**
```csharp
public void SendMessage_ShouldReturnTrue_WhenQueueExists()
public void Constructor_ShouldThrowException_WhenConnectionStringIsNull()
public async Task ProcessAsync_ShouldComplete_WhenMessageIsValid()
```

**After:**
```csharp
public void When_Queue_Exists()
public void When_ConnectionString_Is_Null()
public async Task When_Message_Is_Valid()
```

### 3. Use FluentAssertions with `because` Messages
Add FluentAssertions package and convert all assertions:

```bash
dotnet add package FluentAssertions
```

**Before:**
```csharp
Assert.Equal(expected, actual);
Assert.True(result.Success);
Assert.NotNull(message);
Assert.Throws<ArgumentException>(() => ...);
```

**After:**
```csharp
actual.Should().Be(expected, "it should <expected behavior>");
result.Success.Should().BeTrue("it should <expected behavior>");
message.Should().NotBeNull("it should <expected behavior>");
action.Should().Throw<ArgumentException>("it should <expected behavior>");
```

The `because` message should:
- Start with `"it should"` or `"it should not"`
- Describe expected behavior, not implementation
- Read naturally with the test name: "When X, it should Y"

### 4. Remove Arrange/Act/Assert Comments
Remove explicit `// Arrange`, `// Act`, `// Assert` comments. The structure should be obvious from the code.

### 5. Configuration Classes
For settings/options classes:
- No XML comments
- Use `[Required]` and `[Range]` attributes for validation
- Create separate `IValidateOptions<T>` validator class
- Use `const` for section names and defaults

Example:
```csharp
public class MessageQueueSettings
{
    public const string SectionName = "MessageQueue";
    
    [Required]
    public string ConnectionString { get; set; } = string.Empty;
    
    public int MaxRetryAttempts { get; set; } = 3;
    public TimeSpan MessageTimeout { get; set; } = TimeSpan.FromSeconds(30);
}

public class MessageQueueSettingsValidator : IValidateOptions<MessageQueueSettings>
{
    public ValidateOptionsResult Validate(string? name, MessageQueueSettings options)
    {
        var errors = new List<string>();
        
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            errors.Add("Connection string is required");
            
        return errors.Count > 0
            ? ValidateOptionsResult.Fail(errors)
            : ValidateOptionsResult.Success;
    }
}
```

### 6. General Engineering Principles
- Make only directly requested changes
- No speculative features or "improvements"
- Trust internal code; validate only at system boundaries
- Reuse existing abstractions; follow DRY
- Use mocks only for external dependencies in tests

## Verification Steps
After making changes:
1. Run `dotnet build` - should succeed with no errors
2. Run `dotnet test` - all tests should pass
3. Verify test output reads naturally: "When X, it should Y"

