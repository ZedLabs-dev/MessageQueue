namespace ZedLabs.MessageQueue.Samples.Docs;

// The code shown in docs/getting-started/*.md. Each "#region snippet:<name>" is copied verbatim into the page after a
// "<!-- snippet: <name> -->" marker; Packaging/Given_the_documentation_snippets fails when the two differ, and
// executes or validates the code, so the guides cannot drift from the API.

#region snippet:message
public sealed record OrderPlaced(string OrderId, string CustomerId, decimal Amount);
#endregion
