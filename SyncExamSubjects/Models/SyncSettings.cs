namespace SyncExamSubjects.Models;

public sealed class TableSyncConfig
{
    public required string Name { get; init; }
    public required string PrimaryKey { get; init; }
    public required string CreateDateColumn { get; init; }
    public required string ModifyDateColumn { get; init; }
}

public sealed class RetrySettings
{
    public int MaxAttempts { get; init; } = 3;
    public int BaseDelaySeconds { get; init; } = 2; // starting backoff
    public int MaxDelaySeconds { get; init; } = 30; // cap backoff
}

public sealed class SyncSettings
{
    public string LinkedServerName { get; init; } = string.Empty;
    public int BatchSize { get; init; } = 1000;
    public int CommandTimeoutSeconds { get; init; } = 120;
    public bool DryRun { get; init; } = false;
    public RetrySettings Retry { get; init; } = new();
    public IReadOnlyList<TableSyncConfig> Tables { get; init; } = Array.Empty<TableSyncConfig>();
}
