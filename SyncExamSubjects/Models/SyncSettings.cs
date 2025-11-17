namespace SyncExamSubjects.Models;

public sealed class TableSyncConfig
{
    public required string Name { get; init; }
    public required string PrimaryKey { get; init; }
    public required string CreateDateColumn { get; init; }
    public required string ModifyDateColumn { get; init; }
}

public sealed class SyncSettings
{
    public string LinkedServerName { get; init; } = string.Empty;
    public int BatchSize { get; init; } = 1000;
    public int CommandTimeoutSeconds { get; init; } = 120;
    public IReadOnlyList<TableSyncConfig> Tables { get; init; } = Array.Empty<TableSyncConfig>();
}
