using System.Data;
using Microsoft.Data.SqlClient;
using Dapper;
using SyncExamSubjects.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

namespace SyncExamSubjects.Data;

public interface ISqlRepository
{
    Task<(DateTime? lastRunAt, long? lastProcessedId)> GetLastRunAsync(string tableName, CancellationToken ct);
    Task<int> StartRunLogAsync(string tableName, CancellationToken ct);
    Task UpdateRunLogCompleteAsync(int runLogId, string tableName, DateTime lastRunAt, long? lastProcessedId, int inserted, int updated, int scanned, string status, string? error, CancellationToken ct);
    Task<int> MergeBatchAsync(string tableName, string primaryKey, IEnumerable<IDictionary<string, object?>> rows, CancellationToken ct);
}

public sealed class SqlRepository : ISqlRepository
{
    private readonly string _connectionString;
    private readonly SyncSettings _settings;
    private readonly ILogger<SqlRepository> _logger;

    public SqlRepository(IConfiguration config, SyncSettings settings, ILogger<SqlRepository> logger)
    {
        _connectionString = config.GetConnectionString("SqlServer") ?? throw new InvalidOperationException("Missing SqlServer connection string");
        _settings = settings;
        _logger = logger;
    }

    private SqlConnection CreateConnection() => new(_connectionString);

    public async Task<(DateTime? lastRunAt, long? lastProcessedId)> GetLastRunAsync(string tableName, CancellationToken ct)
    {
        const string sql = "SELECT TOP 1 last_run_at, last_processed_id FROM S4Job_Run_Log WHERE table_name=@table ORDER BY last_run_at DESC";
        await using var conn = CreateConnection();
        var result = await conn.QueryFirstOrDefaultAsync(sql, new { table = tableName });
        if (result == null) return (null, null);
        DateTime? lastRunAt = result.last_run_at == null ? null : (DateTime)result.last_run_at;
        long? lastProcessedId = result.last_processed_id == null ? null : (long)result.last_processed_id;
        return (lastRunAt, lastProcessedId);
    }

    public async Task<int> StartRunLogAsync(string tableName, CancellationToken ct)
    {
        const string sql = @"INSERT INTO S4Job_Run_Log(table_name, started_at, status) VALUES(@t, SYSUTCDATETIME(), 'Running'); SELECT SCOPE_IDENTITY();";
        await using var conn = CreateConnection();
        var id = await conn.ExecuteScalarAsync<decimal>(sql, new { t = tableName });
        return (int)id;
    }

    public async Task UpdateRunLogCompleteAsync(int runLogId, string tableName, DateTime lastRunAt, long? lastProcessedId, int inserted, int updated, int scanned, string status, string? error, CancellationToken ct)
    {
        const string sql = @"UPDATE S4Job_Run_Log SET completed_at = SYSUTCDATETIME(), last_run_at=@lrun, last_processed_id=@lpid, records_inserted=@ins, records_updated=@upd, records_scanned=@scn, status=@st, error_message=@err WHERE run_log_id=@id";
        await using var conn = CreateConnection();
        await conn.ExecuteAsync(sql, new { id = runLogId, lrun = lastRunAt, lpid = lastProcessedId, ins = inserted, upd = updated, scn = scanned, st = status, err = error });
    }

    public async Task<int> MergeBatchAsync(string tableName, string primaryKey, IEnumerable<IDictionary<string, object?>> rows, CancellationToken ct)
    {
        // Simple generic MERGE via temp table
        var rowList = rows.ToList();
        if (rowList.Count == 0) return 0;
        await using var conn = CreateConnection();
        await conn.OpenAsync(ct);
        using var tx = await conn.BeginTransactionAsync(ct);

        var tempTableName = "#Staging" + Guid.NewGuid().ToString("N");
        // Build columns from first row
        var columns = rowList.First().Keys.ToList();
        var createCols = string.Join(",", columns.Select(c => $"[{c}] NVARCHAR(MAX) NULL")); // Simplified types; adjust if needed
        var createSql = $"CREATE TABLE {tempTableName} ({createCols});";
        await conn.ExecuteAsync(createSql, transaction: tx);

        // Bulk insert using individual inserts (for simplicity). For performance, replace with SqlBulkCopy.
        foreach (var r in rowList)
        {
            var colNames = string.Join(",", columns.Select(c => $"[{c}]"));
            var paramNames = string.Join(",", columns.Select(c => "@" + c));
            var insertSql = $"INSERT INTO {tempTableName} ({colNames}) VALUES ({paramNames});";
            await conn.ExecuteAsync(insertSql, r, tx);
        }

        var setCols = string.Join(",", columns.Where(c => c != primaryKey).Select(c => $"T.[{c}] = S.[{c}]"));
        var compareCols = string.Join(" OR ", columns.Where(c => c != primaryKey).Select(c => $"ISNULL(T.[{c}],'') <> ISNULL(S.[{c}],'')"));
        var mergeSql = $@"MERGE [{tableName}] AS T USING {tempTableName} AS S ON T.[{primaryKey}] = S.[{primaryKey}] WHEN MATCHED AND ({compareCols}) THEN UPDATE SET {setCols} WHEN NOT MATCHED BY TARGET THEN INSERT ({string.Join(",", columns.Select(c => $"[{c}]"))}) VALUES ({string.Join(",", columns.Select(c => $"S.[{c}]"))});";
        var affected = await conn.ExecuteAsync(mergeSql, transaction: tx, commandTimeout: _settings.CommandTimeoutSeconds);

        await tx.CommitAsync(ct);
        return affected;
    }
}
