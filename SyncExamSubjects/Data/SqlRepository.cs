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
    Task<(int inserted, int updated, int scanned, DateTime? newLastRunAt, long? newLastProcessedId)> ExecuteGenericSyncAsync(TableSyncConfig table, DateTime? lastRunAt, long? lastProcessedId, bool dryRun, CancellationToken ct);
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
        const string sql = "SELECT TOP 1 last_run_at, last_processed_id FROM S4Job_Run_Log WHERE table_name=@table AND status='Success' ORDER BY last_run_at DESC, last_processed_id DESC";
        await using var conn = CreateConnection();
        var result = await conn.QueryFirstOrDefaultAsync(sql, new { table = tableName });
        if (result == null) return (null, null);
        DateTime? lastRunAt = result.last_run_at == null ? null : (DateTime)result.last_run_at;
        long? lastProcessedId = result.last_processed_id == null ? null : (long)result.last_processed_id;
        return (lastRunAt, lastProcessedId);
    }

    public async Task<int> StartRunLogAsync(string tableName, CancellationToken ct)
    {
        const string sql = @"INSERT INTO S4Job_Run_Log(table_name, started_at, status) VALUES(@t, SYSUTCDATETIME(), 'Running'); SELECT CAST(SCOPE_IDENTITY() AS int);";
        await using var conn = CreateConnection();
        var id = await conn.ExecuteScalarAsync<int>(sql, new { t = tableName });
        return id;
    }

    public async Task UpdateRunLogCompleteAsync(int runLogId, string tableName, DateTime lastRunAt, long? lastProcessedId, int inserted, int updated, int scanned, string status, string? error, CancellationToken ct)
    {
        const string sql = @"UPDATE S4Job_Run_Log SET completed_at = SYSUTCDATETIME(), last_run_at=@lrun, last_processed_id=@lpid, records_inserted=@ins, records_updated=@upd, records_scanned=@scn, status=@st, error_message=@err WHERE run_log_id=@id";
        await using var conn = CreateConnection();
        await conn.ExecuteAsync(sql, new { id = runLogId, lrun = lastRunAt, lpid = lastProcessedId, ins = inserted, upd = updated, scn = scanned, st = status, err = error });
    }

    public async Task<(int inserted, int updated, int scanned, DateTime? newLastRunAt, long? newLastProcessedId)> ExecuteGenericSyncAsync(TableSyncConfig table, DateTime? lastRunAt, long? lastProcessedId, bool dryRun, CancellationToken ct)
    {
        await using var conn = CreateConnection();
        var p = new DynamicParameters();
        p.Add("@TableName", table.Name);
        p.Add("@PrimaryKey", table.PrimaryKey);
        p.Add("@CreateDateColumn", table.CreateDateColumn);
        p.Add("@ModifyDateColumn", table.ModifyDateColumn);
        p.Add("@LinkedServerName", _settings.LinkedServerName);
        p.Add("@LastRunAt", lastRunAt);
        p.Add("@LastProcessedId", lastProcessedId);
        p.Add("@BatchSize", _settings.BatchSize);
        p.Add("@DryRun", dryRun);
        p.Add("@RecordsScanned", dbType: DbType.Int32, direction: ParameterDirection.Output);
        p.Add("@RecordsInserted", dbType: DbType.Int32, direction: ParameterDirection.Output);
        p.Add("@RecordsUpdated", dbType: DbType.Int32, direction: ParameterDirection.Output);
        p.Add("@NewLastRunAt", dbType: DbType.DateTime2, direction: ParameterDirection.Output);
        p.Add("@NewLastProcessedId", dbType: DbType.Int64, direction: ParameterDirection.Output);

        await conn.ExecuteAsync("dbo.usp_S4_Sync_Generic", p, commandType: CommandType.StoredProcedure, commandTimeout: _settings.CommandTimeoutSeconds);

        var scanned = p.Get<int>("@RecordsScanned");
        var inserted = p.Get<int>("@RecordsInserted");
        var updated = p.Get<int>("@RecordsUpdated");
        var newLastRunAt = p.Get<DateTime?>("@NewLastRunAt");
        var newLastProcessedId = p.Get<long?>("@NewLastProcessedId");
        return (inserted, updated, scanned, newLastRunAt, newLastProcessedId);
    }
}
