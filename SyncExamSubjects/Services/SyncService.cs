using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using SyncExamSubjects.Data;
using SyncExamSubjects.Models;
using Dapper;
using Microsoft.Data.SqlClient;

namespace SyncExamSubjects.Services;

public interface ISyncService
{
    Task RunAsync(CancellationToken ct);
}

public sealed class SyncService : ISyncService
{
    private readonly ILogger<SyncService> _logger;
    private readonly ISqlRepository _repo;
    private readonly SyncSettings _settings;
    private readonly IConfiguration _config;

    public SyncService(ILogger<SyncService> logger, ISqlRepository repo, SyncSettings settings, IConfiguration config)
    {
        _logger = logger;
        _repo = repo;
        _settings = settings;
        _config = config;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        foreach (var table in _settings.Tables)
        {
            int runLogId = 0;
            try
            {
                runLogId = await _repo.StartRunLogAsync(table.Name, ct);
                var (lastRunAt, lastProcessedId) = await _repo.GetLastRunAsync(table.Name, ct);
                var since = lastRunAt ?? DateTime.UtcNow.AddDays(-30); // fallback window

                _logger.LogInformation("Starting sync for {Table} since {Since}", table.Name, since);

                var rows = await FetchChangedRowsAsync(table, since, ct);
                var scanned = rows.Count;

                // MERGE batch
                var affected = await _repo.MergeBatchAsync(table.Name, table.PrimaryKey, rows.Select(r => (IDictionary<string, object?>)r), ct);

                // For simplicity, treat affected as inserted+updated unknown split.
                await _repo.UpdateRunLogCompleteAsync(runLogId, table.Name, DateTime.UtcNow, rows.Count > 0 ? rows.Max(r => Convert.ToInt64(r[table.PrimaryKey]!)) : lastProcessedId, affected, 0, scanned, "Success", null, ct);
                _logger.LogInformation("Completed sync for {Table}. Rows scanned={Scanned} affected={Affected}", table.Name, scanned, affected);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Sync failed for {Table}", table.Name);
                if (runLogId != 0)
                {
                    await _repo.UpdateRunLogCompleteAsync(runLogId, table.Name, DateTime.UtcNow, null, 0, 0, 0, "Failed", ex.Message, ct);
                }
            }
        }
    }

    private async Task<List<Dictionary<string, object?>>> FetchChangedRowsAsync(TableSyncConfig table, DateTime sinceUtc, CancellationToken ct)
    {
        // Build OPENQUERY; assumes Oracle side date comparison using TO_TIMESTAMP; adapt as needed.
        var linked = _settings.LinkedServerName;
        var createCol = table.CreateDateColumn;
        var modifyCol = table.ModifyDateColumn;
        var pk = table.PrimaryKey;
        var sinceStr = sinceUtc.ToString("yyyy-MM-dd HH:mm:ss");
        var inner = $"SELECT * FROM {table.Name} WHERE {createCol} > TO_TIMESTAMP('{sinceStr}','YYYY-MM-DD HH24:MI:SS') OR {modifyCol} > TO_TIMESTAMP('{sinceStr}','YYYY-MM-DD HH24:MI:SS')";
        var sql = $"SELECT * FROM OPENQUERY([{linked}], '{inner.Replace("'", "''")}')"; // escape single quotes for OPENQUERY

        using var conn = new SqlConnection(_config.GetConnectionString("SqlServer"));
        var rows = new List<Dictionary<string, object?>>();
        var reader = await conn.ExecuteReaderAsync(sql);
        while (await reader.ReadAsync(ct))
        {
            var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var val = await reader.IsDBNullAsync(i, ct) ? null : reader.GetValue(i);
                dict[reader.GetName(i)] = val;
            }
            rows.Add(dict);
        }
        return rows;
    }
}
