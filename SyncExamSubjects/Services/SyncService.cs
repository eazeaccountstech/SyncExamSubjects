using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using SyncExamSubjects.Data;
using SyncExamSubjects.Models;
using Polly;
using Polly.Retry;

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

    public SyncService(ILogger<SyncService> logger, ISqlRepository repo, SyncSettings settings, IConfiguration config)
    {
        _logger = logger;
        _repo = repo;
        _settings = settings;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var retry = CreateRetryPolicy();

        foreach (var table in _settings.Tables)
        {
            int runLogId = 0;
            await retry.ExecuteAsync(async token =>
            {
                try
                {
                    runLogId = await _repo.StartRunLogAsync(table.Name, token);
                    var (lastRunAt, lastProcessedId) = await _repo.GetLastRunAsync(table.Name, token);
                    _logger.LogInformation("Starting sync for {Table} (since {Since}, lastId {LastId}) DryRun={DryRun}", table.Name, lastRunAt, lastProcessedId, _settings.DryRun);

                    var (inserted, updated, scanned, newLastRunAt, newLastProcessedId) = await _repo.ExecuteGenericSyncAsync(table, lastRunAt, lastProcessedId, _settings.DryRun, token);

                    var status = "Success";
                    if (_settings.DryRun)
                    {
                        status = "DryRun";
                    }

                    await _repo.UpdateRunLogCompleteAsync(runLogId, table.Name, newLastRunAt ?? DateTime.UtcNow, newLastProcessedId, inserted, updated, scanned, status, null, token);
                    _logger.LogInformation("Completed sync for {Table}. Scanned={Scanned}, Inserted={Inserted}, Updated={Updated}, Status={Status}", table.Name, scanned, inserted, updated, status);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Sync attempt failed for {Table}", table.Name);
                    if (runLogId != 0)
                    {
                        await _repo.UpdateRunLogCompleteAsync(runLogId, table.Name, DateTime.UtcNow, null, 0, 0, 0, "Failed", ex.Message, token);
                    }
                    throw;
                }
            }, ct);
        }
    }

    private AsyncRetryPolicy CreateRetryPolicy()
    {
        var attempts = Math.Max(1, _settings.Retry.MaxAttempts);
        var baseDelay = TimeSpan.FromSeconds(Math.Max(0, _settings.Retry.BaseDelaySeconds));
        var maxDelay = TimeSpan.FromSeconds(Math.Max(_settings.Retry.BaseDelaySeconds, _settings.Retry.MaxDelaySeconds));

        return Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(attempts, retryAttempt =>
            {
                var next = TimeSpan.FromSeconds(Math.Min(maxDelay.TotalSeconds, baseDelay.TotalSeconds * Math.Pow(2, retryAttempt - 1)));
                return next;
            }, (ex, delay, attempt, context) =>
            {
                _logger.LogWarning(ex, "Retry {Attempt} after {Delay}", attempt, delay);
            });
    }
}
