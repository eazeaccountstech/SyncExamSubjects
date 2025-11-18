using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SyncExamSubjects.Models;
using SyncExamSubjects.Data;
using SyncExamSubjects.Services;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration.AddJsonFile("appsettings.json", optional: false, reloadOnChange: false);

builder.Services.AddSingleton(sp => sp.GetRequiredService<IConfiguration>().GetSection("SyncSettings").Get<SyncSettings>() ?? new SyncSettings());
builder.Services.AddSingleton<ISqlRepository, SqlRepository>();
builder.Services.AddSingleton<ISyncService, SyncService>();

builder.Services.AddLogging(cfg => cfg.AddConsole());

var host = builder.Build();

var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Startup");
var settings = host.Services.GetRequiredService<SyncSettings>();
logger.LogInformation("SyncExamSubjects job starting; DryRun={DryRun}, BatchSize={BatchSize}, Timeout={Timeout}", settings.DryRun, settings.BatchSize, settings.CommandTimeoutSeconds);

var svc = host.Services.GetRequiredService<ISyncService>();
await svc.RunAsync(CancellationToken.None);

logger.LogInformation("SyncExamSubjects job completed");
