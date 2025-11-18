-- Schema setup for SERFIS4
SET XACT_ABORT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'S4Job_Run_Log' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.S4Job_Run_Log
    (
        run_log_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        table_name NVARCHAR(128) NOT NULL,
        last_run_at DATETIME2(3) NULL,
        last_processed_id BIGINT NULL,
        started_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        completed_at DATETIME2(3) NULL,
        records_scanned INT NULL,
        records_inserted INT NULL,
        records_updated INT NULL,
        status NVARCHAR(20) NOT NULL,
        error_message NVARCHAR(MAX) NULL
    );
    CREATE INDEX IX_S4Job_Run_Log_TableName ON dbo.S4Job_Run_Log(table_name, last_run_at DESC, last_processed_id DESC);
END
GO

IF OBJECT_ID('dbo.usp_S4_Sync_Generic') IS NOT NULL
    DROP PROCEDURE dbo.usp_S4_Sync_Generic;
GO

CREATE PROCEDURE dbo.usp_S4_Sync_Generic
    @TableName           sysname,           -- e.g. 'Entity_Roles' or 'dbo.Entity_Roles'
    @PrimaryKey          sysname,           -- e.g. 'ENRL_ID'
    @CreateDateColumn    sysname,           -- e.g. 'ENRL_CREATE_DTM'
    @ModifyDateColumn    sysname,           -- e.g. 'ENRL_MDFY_DTM'
    @LinkedServerName    sysname,           -- e.g. 'Link_S3DEV'
    @LastRunAt           DATETIME2(3) = NULL,
    @LastProcessedId     BIGINT = NULL,     -- tie breaker when timestamps are equal
    @BatchSize           INT = 1000,
    @DryRun              BIT = 0,
    @RecordsScanned      INT OUTPUT,
    @RecordsInserted     INT OUTPUT,
    @RecordsUpdated      INT OUTPUT,
    @NewLastRunAt        DATETIME2(3) OUTPUT,
    @NewLastProcessedId  BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @schema sysname, @obj sysname;
    IF CHARINDEX('.', @TableName) > 0
    BEGIN
        SET @schema = PARSENAME(@TableName, 2);
        SET @obj    = PARSENAME(@TableName, 1);
    END
    ELSE
    BEGIN
        SET @schema = N'dbo';
        SET @obj    = @TableName;
    END

    DECLARE @target NVARCHAR(400) = QUOTENAME(@schema) + N'.' + QUOTENAME(@obj);

    DECLARE @since DATETIME2(3) = ISNULL(@LastRunAt, DATEADD(DAY, -30, SYSUTCDATETIME()));
    DECLARE @sinceStr NVARCHAR(19) = CONVERT(NVARCHAR(19), @since, 120); -- yyyy-mm-dd hh:mi:ss

    -- Build column lists based on target table metadata
    DECLARE @cols TABLE(name sysname, isPk bit);
    INSERT INTO @cols(name, isPk)
    SELECT c.name, CASE WHEN c.name = @PrimaryKey THEN 1 ELSE 0 END
    FROM sys.columns c
    WHERE c.object_id = OBJECT_ID(@target)
      AND c.is_computed = 0
      AND c.generated_always_type = 0; -- exclude generated columns if any

    IF NOT EXISTS(SELECT 1 FROM @cols)
    BEGIN
        RAISERROR('No columns discovered for %s', 16, 1, @target);
        RETURN;
    END

    DECLARE @colList NVARCHAR(MAX) = (
        SELECT STRING_AGG(QUOTENAME(name), ',') FROM @cols
    );

    DECLARE @colListOracle NVARCHAR(MAX) = (
        SELECT STRING_AGG(QUOTENAME(name, '"'), ',') FROM @cols
    );

    DECLARE @setList NVARCHAR(MAX) = (
        SELECT STRING_AGG('T.' + QUOTENAME(name) + ' = S.' + QUOTENAME(name), ',')
        FROM @cols WHERE isPk = 0
    );

    DECLARE @insertValues NVARCHAR(MAX) = (
        SELECT STRING_AGG('S.' + QUOTENAME(name), ',') FROM @cols
    );

    -- Create #stage with same shape/types as target table
    DECLARE @createStage NVARCHAR(MAX) = N'SELECT TOP (0) ' + @colList + N' INTO #stage FROM ' + @target + N';';
    EXEC sp_executesql @createStage;

    -- Build Oracle-side filter; mind single-quote escaping for OPENQUERY
    DECLARE @filter NVARCHAR(MAX) =
        N'(' + @CreateDateColumn + N' > TO_TIMESTAMP(''' + REPLACE(@sinceStr, '''', '''''') + N''',''YYYY-MM-DD HH24:MI:SS'')'
        + N' OR ' + @ModifyDateColumn + N' > TO_TIMESTAMP(''' + REPLACE(@sinceStr, '''', '''''') + N''',''YYYY-MM-DD HH24:MI:SS'')'
        + N' OR (' + @CreateDateColumn + N' = TO_TIMESTAMP(''' + REPLACE(@sinceStr, '''', '''''') + N''',''YYYY-MM-DD HH24:MI:SS'') AND ' + @PrimaryKey + N' > ' + COALESCE(CONVERT(NVARCHAR(30), @LastProcessedId), N'0') + N') )';

    DECLARE @innerBase NVARCHAR(MAX) = N'SELECT ' + @colListOracle + N' FROM ' + @obj + N' WHERE ' + @filter + N' ORDER BY ' + @CreateDateColumn + N',' + @PrimaryKey;
    DECLARE @innerLimited NVARCHAR(MAX) = N'SELECT * FROM (' + @innerBase + N') WHERE ROWNUM <= ' + CAST(@BatchSize AS NVARCHAR(12));

    DECLARE @openquery NVARCHAR(MAX) = N'SELECT ' + @colList + N' FROM OPENQUERY(' + QUOTENAME(@LinkedServerName) + N', ''' + REPLACE(@innerLimited, '''', '''''') + N''')';

    DECLARE @insertStage NVARCHAR(MAX) = N'INSERT INTO #stage (' + @colList + N') ' + @openquery + N';';
    EXEC sp_executesql @insertStage;

    -- Metrics: scanned count
    SELECT @RecordsScanned = COUNT(*) FROM #stage;

    -- Compute new last run and last processed id from staged rows
    DECLARE @sql NVARCHAR(MAX);
    SET @sql = N'SELECT @outRun = MAX(COALESCE(CONVERT(datetime2(3), S.' + QUOTENAME(@ModifyDateColumn) + N'), CONVERT(datetime2(3), S.' + QUOTENAME(@CreateDateColumn) + N'))) FROM #stage AS S;';
    EXEC sp_executesql @sql, N'@outRun datetime2(3) OUTPUT', @outRun = @NewLastRunAt OUTPUT;

    SET @sql = N'SELECT @outId = MAX(CAST(S.' + QUOTENAME(@PrimaryKey) + N' AS BIGINT)) FROM #stage AS S;';
    EXEC sp_executesql @sql, N'@outId bigint OUTPUT', @outId = @NewLastProcessedId OUTPUT;

    IF (@DryRun = 1)
    BEGIN
        -- In dry-run mode, do not merge; just return metrics
        RETURN;
    END

    -- MERGE into target and capture actions
    DECLARE @out TABLE(action NVARCHAR(10));
    DECLARE @merge NVARCHAR(MAX) = N'MERGE ' + @target + N' AS T USING #stage AS S ON T.' + QUOTENAME(@PrimaryKey) + N' = S.' + QUOTENAME(@PrimaryKey) +
        N' WHEN MATCHED THEN UPDATE SET ' + @setList +
        N' WHEN NOT MATCHED BY TARGET THEN INSERT (' + @colList + N') VALUES (' + @insertValues + N') OUTPUT $action INTO @out;';

    EXEC sp_executesql @merge, N'@out TABLE (action NVARCHAR(10)) READONLY', @out=@out; -- pass table variable context

    SELECT @RecordsInserted = ISNULL(SUM(CASE WHEN action = 'INSERT' THEN 1 ELSE 0 END), 0),
           @RecordsUpdated = ISNULL(SUM(CASE WHEN action = 'UPDATE' THEN 1 ELSE 0 END), 0)
    FROM @out;

    SET @NewLastRunAt = ISNULL(@NewLastRunAt, SYSUTCDATETIME());
END
GO
