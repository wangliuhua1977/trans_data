package com.transdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskTransferJob implements TaskScheduler.TaskExecutable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskTransferJob.class);
    private static final DateTimeFormatter LOG_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final TaskDefinition task;
    private final AppConfig config;
    private final SourceClient sourceClient;
    private final AsyncSqlClient asyncSqlClient;
    private final TaskSqlBuilder sqlBuilder;
    private final UiLogSink uiLog;
    private final ProgressListener progressListener;
    private final AtomicBoolean cancelled;

    public TaskTransferJob(TaskDefinition task,
                           AppConfig config,
                           SourceClient sourceClient,
                           AsyncSqlClient asyncSqlClient,
                           TaskSqlBuilder sqlBuilder,
                           UiLogSink uiLog,
                           ProgressListener progressListener,
                           AtomicBoolean cancelled) {
        this.task = task;
        this.config = config;
        this.sourceClient = sourceClient;
        this.asyncSqlClient = asyncSqlClient;
        this.sqlBuilder = sqlBuilder;
        this.uiLog = uiLog;
        this.progressListener = progressListener;
        this.cancelled = cancelled;
    }

    @Override
    public TaskRunResult run() {
        Instant start = Instant.now();
        String startTimeText = LocalDateTime.now().format(LOG_TIME_FORMATTER);
        String runId = UUID.randomUUID().toString();
        String requestId = UUID.randomUUID().toString();
        String lockName = "trans_data_task:" + task.getTaskId();
        String ownerId = buildOwnerId(runId);
        TransferStats stats = new TransferStats();
        boolean lockHeld = false;
        boolean startLogged = false;
        boolean lockResult = false;
        int totalExpected = 0;
        int insertedRows = 0;
        int retryCount = 0;
        String auditSummary = "未执行";
        TaskRunResult finalStatus = TaskRunResult.FAILED;
        LeaseState leaseState = new LeaseState(Instant.EPOCH);

        try {
            if (!validateCrypto()) {
                updateStage("失败");
                return TaskRunResult.FAILED;
            }

            updateStage("锁定中");
            executeStatement(sqlBuilder.buildCreateLockTableSql(), "创建锁表", runId, requestId, stats,
                    1, 1, false);
            lockHeld = acquireLock(lockName, ownerId, requestId, stats, runId);
            lockResult = lockHeld;
            if (!lockHeld) {
                updateStage("已跳过");
                updateProgress(100, "因锁占用跳过");
                logStartSeparator(runId, startTimeText, 0, 0, lockResult);
                startLogged = true;
                finalStatus = TaskRunResult.SKIPPED_LOCK;
                return finalStatus;
            }
            leaseState.renewedAt = Instant.now();

            updateStage("获取中");
            uiLog.log("INFO", "开始拉取源数据。运行ID=" + runId + "，请求ID=" + requestId);
            SourceFetchResult fetchResult = sourceClient.fetchJson(task, config, uiLog);
            List<com.fasterxml.jackson.databind.JsonNode> records = fetchResult.getRecords();
            totalExpected = records.size();
            stats.setTotalRecords(totalExpected);
            progressListener.updateStats(stats);
            logStartSeparator(runId, startTimeText, totalExpected, task.getBatchSize(), lockResult);
            startLogged = true;

            if (records.isEmpty()) {
                uiLog.log("INFO", "本次拉取记录数为 0，无需写入。运行ID=" + runId + "，请求ID=" + requestId);
                updateStage("完成");
                finalStatus = TaskRunResult.SUCCESS;
                auditSummary = "无数据";
                return finalStatus;
            }
            if (cancelled.get()) {
                uiLog.log("WARN", "拉取后任务被取消。运行ID=" + runId + "，请求ID=" + requestId);
                updateStage("已取消");
                return TaskRunResult.FAILED;
            }

            updateStage("准备中");
            executeStatement(sqlBuilder.buildCreateStagingTableSql(), "创建临时表", runId, requestId, stats,
                    1, 1, false);
            executeStatement(sqlBuilder.buildCreateStagingIndexSql(), "创建临时表索引", runId, requestId, stats,
                    1, 1, false);

            int maxRetries = Math.max(1, task.getSchedulePolicy().getMaxRetries());
            boolean success = false;
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                retryCount = Math.max(0, attempt - 1);
                if (cancelled.get()) {
                    throw new InterruptedException("任务在第 " + attempt + " 次尝试前被取消");
                }
                uiLog.log("INFO", "开始第 " + attempt + "/" + maxRetries + " 次尝试。运行ID=" + runId
                        + "，请求ID=" + requestId);
                try {
                    AuditResult attemptResult = runAttempt(records, runId, requestId, stats, lockName, ownerId,
                            leaseState);
                    success = attemptResult.success();
                    auditSummary = attemptResult.message();
                } catch (Exception ex) {
                    uiLog.log("ERROR", "第 " + attempt + " 次尝试失败：" + ex.getMessage() + "，运行ID=" + runId
                            + "，请求ID=" + requestId);
                    LOGGER.error("第 {} 次尝试失败", attempt, ex);
                    cleanupScope(runId, requestId, stats, "尝试失败");
                    success = false;
                    auditSummary = "异常：" + ex.getMessage();
                }
                if (success) {
                    break;
                }
                if (attempt < maxRetries) {
                    uiLog.log("WARN", "稽核失败，清理后重试。尝试=" + attempt + "，运行ID=" + runId
                            + "，请求ID=" + requestId);
                }
            }

            if (!success) {
                uiLog.log("ERROR", "任务在达到最大重试次数后失败，已完成清理。运行ID=" + runId
                        + "，请求ID=" + requestId);
                updateStage("失败");
                finalStatus = TaskRunResult.FAILED;
                return finalStatus;
            }

            updateStage("完成");
            Duration duration = Duration.between(start, Instant.now());
            uiLog.log("INFO", "任务成功完成，耗时 " + duration.toMillis() + " ms，运行ID=" + runId
                    + "，请求ID=" + requestId);
            finalStatus = TaskRunResult.SUCCESS;
            insertedRows = totalExpected;
            return finalStatus;
        } catch (InterruptedException ex) {
            updateStage("已取消");
            uiLog.log("WARN", "任务取消：" + ex.getMessage() + "，运行ID=" + runId + "，请求ID=" + requestId);
            finalStatus = TaskRunResult.FAILED;
            return finalStatus;
        } catch (Exception ex) {
            updateStage("失败");
            uiLog.log("ERROR", "任务失败：" + ex.getMessage() + "，运行ID=" + runId + "，请求ID=" + requestId);
            LOGGER.error("任务失败", ex);
            finalStatus = TaskRunResult.FAILED;
            return finalStatus;
        } finally {
            if (lockHeld) {
                releaseLock(lockName, ownerId, runId, requestId, stats);
            }
            if (!startLogged) {
                logStartSeparator(runId, startTimeText, totalExpected, task.getBatchSize(), lockResult);
            }
            String endTimeText = LocalDateTime.now().format(LOG_TIME_FORMATTER);
            Duration duration = Duration.between(start, Instant.now());
            logEndSeparator(runId, endTimeText, duration, finalStatus, insertedRows, auditSummary, retryCount);
        }
    }

    private AuditResult runAttempt(List<com.fasterxml.jackson.databind.JsonNode> records,
                                   String runId,
                                   String requestId,
                                   TransferStats stats,
                                   String lockName,
                                   String ownerId,
                                   LeaseState leaseState) throws Exception {
        updateStage("分段写入中");
        leaseState.renewedAt = maybeRenewLease(lockName, ownerId, leaseState.renewedAt, runId, requestId, stats);
        executeStatement(sqlBuilder.buildClearStagingSql(task.getTaskId(), runId), "清理临时表", runId, requestId,
                stats, 1, 1, false);
        int batchSize = Math.max(1, task.getBatchSize());
        List<String> segments = sqlBuilder.buildInsertStagingSqlSegments(records, task.getTaskId(), runId, batchSize);
        int totalSegments = segments.size();
        stats.setTotalBatches(totalSegments);
        stats.setCurrentBatch(0);
        progressListener.updateStats(stats);

        for (int i = 0; i < segments.size(); i++) {
            if (cancelled.get()) {
                throw new InterruptedException("分段写入提交前任务已取消");
            }
            int segmentIndex = i + 1;
            stats.setCurrentBatch(segmentIndex);
            progressListener.updateStats(stats);
            int segmentRows = Math.min(batchSize, records.size() - (segmentIndex - 1) * batchSize);
            uiLog.log("INFO", "分段写入 " + segmentIndex + "/" + totalSegments
                    + "，行数=" + segmentRows
                    + "，运行ID=" + runId
                    + "，请求ID=" + requestId);
            leaseState.renewedAt = maybeRenewLease(lockName, ownerId, leaseState.renewedAt, runId, requestId, stats);
            executeStatement(segments.get(i), "分段写入_" + segmentIndex, runId, requestId, stats,
                    segmentRows, totalSegments, false);
            updateProgress(segmentIndex, totalSegments);
        }

        updateStage("应用中");
        leaseState.renewedAt = maybeRenewLease(lockName, ownerId, leaseState.renewedAt, runId, requestId, stats);
        String createTableSql = task.getTargetConfig().getCreateTableSql();
        if (createTableSql != null && !createTableSql.isBlank()) {
            executeStatement(createTableSql, "创建目标表", runId, requestId, stats, 1, 1, true);
        }
        executeStatement(sqlBuilder.buildApplyToTargetSql(task, runId), "应用到目标表", runId, requestId, stats,
                records.size(), 1, false);

        updateStage("稽核中");
        leaseState.renewedAt = maybeRenewLease(lockName, ownerId, leaseState.renewedAt, runId, requestId, stats);
        AuditResult auditResult = auditTarget(records.size(), runId, requestId, stats);
        if (!auditResult.success()) {
            uiLog.log("WARN", "稽核不一致：" + auditResult.message() + "，运行ID=" + runId
                    + "，请求ID=" + requestId);
            cleanupScope(runId, requestId, stats, "稽核失败");
            return auditResult;
        }

        executeStatement(sqlBuilder.buildClearStagingSql(task.getTaskId(), runId), "清理临时表_成功", runId, requestId,
                stats, 1, 1, false);
        uiLog.log("INFO", "稽核通过。运行ID=" + runId + "，请求ID=" + requestId);
        return auditResult;
    }

    private AuditResult auditTarget(int expectedTotal,
                                    String runId,
                                    String requestId,
                                    TransferStats stats) throws Exception {
        AsyncSqlClient.ResultResponse totalResult = executeQuery(sqlBuilder.buildAuditTotalSql(task, runId),
                "汇总稽核", runId, requestId, stats, 1, 1);
        int totalActual = parseSingleCount(totalResult, "汇总稽核", requestId);

        AsyncSqlClient.ResultResponse expectedDistinctResult = executeQuery(
                sqlBuilder.buildExpectedDistinctSql(task, runId), "预期去重数", runId, requestId, stats, 1, 1);
        int expectedDistinct = parseSingleCount(expectedDistinctResult, "预期去重数", requestId);

        AsyncSqlClient.ResultResponse actualDistinctResult = executeQuery(sqlBuilder.buildAuditDistinctSql(task, runId),
                "目标去重数", runId, requestId, stats, 1, 1);
        int actualDistinct = parseSingleCount(actualDistinctResult, "目标去重数", requestId);

        StringBuilder diffs = new StringBuilder();
        if (totalActual != expectedTotal) {
            diffs.append("总数预期=").append(expectedTotal).append(" 总数实际=").append(totalActual);
        }
        if (expectedDistinct != actualDistinct) {
            if (!diffs.isEmpty()) {
                diffs.append("; ");
            }
            diffs.append("去重预期=").append(expectedDistinct).append(" 去重实际=").append(actualDistinct);
        }
        if (!diffs.isEmpty()) {
            String message = diffs.toString();
            uiLog.log("WARN", "稽核差异：" + message + "，运行ID=" + runId + "，请求ID=" + requestId);
            return new AuditResult(false, message);
        }
        return new AuditResult(true, "通过");
    }

    private int parseSingleCount(AsyncSqlClient.ResultResponse result, String context, String requestId) {
        List<List<String>> rows = result.getRows();
        if (rows == null || rows.isEmpty() || rows.get(0).isEmpty()) {
            throw new IllegalStateException(context + " 结果解析失败。原始JSON=" + safeString(result.getRawJson()));
        }
        return parseInt(rows.get(0).get(0));
    }

    private boolean validateCrypto() {
        if (config.getAesKey().isBlank() || config.getAesIv().isBlank()) {
            uiLog.log("ERROR", "AES Key/IV 缺失，请配置 crypto.aesKey 与 crypto.aesIv。");
            return false;
        }
        return true;
    }

    private boolean acquireLock(String lockName, String ownerId, String requestId, TransferStats stats, String runId)
            throws Exception {
        int leaseSeconds = Math.max(30, task.getSchedulePolicy().getLeaseSeconds());
        SqlExecution execution = executeStatement(sqlBuilder.buildLockAcquireSql(lockName, ownerId, leaseSeconds),
                "获取锁", runId, requestId, stats, 1, 1, false);
        Integer rowsAffected = resolveRowsAffected(execution, requestId, "获取锁");
        if (rowsAffected == null) {
            throw new IllegalStateException("无法确定锁获取结果。");
        }
        if (rowsAffected == 0) {
            uiLog.log("INFO", "已有其他实例在运行，跳过本次执行。锁名称=" + lockName + "，运行ID=" + runId
                    + "，请求ID=" + requestId);
            return false;
        }
        uiLog.log("INFO", "锁获取成功。锁名称=" + lockName + "，持有者=" + ownerId + "，运行ID=" + runId
                + "，请求ID=" + requestId);
        return true;
    }

    private Instant maybeRenewLease(String lockName,
                                   String ownerId,
                                   Instant lastRenewedAt,
                                   String runId,
                                   String requestId,
                                   TransferStats stats) throws Exception {
        int leaseSeconds = Math.max(30, task.getSchedulePolicy().getLeaseSeconds());
        Instant now = Instant.now();
        if (Duration.between(lastRenewedAt, now).toSeconds() < leaseSeconds / 2L) {
            return lastRenewedAt;
        }
        SqlExecution execution = executeStatement(sqlBuilder.buildLockAcquireSql(lockName, ownerId, leaseSeconds),
                "续期锁", runId, requestId, stats, 1, 1, false);
        Integer rowsAffected = resolveRowsAffected(execution, requestId, "续期锁");
        if (rowsAffected == null || rowsAffected == 0) {
            throw new IllegalStateException("锁租约续期失败，可能已被其他实例接管。");
        }
        uiLog.log("INFO", "锁续期成功。锁名称=" + lockName + "，持有者=" + ownerId + "，运行ID=" + runId
                + "，请求ID=" + requestId);
        return now;
    }

    private void releaseLock(String lockName, String ownerId, String runId, String requestId, TransferStats stats) {
        try {
            executeStatement(sqlBuilder.buildLockReleaseSql(lockName, ownerId), "释放锁", runId, requestId, stats,
                    1, 1, false);
            uiLog.log("INFO", "锁已释放。锁名称=" + lockName + "，持有者=" + ownerId + "，运行ID=" + runId
                    + "，请求ID=" + requestId);
        } catch (Exception ex) {
            uiLog.log("WARN", "释放锁失败：" + ex.getMessage() + "，锁名称=" + lockName
                    + "，运行ID=" + runId + "，请求ID=" + requestId);
            LOGGER.warn("释放锁失败", ex);
        }
    }

    private void cleanupScope(String runId, String requestId, TransferStats stats, String reason) {
        try {
            updateStage("清理中");
            uiLog.log("INFO", "开始清理范围：原因=" + reason + "，运行ID=" + runId + "，请求ID=" + requestId);
            executeStatement(sqlBuilder.buildDeleteTargetByScopeSql(task, runId), "清理目标表", runId, requestId,
                    stats, 1, 1, false);
            executeStatement(sqlBuilder.buildClearStagingSql(task.getTaskId(), runId), "清理临时表", runId, requestId,
                    stats, 1, 1, false);
        } catch (Exception ex) {
            uiLog.log("ERROR", "清理失败：" + ex.getMessage() + "，运行ID=" + runId
                    + "，请求ID=" + requestId);
            LOGGER.error("清理失败", ex);
        }
    }

    private SqlExecution executeStatement(String sql,
                                          String context,
                                          String runId,
                                          String requestId,
                                          TransferStats stats,
                                          int records,
                                          int totalSegments,
                                          boolean allowAlreadyExists) throws Exception {
        return executeSql(sql, context, runId, requestId, stats, records, totalSegments, false, allowAlreadyExists);
    }

    private AsyncSqlClient.ResultResponse executeQuery(String sql,
                                                      String context,
                                                      String runId,
                                                      String requestId,
                                                      TransferStats stats,
                                                      int records,
                                                      int totalSegments) throws Exception {
        return executeSql(sql, context, runId, requestId, stats, records, totalSegments, true, false).result;
    }

    private SqlExecution executeSql(String sql,
                                    String context,
                                    String runId,
                                    String requestId,
                                    TransferStats stats,
                                    int records,
                                    int totalSegments,
                                    boolean fetchResult,
                                    boolean allowAlreadyExists) throws Exception {
        if (cancelled.get()) {
            throw new InterruptedException("SQL 执行前任务已取消：" + context);
        }
        updateStage("加密中");
        logPlainSql(sql, runId, requestId, context, records, totalSegments);
        String encryptedSql = encryptSql(sql);

        updateStage("提交中");
        AsyncSqlClient.SubmitResponse submitResponse = asyncSqlClient.submit(config, encryptedSql, uiLog, requestId);
        String jobId = submitResponse.getJobId();
        stats.setJobId(jobId);
        progressListener.updateStats(stats);
        uiLog.log("INFO", "已提交 SQL：上下文=" + context + "，作业ID=" + jobId + "，状态="
                + submitResponse.getStatus() + "，运行ID=" + runId + "，请求ID=" + requestId);

        updateStage("轮询中");
        AsyncSqlClient.StatusResponse status = asyncSqlClient.pollStatus(
                config, jobId, uiLog, progressListener, cancelled, requestId);
        if (handleTerminalStatus(status, jobId, requestId, context, allowAlreadyExists)) {
            throw new IllegalStateException(context + " 执行失败，状态=" + status.getStatus());
        }

        AsyncSqlClient.ResultResponse result = null;
        if (fetchResult) {
            result = asyncSqlClient.result(config, jobId, uiLog, requestId);
        }
        logSuccessSummary(status, result, jobId, requestId, context, runId);
        return new SqlExecution(jobId, status, result);
    }

    private Integer resolveRowsAffected(SqlExecution execution, String requestId, String context) {
        if (execution == null) {
            return null;
        }
        AsyncSqlClient.StatusResponse status = execution.status;
        Integer count = firstNonNull(status.getRowsAffected(), status.getUpdatedRows(), status.getAffectedRows(),
                status.getActualRows());
        if (count != null) {
            return count;
        }
        AsyncSqlClient.ResultResponse result = execution.result;
        if (result != null) {
            return firstNonNull(result.getRowsAffected(), result.getActualRows());
        }
        try {
            AsyncSqlClient.ResultResponse fetched = asyncSqlClient.result(config, execution.jobId, uiLog, requestId);
            return firstNonNull(fetched.getRowsAffected(), fetched.getActualRows());
        } catch (Exception ex) {
            uiLog.log("WARN", "无法获取影响行数：" + context + "，原因=" + ex.getMessage()
                    + "，请求ID=" + requestId);
            return null;
        }
    }

    private boolean handleTerminalStatus(AsyncSqlClient.StatusResponse status,
                                         String jobId,
                                         String requestId,
                                         String context,
                                         boolean allowAlreadyExists) {
        if ("CANCELLED_LOCAL".equalsIgnoreCase(status.getStatus())) {
            uiLog.log("WARN", context + " 轮询中被取消。作业ID=" + jobId + "，请求ID=" + requestId
                    + "，原因=" + status.getErrorMessage());
            updateStage("已取消");
            return true;
        }
        if ("TIMEOUT".equalsIgnoreCase(status.getStatus())) {
            uiLog.log("ERROR", context + " 轮询超时。作业ID=" + jobId
                    + "，错误信息=" + status.getErrorMessage()
                    + "，请求ID=" + requestId);
            updateStage("失败");
            return true;
        }
        if (!"SUCCEEDED".equalsIgnoreCase(status.getStatus())) {
            String errorMessage = safeString(status.getErrorMessage());
            if (allowAlreadyExists && errorMessage.toLowerCase().contains("already exists")) {
                uiLog.log("INFO", "目标表已存在，继续执行。作业ID=" + jobId + "，请求ID=" + requestId);
                return false;
            }
            uiLog.log("ERROR", context + " 执行失败。作业ID=" + jobId
                    + "，状态=" + status.getStatus()
                    + "，错误信息=" + errorMessage
                    + "，错误位置=" + safeString(status.getErrorPosition())
                    + "，SQLState=" + safeString(status.getSqlState())
                    + "，TraceId=" + safeString(status.getTraceId())
                    + "，请求ID=" + requestId);
            updateStage("失败");
            return true;
        }
        return false;
    }

    private void logSuccessSummary(AsyncSqlClient.StatusResponse status,
                                   AsyncSqlClient.ResultResponse result,
                                   String jobId,
                                   String requestId,
                                   String context,
                                   String runId) {
        boolean hasRowsSummary = status.getRowsAffected() != null
                || status.getUpdatedRows() != null
                || status.getAffectedRows() != null
                || status.getActualRows() != null;
        if (!hasRowsSummary && result != null) {
            uiLog.log("INFO", "作业成功。上下文=" + context + "，作业ID=" + jobId
                    + "，影响行数=" + result.getRowsAffected()
                    + "，实际行数=" + result.getActualRows()
                    + "，列数=" + result.getColumnsCount()
                    + "，运行ID=" + runId
                    + "，请求ID=" + requestId);
        } else {
            uiLog.log("INFO", "作业成功。上下文=" + context + "，作业ID=" + jobId
                    + "，影响行数=" + status.getRowsAffected()
                    + "，更新行数=" + status.getUpdatedRows()
                    + "，受影响行数=" + status.getAffectedRows()
                    + "，实际行数=" + status.getActualRows()
                    + "，耗时=" + status.getElapsedMillis()
                    + "，运行ID=" + runId
                    + "，请求ID=" + requestId);
        }
    }

    private String encryptSql(String sql) throws Exception {
        byte[] keyBytes = CryptoUtil.decodeKey(config.getAesKey(), config.getKeyFormat());
        byte[] ivBytes = CryptoUtil.decodeKey(config.getAesIv(), config.getKeyFormat());
        return CryptoUtil.encrypt(sql, keyBytes, ivBytes);
    }

    private void logPlainSql(String sql,
                             String runId,
                             String requestId,
                             String contextTag,
                             int records,
                             int totalSegments) {
        uiLog.log("INFO", "SQL 已准备提交。运行ID=" + runId
                + "，请求ID=" + requestId
                + "，上下文=" + contextTag
                + "，记录数=" + records
                + "，分段数=" + totalSegments
                + "，数据库用户=" + config.getAsyncDbUser()
                + "，目标表=" + task.getTargetConfig().getTargetTable());
        int maxChars = config.getLoggingSqlMaxChars();
        if (sql.length() > maxChars) {
            String truncated = JsonUtil.safeTruncate(sql, maxChars);
            uiLog.log("INFO", "SQL 预览（已截断）：" + truncated);
        } else {
            uiLog.log("INFO", "SQL 内容：" + sql);
        }
    }

    private void updateStage(String stage) {
        progressListener.updateStage(stage);
    }

    private void updateProgress(int currentSegment, int totalSegments) {
        int percent = totalSegments == 0 ? 100 : (int) Math.round((currentSegment * 100.0) / totalSegments);
        progressListener.updateProgress(percent, "分段 " + currentSegment + "/" + totalSegments);
    }

    private void updateProgress(int percent, String message) {
        progressListener.updateProgress(percent, message);
    }

    private void logStartSeparator(String runId,
                                   String startTime,
                                   int recordCount,
                                   int batchSize,
                                   boolean lockAcquired) {
        String title = "====================【TASK START】====================";
        String detail = "任务名称=" + task.displayName()
                + "，taskId=" + task.getTaskId()
                + "，runId=" + runId
                + "，开始时间=" + startTime
                + "，记录数=" + recordCount
                + "，批大小=" + batchSize
                + "，插入模式=" + task.getTargetConfig().getInsertMode()
                + "，锁获取=" + (lockAcquired ? "成功" : "失败")
                + "，调度=" + scheduleSummary();
        logSeparatorBlock(title, detail);
    }

    private void logEndSeparator(String runId,
                                 String endTime,
                                 Duration duration,
                                 TaskRunResult status,
                                 int insertedRows,
                                 String auditSummary,
                                 int retryCount) {
        String title = "====================【TASK END】====================";
        String detail = "任务名称=" + task.displayName()
                + "，taskId=" + task.getTaskId()
                + "，runId=" + runId
                + "，结束时间=" + endTime
                + "，耗时=" + duration.toMillis() + " ms"
                + "，最终状态=" + status
                + "，插入行数=" + insertedRows
                + "，稽核摘要=" + (auditSummary == null || auditSummary.isBlank() ? "未执行" : auditSummary)
                + "，重试次数=" + retryCount;
        logSeparatorBlock(title, detail);
    }

    private void logSeparatorBlock(String title, String detail) {
        uiLog.log("INFO", title);
        uiLog.log("INFO", detail);
        uiLog.log("INFO", title);
        LOGGER.info(title);
        LOGGER.info(detail);
        LOGGER.info(title);
    }

    private String scheduleSummary() {
        SchedulePolicy policy = task.getSchedulePolicy();
        return "间隔=" + policy.getIntervalSeconds() + "秒"
                + "，窗口=" + policy.getWindowStart() + "-" + policy.getWindowEnd()
                + "，重试=" + policy.getMaxRetries()
                + "，租约=" + policy.getLeaseSeconds() + "秒";
    }

    private String safeString(String value) {
        return value == null ? "" : value;
    }

    private String buildOwnerId(String runId) {
        String hostname = "未知";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception ignored) {
            // ignore
        }
        long pid = ProcessHandle.current().pid();
        return runId + "@" + hostname + ":" + pid;
    }

    private int parseInt(String value) {
        if (value == null || value.isBlank()) {
            return 0;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ex) {
            return 0;
        }
    }

    @SafeVarargs
    private final <T> T firstNonNull(T... values) {
        if (values == null) {
            return null;
        }
        for (T value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private record SqlExecution(String jobId,
                                AsyncSqlClient.StatusResponse status,
                                AsyncSqlClient.ResultResponse result) {
    }

    private record AuditResult(boolean success, String message) {
    }

    private static class LeaseState {
        private Instant renewedAt;

        private LeaseState(Instant renewedAt) {
            this.renewedAt = renewedAt;
        }
    }
}
