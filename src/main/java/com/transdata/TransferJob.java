package com.transdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransferJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransferJob.class);
    private static final DateTimeFormatter LOG_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final AppConfig config;
    private final SourceClient sourceClient;
    private final AsyncSqlClient asyncSqlClient;
    private final SqlBuilder sqlBuilder;
    private final UiLogSink uiLog;
    private final ProgressListener progressListener;
    private final AtomicBoolean cancelled;

    public TransferJob(AppConfig config,
                       SourceClient sourceClient,
                       AsyncSqlClient asyncSqlClient,
                       SqlBuilder sqlBuilder,
                       UiLogSink uiLog,
                       ProgressListener progressListener,
                       AtomicBoolean cancelled) {
        this.config = config;
        this.sourceClient = sourceClient;
        this.asyncSqlClient = asyncSqlClient;
        this.sqlBuilder = sqlBuilder;
        this.uiLog = uiLog;
        this.progressListener = progressListener;
        this.cancelled = cancelled;
    }

    @Override
    public void run() {
        Instant start = Instant.now();
        String startTimeText = LocalDateTime.now().format(LOG_TIME_FORMATTER);
        String requestId = UUID.randomUUID().toString();
        String runId = UUID.randomUUID().toString();
        String lockName = resolveLockName();
        String ownerId = buildOwnerId(runId);
        TransferStats stats = new TransferStats();
        Map<GroupKey, Integer> expectedByKey = null;
        List<GroupKey> keys = null;
        boolean lockHeld = false;
        boolean startLogged = false;
        boolean lockResult = false;
        int totalExpected = 0;
        int totalGroups = 0;
        int batchSize = Math.max(1, config.getBatchSize());
        int insertedRows = 0;
        int retryCount = 0;
        String auditSummary = "未执行";
        String finalStatus = "FAILED";
        LeaseState leaseState = new LeaseState(Instant.EPOCH);

        try {
            if (!validateCrypto()) {
                updateStage("失败");
                return;
            }

            updateStage("锁定中");
            executeStatement(sqlBuilder.buildCreateLockTableSql(), "创建锁表", runId, requestId, stats,
                    null, 1, 1);
            lockHeld = acquireLock(lockName, ownerId, requestId, stats, runId);
            lockResult = lockHeld;
            if (!lockHeld) {
                updateStage("已跳过");
                updateProgress(100, "因锁占用跳过");
                logStartSeparator(runId, startTimeText, totalGroups, totalExpected, batchSize, lockResult);
                startLogged = true;
                finalStatus = "SKIPPED_LOCK";
                return;
            }
            leaseState.renewedAt = Instant.now();

            updateStage("获取中");
            uiLog.log("INFO", "开始拉取源数据。运行ID=" + runId + "，请求ID=" + requestId);
            List<SourceRecord> records = sourceClient.fetch(config, uiLog);
            totalExpected = records.size();
            stats.setTotalRecords(totalExpected);
            progressListener.updateStats(stats);
            if (records.isEmpty()) {
                logStartSeparator(runId, startTimeText, 0, 0, batchSize, lockResult);
                startLogged = true;
                uiLog.log("INFO", "本次拉取记录数为 0，无需写入。运行ID=" + runId + "，请求ID=" + requestId);
                updateStage("完成");
                finalStatus = "SUCCESS";
                auditSummary = "无数据";
                return;
            }
            if (cancelled.get()) {
                logStartSeparator(runId, startTimeText, 0, totalExpected, batchSize, lockResult);
                startLogged = true;
                uiLog.log("WARN", "拉取后任务被取消。运行ID=" + runId + "，请求ID=" + requestId);
                updateStage("已取消");
                return;
            }

            updateStage("分组中");
            Map<GroupKey, List<SourceRecord>> grouped = groupRecords(records);
            expectedByKey = expectedCounts(grouped);
            keys = new ArrayList<>(grouped.keySet());
            totalGroups = grouped.size();
            stats.setTotalGroups(totalGroups);
            progressListener.updateStats(stats);
            logStartSeparator(runId, startTimeText, totalGroups, totalExpected, batchSize, lockResult);
            startLogged = true;

            int distinctOrderItemIds = countDistinctOrderItemIds(records);
            uiLog.log("INFO", "已拉取记录=" + totalExpected
                    + "，分组数=" + grouped.size()
                    + "，预期分组明细=" + expectedByKey
                    + "，order_item_id 去重数=" + distinctOrderItemIds
                    + "，运行ID=" + runId
                    + "，请求ID=" + requestId);

            updateStage("准备中");
            executeStatement(sqlBuilder.buildCreateStagingTableSql(), "创建临时表", runId, requestId, stats,
                    null, 1, 1);
            executeStatement(sqlBuilder.buildCreateStagingIndexSql(), "创建临时表索引", runId, requestId, stats,
                    null, 1, 1);

            int maxRetries = Math.max(1, config.getJobMaxRetries());
            boolean success = false;
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                retryCount = Math.max(0, attempt - 1);
                if (cancelled.get()) {
                    throw new InterruptedException("任务在第 " + attempt + " 次尝试前被取消");
                }
                uiLog.log("INFO", "开始第 " + attempt + "/" + maxRetries + " 次尝试。运行ID=" + runId
                        + "，请求ID=" + requestId);
                try {
                    AuditResult attemptResult = runAttempt(records, keys, expectedByKey, totalExpected,
                            distinctOrderItemIds, runId, requestId, stats, lockName, ownerId, leaseState);
                    success = attemptResult.success();
                    auditSummary = attemptResult.message();
                } catch (Exception ex) {
                    uiLog.log("ERROR", "第 " + attempt + " 次尝试失败：" + ex.getMessage() + "，运行ID=" + runId
                            + "，请求ID=" + requestId);
                    LOGGER.error("第 {} 次尝试失败", attempt, ex);
                    cleanupScope(keys, runId, requestId, stats, "尝试失败");
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
                finalStatus = "FAILED";
                return;
            }

            updateStage("完成");
            Duration duration = Duration.between(start, Instant.now());
            uiLog.log("INFO", "任务成功完成，耗时 " + duration.toMillis() + " ms，运行ID=" + runId
                    + "，请求ID=" + requestId);
            finalStatus = "SUCCESS";
            insertedRows = totalExpected;
        } catch (InterruptedException ex) {
            updateStage("已取消");
            uiLog.log("WARN", "任务取消：" + ex.getMessage() + "，运行ID=" + runId + "，请求ID=" + requestId);
            finalStatus = "FAILED";
        } catch (Exception ex) {
            updateStage("失败");
            uiLog.log("ERROR", "任务失败：" + ex.getMessage() + "，运行ID=" + runId + "，请求ID=" + requestId);
            LOGGER.error("任务失败", ex);
            finalStatus = "FAILED";
        } finally {
            if (lockHeld) {
                releaseLock(lockName, ownerId, runId, requestId, stats);
            }
            if (!startLogged) {
                logStartSeparator(runId, startTimeText, totalGroups, totalExpected, batchSize, lockResult);
            }
            String endTimeText = LocalDateTime.now().format(LOG_TIME_FORMATTER);
            Duration duration = Duration.between(start, Instant.now());
            logEndSeparator(runId, endTimeText, duration, finalStatus, insertedRows, auditSummary, retryCount);
        }
    }


    private AuditResult runAttempt(List<SourceRecord> records,
                                   List<GroupKey> keys,
                                   Map<GroupKey, Integer> expectedByKey,
                                   int totalExpected,
                                   int distinctOrderItemIds,
                                   String runId,
                                   String requestId,
                                   TransferStats stats,
                                   String lockName,
                                   String ownerId,
                                   LeaseState leaseState) throws Exception {
        updateStage("分段写入中");
        leaseState.renewedAt = maybeRenewLease(lockName, ownerId, leaseState.renewedAt, runId, requestId, stats);
        executeStatement(sqlBuilder.buildClearStagingSql(runId), "清理临时表", runId, requestId, stats,
                null, 1, 1);
        int batchSize = Math.max(1, config.getBatchSize());
        List<String> segments = sqlBuilder.buildInsertStagingSqlSegments(records, runId, batchSize, uiLog);
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
                    null, segmentRows, totalSegments);
            updateProgress(segmentIndex, totalSegments);
        }

        updateStage("应用中");
        uiLog.log("INFO", "正在将临时表数据应用到目标表。运行ID=" + runId
                + "，分组键=" + keys + "，请求ID=" + requestId);
        leaseState.renewedAt = maybeRenewLease(lockName, ownerId, leaseState.renewedAt, runId, requestId, stats);
        executeStatement(sqlBuilder.buildApplyFromStagingSql(keys, runId, uiLog), "应用到目标表", runId, requestId,
                stats, null, records.size(), 1);

        updateStage("稽核中");
        leaseState.renewedAt = maybeRenewLease(lockName, ownerId, leaseState.renewedAt, runId, requestId, stats);
        AuditResult auditResult = auditTarget(keys, expectedByKey, totalExpected, distinctOrderItemIds,
                runId, requestId, stats);
        if (!auditResult.success()) {
            uiLog.log("WARN", "稽核不一致：" + auditResult.message() + "，运行ID=" + runId
                    + "，请求ID=" + requestId);
            cleanupScope(keys, runId, requestId, stats, "稽核失败");
            return auditResult;
        }

        executeStatement(sqlBuilder.buildClearStagingSql(runId), "清理临时表_成功", runId, requestId, stats,
                null, 1, 1);
        uiLog.log("INFO", "稽核通过。运行ID=" + runId + "，请求ID=" + requestId);
        return auditResult;
    }

    private AuditResult auditTarget(List<GroupKey> keys,
                                    Map<GroupKey, Integer> expectedByKey,
                                    int totalExpected,
                                    int distinctExpected,
                                    String runId,
                                    String requestId,
                                    TransferStats stats) throws Exception {
        AsyncSqlClient.ResultResponse groupResult = executeQuery(sqlBuilder.buildAuditByGroupSql(keys, uiLog),
                "分组稽核", runId, requestId, stats, null, 1, 1);
        List<List<String>> rows = groupResult.getRows();
        if (rows == null) {
            throw new IllegalStateException("分组稽核结果解析失败。原始JSON="
                    + safeString(groupResult.getRawJson()));
        }
        Map<GroupKey, AuditCounts> actualByKey = new LinkedHashMap<>();
        int totalActual = 0;
        for (List<String> row : rows) {
            String dateNo = getCell(row, 0);
            String datetime = getCell(row, 1);
            int cnt = parseInt(getCell(row, 2));
            int dcnt = parseInt(getCell(row, 3));
            GroupKey key = new GroupKey(dateNo, datetime);
            actualByKey.put(key, new AuditCounts(cnt, dcnt));
            totalActual += cnt;
        }

        AsyncSqlClient.ResultResponse totalsResult = executeQuery(sqlBuilder.buildAuditTotalsSql(keys, uiLog),
                "汇总稽核", runId, requestId, stats, null, 1, 1);
        List<List<String>> totalRows = totalsResult.getRows();
        if (totalRows == null || totalRows.isEmpty()) {
            throw new IllegalStateException("汇总稽核结果解析失败。原始JSON="
                    + safeString(totalsResult.getRawJson()));
        }
        int totalDistinctActual = parseInt(getCell(totalRows.get(0), 1));

        List<String> diffs = new ArrayList<>();
        for (Map.Entry<GroupKey, Integer> entry : expectedByKey.entrySet()) {
            GroupKey key = entry.getKey();
            int expected = entry.getValue();
            AuditCounts actual = actualByKey.get(key);
            if (actual == null) {
                diffs.add(key + " 预期=" + expected + " 实际=0");
                continue;
            }
            if (actual.count() != expected) {
                diffs.add(key + " 预期=" + expected + " 实际=" + actual.count());
            }
        }
        for (GroupKey actualKey : actualByKey.keySet()) {
            if (!expectedByKey.containsKey(actualKey)) {
                diffs.add(actualKey + " 在目标表中出现但不在预期范围内");
            }
        }
        if (totalActual != totalExpected) {
            diffs.add("总数预期=" + totalExpected + " 总数实际=" + totalActual);
        }
        if (totalDistinctActual != distinctExpected) {
            diffs.add("去重预期=" + distinctExpected + " 去重实际=" + totalDistinctActual);
        }

        if (!diffs.isEmpty()) {
            String message = String.join("; ", diffs);
            uiLog.log("WARN", "稽核差异：" + message + "，运行ID=" + runId + "，请求ID=" + requestId);
            return new AuditResult(false, message);
        }
        return new AuditResult(true, "通过");
    }

    private boolean validateCrypto() {
        if (config.getAesKey().isBlank() || config.getAesIv().isBlank()) {
            uiLog.log("ERROR", "AES Key/IV 缺失，请配置 crypto.aesKey 与 crypto.aesIv。");
            return false;
        }
        return true;
    }

    private Map<GroupKey, List<SourceRecord>> groupRecords(List<SourceRecord> records) {
        Map<GroupKey, List<SourceRecord>> grouped = new LinkedHashMap<>();
        for (SourceRecord record : records) {
            GroupKey key = new GroupKey(record.getDateNo(), record.getDatetime());
            grouped.computeIfAbsent(key, ignored -> new ArrayList<>()).add(record);
        }
        return grouped;
    }

    private Map<GroupKey, Integer> expectedCounts(Map<GroupKey, List<SourceRecord>> grouped) {
        Map<GroupKey, Integer> expected = new LinkedHashMap<>();
        for (Map.Entry<GroupKey, List<SourceRecord>> entry : grouped.entrySet()) {
            expected.put(entry.getKey(), entry.getValue().size());
        }
        return expected;
    }

    private int countDistinctOrderItemIds(List<SourceRecord> records) {
        Set<String> distinct = new HashSet<>();
        for (SourceRecord record : records) {
            String orderItemId = record.getOrderItemId();
            if (orderItemId != null && !orderItemId.isBlank()) {
                distinct.add(orderItemId);
            }
        }
        return distinct.size();
    }

    private boolean acquireLock(String lockName, String ownerId, String requestId, TransferStats stats, String runId)
            throws Exception {
        int leaseSeconds = Math.max(30, config.getLockLeaseSeconds());
        SqlExecution execution = executeStatement(sqlBuilder.buildLockAcquireSql(lockName, ownerId, leaseSeconds),
                "获取锁", runId, requestId, stats, null, 1, 1);
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
        int leaseSeconds = Math.max(30, config.getLockLeaseSeconds());
        Instant now = Instant.now();
        if (Duration.between(lastRenewedAt, now).toSeconds() < leaseSeconds / 2L) {
            return lastRenewedAt;
        }
        SqlExecution execution = executeStatement(sqlBuilder.buildLockAcquireSql(lockName, ownerId, leaseSeconds),
                "续期锁", runId, requestId, stats, null, 1, 1);
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
                    null, 1, 1);
            uiLog.log("INFO", "锁已释放。锁名称=" + lockName + "，持有者=" + ownerId + "，运行ID=" + runId
                    + "，请求ID=" + requestId);
        } catch (Exception ex) {
            uiLog.log("WARN", "释放锁失败：" + ex.getMessage() + "，锁名称=" + lockName
                    + "，运行ID=" + runId + "，请求ID=" + requestId);
            LOGGER.warn("释放锁失败", ex);
        }
    }

    private void cleanupScope(List<GroupKey> keys, String runId, String requestId, TransferStats stats, String reason) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        try {
            updateStage("清理中");
            uiLog.log("INFO", "开始清理范围：原因=" + reason + "，分组键=" + keys + "，运行ID=" + runId
                    + "，请求ID=" + requestId);
            executeStatement(sqlBuilder.buildDeleteTargetByKeysSql(keys, uiLog), "清理目标表", runId, requestId,
                    stats, null, 1, 1);
            executeStatement(sqlBuilder.buildClearStagingSql(runId), "清理临时表", runId, requestId, stats,
                    null, 1, 1);
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
                                          GroupKey key,
                                          int records,
                                          int totalSegments) throws Exception {
        return executeSql(sql, context, runId, requestId, stats, key, records, totalSegments, false);
    }

    private AsyncSqlClient.ResultResponse executeQuery(String sql,
                                                      String context,
                                                      String runId,
                                                      String requestId,
                                                      TransferStats stats,
                                                      GroupKey key,
                                                      int records,
                                                      int totalSegments) throws Exception {
        return executeSql(sql, context, runId, requestId, stats, key, records, totalSegments, true).result;
    }

    private SqlExecution executeSql(String sql,
                                    String context,
                                    String runId,
                                    String requestId,
                                    TransferStats stats,
                                    GroupKey key,
                                    int records,
                                    int totalSegments,
                                    boolean fetchResult) throws Exception {
        if (cancelled.get()) {
            throw new InterruptedException("SQL 执行前任务已取消：" + context);
        }
        updateStage("加密中");
        logPlainSql(sql, runId, requestId, key, context, records, totalSegments);
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
        if (handleTerminalStatus(status, jobId, requestId, context)) {
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
                                         String context) {
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
            uiLog.log("ERROR", context + " 执行失败。作业ID=" + jobId
                    + "，状态=" + status.getStatus()
                    + "，错误信息=" + safeString(status.getErrorMessage())
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
                             GroupKey key,
                             String contextTag,
                             int records,
                             int totalSegments) {
        String targetTable = sqlBuilder.getTargetTable();
        String keyText = key == null ? "(全部)" : key.toString();
        uiLog.log("INFO", "SQL 已准备提交。运行ID=" + runId
                + "，请求ID=" + requestId
                + "，上下文=" + contextTag
                + "，分组键=" + keyText
                + "，记录数=" + records
                + "，分段数=" + totalSegments
                + "，数据库用户=" + config.getAsyncDbUser()
                + "，目标表=" + targetTable);
        int maxChars = config.getLoggingSqlMaxChars();
        if (sql.length() > maxChars) {
            int snippet = Math.min(8000, Math.max(1, maxChars / 2));
            String head = sql.substring(0, Math.min(snippet, sql.length()));
            String tail = sql.substring(Math.max(0, sql.length() - snippet));
            Path filePath = buildSqlDumpPath(requestId, contextTag, key);
            uiLog.log("INFO", "SQL 预览（已截断）。完整 SQL 将保存到 " + filePath
                    + "。预览=" + head + "...(已截断 " + (sql.length() - (head.length() + tail.length()))
                    + " 个字符)..." + tail);
            writeSqlDump(filePath, sql);
        } else {
            uiLog.log("INFO", "SQL 内容：" + sql);
        }
    }

    private Path buildSqlDumpPath(String requestId, String contextTag, GroupKey key) {
        String date = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
        String dumpDir = config.getLoggingSqlDumpDir();
        String safeGroup = key == null ? "全部" : sanitizeFilePart(key.getDateNo() + "_" + key.getDatetime());
        String safeRequest = sanitizeFilePart(requestId);
        String safeContext = sanitizeFilePart(contextTag);
        return Paths.get(dumpDir, date, "sql_" + safeRequest + "_" + safeContext + "_" + safeGroup + ".sql");
    }

    private void writeSqlDump(Path path, String sql) {
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(path, sql, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            uiLog.log("WARN", "SQL 落盘失败：" + path + "，原因=" + ex.getMessage());
            LOGGER.warn("SQL 落盘失败: {}", path, ex);
        }
    }

    private String sanitizeFilePart(String value) {
        if (value == null || value.isBlank()) {
            return "未知";
        }
        return value.replaceAll("[^\\p{L}\\p{N}._-]", "_");
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
                                   int expectedGroups,
                                   int recordCount,
                                   int batchSize,
                                   boolean lockAcquired) {
        String title = "====================【传输任务开始】====================";
        String detail = "运行ID=" + runId
                + "，开始时间=" + startTime
                + "，预期分组数=" + expectedGroups
                + "，记录数=" + recordCount
                + "，分段大小=" + batchSize
                + "，分布式锁=" + (lockAcquired ? "成功" : "失败");
        logSeparatorBlock(title, detail);
    }

    private void logEndSeparator(String runId,
                                 String endTime,
                                 Duration duration,
                                 String status,
                                 int insertedRows,
                                 String auditSummary,
                                 int retryCount) {
        String title = "====================【传输任务结束】====================";
        String detail = "运行ID=" + runId
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

    private String safeString(String value) {
        return value == null ? "" : value;
    }

    private String resolveLockName() {
        String configured = config.getLockName();
        return configured.isBlank() ? "trans_data_job" : configured;
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

    private String getCell(List<String> row, int index) {
        if (row == null || index < 0 || index >= row.size()) {
            return null;
        }
        return row.get(index);
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

    private record AuditCounts(int count, int distinctCount) {
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
