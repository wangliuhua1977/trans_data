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
        String requestId = UUID.randomUUID().toString();
        String runId = UUID.randomUUID().toString();
        String lockName = resolveLockName();
        String ownerId = buildOwnerId(runId);
        TransferStats stats = new TransferStats();
        Map<GroupKey, Integer> expectedByKey = null;
        List<GroupKey> keys = null;
        boolean lockHeld = false;
        LeaseState leaseState = new LeaseState(Instant.EPOCH);

        try {
            if (!validateCrypto()) {
                updateStage("Failed");
                return;
            }

            updateStage("Locking");
            executeStatement(sqlBuilder.buildCreateLockTableSql(), "create_lock_table", runId, requestId, stats,
                    null, 1, 1);
            lockHeld = acquireLock(lockName, ownerId, requestId, stats, runId);
            if (!lockHeld) {
                updateStage("Skipped");
                updateProgress(100, "Skipped due to lock");
                return;
            }
            leaseState.renewedAt = Instant.now();

            updateStage("Fetching");
            uiLog.log("INFO", "Run start: acquiring source data. runId=" + runId + ", requestId=" + requestId);
            List<SourceRecord> records = sourceClient.fetch(config, uiLog);
            stats.setTotalRecords(records.size());
            progressListener.updateStats(stats);
            if (records.isEmpty()) {
                uiLog.log("INFO", "0 records fetched, no write needed. runId=" + runId + ", requestId=" + requestId);
                updateStage("Done");
                return;
            }
            if (cancelled.get()) {
                uiLog.log("WARN", "Job cancelled after fetch. runId=" + runId + ", requestId=" + requestId);
                updateStage("Cancelled");
                return;
            }

            updateStage("Grouping");
            Map<GroupKey, List<SourceRecord>> grouped = groupRecords(records);
            expectedByKey = expectedCounts(grouped);
            keys = new ArrayList<>(grouped.keySet());
            stats.setTotalGroups(grouped.size());
            progressListener.updateStats(stats);

            int totalExpected = records.size();
            int distinctOrderItemIds = countDistinctOrderItemIds(records);
            uiLog.log("INFO", "Fetched records=" + totalExpected
                    + ", groups=" + grouped.size()
                    + ", expectedByKey=" + expectedByKey
                    + ", distinctOrderItemIds=" + distinctOrderItemIds
                    + ", runId=" + runId
                    + ", requestId=" + requestId);

            updateStage("Preparing");
            executeStatement(sqlBuilder.buildCreateStagingTableSql(), "create_staging_table", runId, requestId, stats,
                    null, 1, 1);
            executeStatement(sqlBuilder.buildCreateStagingIndexSql(), "create_staging_index", runId, requestId, stats,
                    null, 1, 1);

            int maxRetries = Math.max(1, config.getJobMaxRetries());
            boolean success = false;
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                if (cancelled.get()) {
                    throw new InterruptedException("Job cancelled before attempt " + attempt);
                }
                uiLog.log("INFO", "Attempt " + attempt + "/" + maxRetries + " started. runId=" + runId
                        + ", requestId=" + requestId);
                try {
                    success = runAttempt(records, keys, expectedByKey, totalExpected, distinctOrderItemIds, runId,
                            requestId, stats, lockName, ownerId, leaseState);
                } catch (Exception ex) {
                    uiLog.log("ERROR", "Attempt " + attempt + " failed: " + ex.getMessage() + ", runId=" + runId
                            + ", requestId=" + requestId);
                    LOGGER.error("Attempt {} failed", attempt, ex);
                    cleanupScope(keys, runId, requestId, stats, "attempt_failure");
                    success = false;
                }
                if (success) {
                    break;
                }
                if (attempt < maxRetries) {
                    uiLog.log("WARN", "Audit failed; retrying after cleanup. attempt=" + attempt + ", runId=" + runId
                            + ", requestId=" + requestId);
                }
            }

            if (!success) {
                uiLog.log("ERROR", "Job failed after max retries. cleanup executed. runId=" + runId
                        + ", requestId=" + requestId);
                updateStage("Failed");
                return;
            }

            updateStage("Done");
            Duration duration = Duration.between(start, Instant.now());
            uiLog.log("INFO", "Job completed successfully in " + duration.toMillis() + " ms, runId=" + runId
                    + ", requestId=" + requestId);
        } catch (InterruptedException ex) {
            updateStage("Cancelled");
            uiLog.log("WARN", "Job cancelled: " + ex.getMessage() + ", runId=" + runId + ", requestId=" + requestId);
        } catch (Exception ex) {
            updateStage("Failed");
            uiLog.log("ERROR", "Job failed: " + ex.getMessage() + ", runId=" + runId + ", requestId=" + requestId);
            LOGGER.error("Job failed", ex);
        } finally {
            if (lockHeld) {
                releaseLock(lockName, ownerId, runId, requestId, stats);
            }
        }
    }
    private void updateProgress(int percent, String message) {
        progressListener.updateProgress(percent, message);
    }

    private boolean runAttempt(List<SourceRecord> records,
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
        updateStage("Staging");
        leaseState.renewedAt = maybeRenewLease(lockName, ownerId, leaseState.renewedAt, runId, requestId, stats);
        executeStatement(sqlBuilder.buildClearStagingSql(runId), "cleanup_staging", runId, requestId, stats,
                null, 1, 1);
        int batchSize = Math.max(1, config.getBatchSize());
        List<String> segments = sqlBuilder.buildInsertStagingSqlSegments(records, runId, batchSize, uiLog);
        int totalSegments = segments.size();
        stats.setTotalBatches(totalSegments);
        stats.setCurrentBatch(0);
        progressListener.updateStats(stats);

        for (int i = 0; i < segments.size(); i++) {
            if (cancelled.get()) {
                throw new InterruptedException("Job cancelled before staging segment submit");
            }
            int segmentIndex = i + 1;
            stats.setCurrentBatch(segmentIndex);
            progressListener.updateStats(stats);
            int segmentRows = Math.min(batchSize, records.size() - (segmentIndex - 1) * batchSize);
            uiLog.log("INFO", "Staging segment " + segmentIndex + "/" + totalSegments
                    + ", rows=" + segmentRows
                    + ", runId=" + runId
                    + ", requestId=" + requestId);
            leaseState.renewedAt = maybeRenewLease(lockName, ownerId, leaseState.renewedAt, runId, requestId, stats);
            executeStatement(segments.get(i), "staging_segment_" + segmentIndex, runId, requestId, stats,
                    null, segmentRows, totalSegments);
            updateProgress(segmentIndex, totalSegments);
        }

        updateStage("Applying");
        uiLog.log("INFO", "Applying staged data to target. runId=" + runId
                + ", keys=" + keys + ", requestId=" + requestId);
        leaseState.renewedAt = maybeRenewLease(lockName, ownerId, leaseState.renewedAt, runId, requestId, stats);
        executeStatement(sqlBuilder.buildApplyFromStagingSql(keys, runId, uiLog), "apply_to_target", runId, requestId,
                stats, null, records.size(), 1);

        updateStage("Auditing");
        leaseState.renewedAt = maybeRenewLease(lockName, ownerId, leaseState.renewedAt, runId, requestId, stats);
        AuditResult auditResult = auditTarget(keys, expectedByKey, totalExpected, distinctOrderItemIds,
                runId, requestId, stats);
        if (!auditResult.success()) {
            uiLog.log("WARN", "Audit mismatch detected: " + auditResult.message() + ", runId=" + runId
                    + ", requestId=" + requestId);
            cleanupScope(keys, runId, requestId, stats, "audit_failure");
            return false;
        }

        executeStatement(sqlBuilder.buildClearStagingSql(runId), "cleanup_staging_success", runId, requestId, stats,
                null, 1, 1);
        uiLog.log("INFO", "Audit passed. runId=" + runId + ", requestId=" + requestId);
        return true;
    }

    private AuditResult auditTarget(List<GroupKey> keys,
                                    Map<GroupKey, Integer> expectedByKey,
                                    int totalExpected,
                                    int distinctExpected,
                                    String runId,
                                    String requestId,
                                    TransferStats stats) throws Exception {
        AsyncSqlClient.ResultResponse groupResult = executeQuery(sqlBuilder.buildAuditByGroupSql(keys, uiLog),
                "audit_by_group", runId, requestId, stats, null, 1, 1);
        List<List<String>> rows = groupResult.getRows();
        if (rows == null) {
            throw new IllegalStateException("Audit result parsing failed for group query. rawJson="
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
                "audit_totals", runId, requestId, stats, null, 1, 1);
        List<List<String>> totalRows = totalsResult.getRows();
        if (totalRows == null || totalRows.isEmpty()) {
            throw new IllegalStateException("Audit totals parsing failed. rawJson="
                    + safeString(totalsResult.getRawJson()));
        }
        int totalDistinctActual = parseInt(getCell(totalRows.get(0), 1));

        List<String> diffs = new ArrayList<>();
        for (Map.Entry<GroupKey, Integer> entry : expectedByKey.entrySet()) {
            GroupKey key = entry.getKey();
            int expected = entry.getValue();
            AuditCounts actual = actualByKey.get(key);
            if (actual == null) {
                diffs.add(key + " expected=" + expected + " actual=0");
                continue;
            }
            if (actual.count() != expected) {
                diffs.add(key + " expected=" + expected + " actual=" + actual.count());
            }
        }
        for (GroupKey actualKey : actualByKey.keySet()) {
            if (!expectedByKey.containsKey(actualKey)) {
                diffs.add(actualKey + " unexpected_in_target");
            }
        }
        if (totalActual != totalExpected) {
            diffs.add("total_expected=" + totalExpected + " total_actual=" + totalActual);
        }
        if (totalDistinctActual != distinctExpected) {
            diffs.add("distinct_expected=" + distinctExpected + " distinct_actual=" + totalDistinctActual);
        }

        if (!diffs.isEmpty()) {
            String message = String.join("; ", diffs);
            uiLog.log("WARN", "Audit differences: " + message + ", runId=" + runId + ", requestId=" + requestId);
            return new AuditResult(false, message);
        }
        return new AuditResult(true, "OK");
    }

    private boolean validateCrypto() {
        if (config.getAesKey().isBlank() || config.getAesIv().isBlank()) {
            uiLog.log("ERROR", "AES key/IV missing. Please configure crypto.aesKey and crypto.aesIv.");
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
                "lock_acquire", runId, requestId, stats, null, 1, 1);
        Integer rowsAffected = resolveRowsAffected(execution, requestId, "lock_acquire");
        if (rowsAffected == null) {
            throw new IllegalStateException("Unable to determine lock acquisition result.");
        }
        if (rowsAffected == 0) {
            uiLog.log("INFO", "Another instance is running; skipping run. lockName=" + lockName + ", runId=" + runId
                    + ", requestId=" + requestId);
            return false;
        }
        uiLog.log("INFO", "Lock acquired. lockName=" + lockName + ", ownerId=" + ownerId + ", runId=" + runId
                + ", requestId=" + requestId);
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
                "lock_renew", runId, requestId, stats, null, 1, 1);
        Integer rowsAffected = resolveRowsAffected(execution, requestId, "lock_renew");
        if (rowsAffected == null || rowsAffected == 0) {
            throw new IllegalStateException("Failed to renew lock lease; another instance may have taken over.");
        }
        uiLog.log("INFO", "Lock renewed. lockName=" + lockName + ", ownerId=" + ownerId + ", runId=" + runId
                + ", requestId=" + requestId);
        return now;
    }

    private void releaseLock(String lockName, String ownerId, String runId, String requestId, TransferStats stats) {
        try {
            executeStatement(sqlBuilder.buildLockReleaseSql(lockName, ownerId), "lock_release", runId, requestId, stats,
                    null, 1, 1);
            uiLog.log("INFO", "Lock released. lockName=" + lockName + ", ownerId=" + ownerId + ", runId=" + runId
                    + ", requestId=" + requestId);
        } catch (Exception ex) {
            uiLog.log("WARN", "Failed to release lock: " + ex.getMessage() + ", lockName=" + lockName
                    + ", runId=" + runId + ", requestId=" + requestId);
            LOGGER.warn("Failed to release lock", ex);
        }
    }

    private void cleanupScope(List<GroupKey> keys, String runId, String requestId, TransferStats stats, String reason) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        try {
            updateStage("Cleaning");
            uiLog.log("INFO", "Cleanup scope: reason=" + reason + ", keys=" + keys + ", runId=" + runId
                    + ", requestId=" + requestId);
            executeStatement(sqlBuilder.buildDeleteTargetByKeysSql(keys, uiLog), "cleanup_target", runId, requestId,
                    stats, null, 1, 1);
            executeStatement(sqlBuilder.buildClearStagingSql(runId), "cleanup_staging", runId, requestId, stats,
                    null, 1, 1);
        } catch (Exception ex) {
            uiLog.log("ERROR", "Cleanup failed: " + ex.getMessage() + ", runId=" + runId
                    + ", requestId=" + requestId);
            LOGGER.error("Cleanup failed", ex);
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
            throw new InterruptedException("Job cancelled before SQL execution: " + context);
        }
        updateStage("Encrypting");
        logPlainSql(sql, runId, requestId, key, context, records, totalSegments);
        String encryptedSql = encryptSql(sql);

        updateStage("Submitting");
        AsyncSqlClient.SubmitResponse submitResponse = asyncSqlClient.submit(config, encryptedSql, uiLog, requestId);
        String jobId = submitResponse.getJobId();
        stats.setJobId(jobId);
        progressListener.updateStats(stats);
        uiLog.log("INFO", "Submitted SQL: context=" + context + ", jobId=" + jobId + ", status="
                + submitResponse.getStatus() + ", runId=" + runId + ", requestId=" + requestId);

        updateStage("Polling");
        AsyncSqlClient.StatusResponse status = asyncSqlClient.pollStatus(
                config, jobId, uiLog, progressListener, cancelled, requestId);
        if (handleTerminalStatus(status, jobId, requestId, context)) {
            throw new IllegalStateException(context + " failed with status=" + status.getStatus());
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
            uiLog.log("WARN", "Unable to resolve rowsAffected for " + context + ": " + ex.getMessage()
                    + ", requestId=" + requestId);
            return null;
        }
    }

    private boolean handleTerminalStatus(AsyncSqlClient.StatusResponse status,
                                         String jobId,
                                         String requestId,
                                         String context) {
        if ("CANCELLED_LOCAL".equalsIgnoreCase(status.getStatus())) {
            uiLog.log("WARN", context + " cancelled during polling. jobId=" + jobId + ", requestId=" + requestId
                    + ", reason=" + status.getErrorMessage());
            updateStage("Cancelled");
            return true;
        }
        if ("TIMEOUT".equalsIgnoreCase(status.getStatus())) {
            uiLog.log("ERROR", context + " polling timeout. jobId=" + jobId
                    + ", errorMessage=" + status.getErrorMessage()
                    + ", requestId=" + requestId);
            updateStage("Failed");
            return true;
        }
        if (!"SUCCEEDED".equalsIgnoreCase(status.getStatus())) {
            uiLog.log("ERROR", context + " failed. jobId=" + jobId
                    + ", status=" + status.getStatus()
                    + ", errorMessage=" + safeString(status.getErrorMessage())
                    + ", errorPosition=" + safeString(status.getErrorPosition())
                    + ", sqlState=" + safeString(status.getSqlState())
                    + ", traceId=" + safeString(status.getTraceId())
                    + ", requestId=" + requestId);
            updateStage("Failed");
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
            uiLog.log("INFO", "Job succeeded. context=" + context + ", jobId=" + jobId
                    + ", rowsAffected=" + result.getRowsAffected()
                    + ", actualRows=" + result.getActualRows()
                    + ", columns=" + result.getColumnsCount()
                    + ", runId=" + runId
                    + ", requestId=" + requestId);
        } else {
            uiLog.log("INFO", "Job succeeded. context=" + context + ", jobId=" + jobId
                    + ", rowsAffected=" + status.getRowsAffected()
                    + ", updatedRows=" + status.getUpdatedRows()
                    + ", affectedRows=" + status.getAffectedRows()
                    + ", actualRows=" + status.getActualRows()
                    + ", elapsed=" + status.getElapsedMillis()
                    + ", runId=" + runId
                    + ", requestId=" + requestId);
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
        String keyText = key == null ? "(all)" : key.toString();
        uiLog.log("INFO", "Prepared SQL for submit. runId=" + runId
                + ", requestId=" + requestId
                + ", context=" + contextTag
                + ", groupKey=" + keyText
                + ", records=" + records
                + ", segments=" + totalSegments
                + ", dbUser=" + config.getAsyncDbUser()
                + ", targetTable=" + targetTable);
        int maxChars = config.getLoggingSqlMaxChars();
        if (sql.length() > maxChars) {
            int snippet = Math.min(8000, Math.max(1, maxChars / 2));
            String head = sql.substring(0, Math.min(snippet, sql.length()));
            String tail = sql.substring(Math.max(0, sql.length() - snippet));
            Path filePath = buildSqlDumpPath(requestId, contextTag, key);
            uiLog.log("INFO", "SQL preview (truncated). Full SQL will be saved to " + filePath
                    + ". Preview=" + head + "...(truncated " + (sql.length() - (head.length() + tail.length()))
                    + " chars)..." + tail);
            writeSqlDump(filePath, sql);
        } else {
            uiLog.log("INFO", "SQL prepared: " + sql);
        }
    }

    private Path buildSqlDumpPath(String requestId, String contextTag, GroupKey key) {
        String date = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
        String dumpDir = config.getLoggingSqlDumpDir();
        String safeGroup = key == null ? "all" : sanitizeFilePart(key.getDateNo() + "_" + key.getDatetime());
        String safeRequest = sanitizeFilePart(requestId);
        String safeContext = sanitizeFilePart(contextTag);
        return Paths.get(dumpDir, date, "sql_" + safeRequest + "_" + safeContext + "_" + safeGroup + ".sql");
    }

    private void writeSqlDump(Path path, String sql) {
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(path, sql, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            uiLog.log("WARN", "Failed to dump SQL to " + path + ": " + ex.getMessage());
            LOGGER.warn("Failed to dump SQL to {}", path, ex);
        }
    }

    private String sanitizeFilePart(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private void updateStage(String stage) {
        progressListener.updateStage(stage);
    }

    private void updateProgress(int currentSegment, int totalSegments) {
        int percent = totalSegments == 0 ? 100 : (int) Math.round((currentSegment * 100.0) / totalSegments);
        progressListener.updateProgress(percent, "Segment " + currentSegment + "/" + totalSegments);
    }

    private void updateProgress(int percent, String message) {
        progressListener.updateProgress(percent, message);
    }

    private String safeString(String value) {
        return value == null ? "" : value;
    }

    private String resolveLockName() {
        String configured = config.getLockName();
        return configured.isBlank() ? "trans_data_job" : configured;
    }

    private String buildOwnerId(String runId) {
        String hostname = "unknown";
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
