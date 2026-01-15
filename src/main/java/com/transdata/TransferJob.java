package com.transdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
        String requestId = java.util.UUID.randomUUID().toString();
        TransferStats stats = new TransferStats();
        try {
            if (!validateCrypto()) {
                updateStage("Failed");
                return;
            }

            updateStage("Fetching");
            uiLog.log("INFO", "Starting fetch from source. requestId=" + requestId);
            List<SourceRecord> records = sourceClient.fetch(config, uiLog);
            stats.setTotalRecords(records.size());
            progressListener.updateStats(stats);
            if (records.isEmpty()) {
                uiLog.log("INFO", "0 records fetched, no write needed.");
                updateStage("Done");
                return;
            }
            if (cancelled.get()) {
                uiLog.log("WARN", "Job cancelled after fetch.");
                updateStage("Failed");
                return;
            }

            updateStage("Grouping");
            Map<GroupKey, List<SourceRecord>> grouped = groupRecords(records);
            stats.setTotalGroups(grouped.size());
            progressListener.updateStats(stats);
            uiLog.log("INFO", "Grouped into " + grouped.size() + " groups by (date_no, datetime). requestId=" + requestId);

            int groupIndex = 0;
            for (Map.Entry<GroupKey, List<SourceRecord>> entry : grouped.entrySet()) {
                if (cancelled.get()) {
                    uiLog.log("WARN", "Job cancelled before processing group.");
                    updateStage("Failed");
                    return;
                }
                groupIndex++;
                GroupKey key = entry.getKey();
                List<SourceRecord> groupRecords = entry.getValue();
                stats.setCurrentGroup(groupIndex);
                stats.setTotalBatches(calculateTotalBatches(groupRecords.size(), config.getBatchSize()));
                stats.setCurrentBatch(0);
                progressListener.updateStats(stats);
                uiLog.log("INFO", "Processing group " + groupIndex + "/" + grouped.size()
                        + " key=" + key + ", records=" + groupRecords.size() + ", requestId=" + requestId);

                updateStage("Writing");
                String sql = buildSqlForGroup(key, groupRecords, stats);

                updateStage("Encrypting");
                String encryptedSql = encryptSql(sql);

                updateStage("Submitting");
                AsyncSqlClient.SubmitResponse submitResponse = asyncSqlClient.submit(config, encryptedSql);
                String jobId = submitResponse.getJobId();
                stats.setJobId(jobId);
                progressListener.updateStats(stats);
                uiLog.log("INFO", "Submitted jobId=" + jobId + ", status=" + submitResponse.getStatus()
                        + ", requestId=" + requestId);

                updateStage("Polling");
                AsyncSqlClient.StatusResponse status = asyncSqlClient.pollStatus(config, jobId, uiLog, cancelled);
                if (cancelled.get()) {
                    uiLog.log("WARN", "Job cancelled during polling. jobId=" + jobId);
                    updateStage("Failed");
                    return;
                }
                if (!"SUCCEEDED".equalsIgnoreCase(status.getStatus())) {
                    uiLog.log("ERROR", "Job failed. jobId=" + jobId
                            + ", status=" + status.getStatus()
                            + ", errorMessage=" + status.getErrorMessage()
                            + ", errorPosition=" + status.getErrorPosition()
                            + ", requestId=" + requestId);
                    updateStage("Failed");
                    return;
                }

                AsyncSqlClient.ResultResponse result = asyncSqlClient.result(config, jobId);
                uiLog.log("INFO", "Job succeeded. jobId=" + jobId
                        + ", rowsAffected=" + result.getRowsAffected()
                        + ", actualRows=" + result.getActualRows()
                        + ", requestId=" + requestId);

                updateProgress(groupIndex, grouped.size());
            }

            updateStage("Done");
            Duration duration = Duration.between(start, Instant.now());
            uiLog.log("INFO", "Job completed in " + duration.toMillis() + " ms, requestId=" + requestId);
        } catch (Exception ex) {
            updateStage("Failed");
            uiLog.log("ERROR", "Job failed: " + ex.getMessage() + ", requestId=" + requestId);
            LOGGER.error("Job failed", ex);
        }
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

    private int calculateTotalBatches(int totalRecords, int batchSize) {
        return (int) Math.ceil((double) totalRecords / batchSize);
    }

    private String buildSqlForGroup(GroupKey key, List<SourceRecord> records, TransferStats stats) {
        StringBuilder builder = new StringBuilder();
        builder.append("BEGIN;");
        builder.append(sqlBuilder.buildCreateTableSql());
        builder.append(sqlBuilder.buildCreateIndexSql());
        builder.append(sqlBuilder.buildDeleteSql(key));
        List<String> inserts = sqlBuilder.buildInsertSql(records, config.getBatchSize(), uiLog);
        for (int i = 0; i < inserts.size(); i++) {
            if (cancelled.get()) {
                break;
            }
            int batchIndex = i + 1;
            stats.setCurrentBatch(batchIndex);
            progressListener.updateStats(stats);
            uiLog.log("INFO", "Batch " + batchIndex + "/" + inserts.size() + " for group "
                    + stats.getCurrentGroup() + "/" + stats.getTotalGroups());
            builder.append(inserts.get(i));
        }
        builder.append("COMMIT;");
        return builder.toString();
    }

    private String encryptSql(String sql) throws Exception {
        byte[] keyBytes = CryptoUtil.decodeKey(config.getAesKey(), config.getKeyFormat());
        byte[] ivBytes = CryptoUtil.decodeKey(config.getAesIv(), config.getKeyFormat());
        return CryptoUtil.encrypt(sql, keyBytes, ivBytes);
    }

    private void updateStage(String stage) {
        progressListener.updateStage(stage);
    }

    private void updateProgress(int currentGroup, int totalGroups) {
        int percent = totalGroups == 0 ? 100 : (int) Math.round((currentGroup * 100.0) / totalGroups);
        progressListener.updateProgress(percent, "Group " + currentGroup + "/" + totalGroups);
    }
}
