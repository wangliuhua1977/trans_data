package com.transdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransferJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransferJob.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int SEGMENT_SIZE = 100;
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
                updateStage("Cancelled");
                return;
            }

            updateStage("Grouping");
            Map<GroupKey, List<SourceRecord>> grouped = groupRecords(records);
            stats.setTotalGroups(grouped.size());
            progressListener.updateStats(stats);
            uiLog.log("INFO", "Grouped into " + grouped.size() + " groups by (date_no, datetime). requestId=" + requestId);

            GroupKey firstKey = grouped.keySet().iterator().next();
            String firstKeyPolicy = "appearance order";
            uiLog.log("INFO", "First groupKey policy=" + firstKeyPolicy + ", firstKey=" + firstKey
                    + ", requestId=" + requestId);

            updateStage("Checking");
            Boolean exists = runExistenceCheck(firstKey, stats, requestId);
            if (exists == null) {
                return;
            }
            if (exists) {
                uiLog.log("INFO", "Existing records found for (date_no, datetime). Skipping this run and waiting for next schedule.");
                updateStage("Skipped");
                updateProgress(100, "Skipped due to existing records");
                return;
            }

            List<SourceRecord> orderedRecords = flattenRecords(grouped);
            int totalSegments = calculateTotalSegments(orderedRecords.size());
            stats.setTotalBatches(totalSegments);
            stats.setCurrentBatch(0);
            progressListener.updateStats(stats);
            uiLog.log("INFO", "Run start: totalRecords=" + orderedRecords.size()
                    + ", totalSegments=" + totalSegments
                    + ", firstKey=" + firstKey
                    + ", firstKeyPolicy=" + firstKeyPolicy
                    + ", existenceCheckResult=NOT_EXISTS"
                    + ", requestId=" + requestId);

            List<String> segments = sqlBuilder.buildInsertSqlSegments(orderedRecords, uiLog);
            for (int i = 0; i < segments.size(); i++) {
                if (cancelled.get()) {
                    uiLog.log("WARN", "Job cancelled before submitting segment.");
                    updateStage("Cancelled");
                    return;
                }
                int segmentIndex = i + 1;
                stats.setCurrentBatch(segmentIndex);
                progressListener.updateStats(stats);
                String segmentSql = segments.get(i);
                int segmentRows = Math.min(SEGMENT_SIZE, orderedRecords.size() - (segmentIndex - 1) * SEGMENT_SIZE);
                uiLog.log("INFO", "Segment " + segmentIndex + "/" + totalSegments + ", rows=" + segmentRows
                        + ", requestId=" + requestId);

                updateStage("Writing");
                logPlainSql(segmentSql, requestId, firstKey, "segment_" + segmentIndex + "_of_" + totalSegments,
                        segmentRows, totalSegments);

                updateStage("Encrypting");
                String encryptedSql = encryptSql(segmentSql);

                updateStage("Submitting");
                AsyncSqlClient.SubmitResponse submitResponse = asyncSqlClient.submit(config, encryptedSql, uiLog, requestId);
                String jobId = submitResponse.getJobId();
                stats.setJobId(jobId);
                progressListener.updateStats(stats);
                uiLog.log("INFO", "Submitted jobId=" + jobId + ", status=" + submitResponse.getStatus()
                        + ", requestId=" + requestId);

                updateStage("Polling");
                AsyncSqlClient.StatusResponse status = asyncSqlClient.pollStatus(
                        config, jobId, uiLog, progressListener, cancelled, requestId);
                if (handleTerminalStatus(status, jobId, requestId, "Segment " + segmentIndex + "/" + totalSegments)) {
                    return;
                }

                logSuccessSummary(status, jobId, requestId);
                updateProgress(segmentIndex, totalSegments);
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

    private List<SourceRecord> flattenRecords(Map<GroupKey, List<SourceRecord>> grouped) {
        List<SourceRecord> ordered = new ArrayList<>();
        for (List<SourceRecord> groupRecords : grouped.values()) {
            ordered.addAll(groupRecords);
        }
        return ordered;
    }

    private int calculateTotalSegments(int totalRecords) {
        return (int) Math.ceil((double) totalRecords / SEGMENT_SIZE);
    }

    private Boolean runExistenceCheck(GroupKey firstKey, TransferStats stats, String requestId) throws Exception {
        if (cancelled.get()) {
            uiLog.log("WARN", "Job cancelled before existence check.");
            updateStage("Cancelled");
            return null;
        }
        String checkSql = sqlBuilder.buildExistenceCheckSql(firstKey, uiLog);
        logPlainSql(checkSql, requestId, firstKey, "existence_check", 1, 1);

        updateStage("Encrypting");
        String encryptedSql = encryptSql(checkSql);

        updateStage("Submitting");
        AsyncSqlClient.SubmitResponse submitResponse = asyncSqlClient.submit(config, encryptedSql, uiLog, requestId);
        String jobId = submitResponse.getJobId();
        stats.setJobId(jobId);
        progressListener.updateStats(stats);
        uiLog.log("INFO", "Submitted existence check jobId=" + jobId + ", status=" + submitResponse.getStatus()
                + ", requestId=" + requestId);

        updateStage("Polling");
        AsyncSqlClient.StatusResponse status = asyncSqlClient.pollStatus(
                config, jobId, uiLog, progressListener, cancelled, requestId);
        if (handleTerminalStatus(status, jobId, requestId, "Existence check")) {
            return null;
        }

        AsyncSqlClient.ResultResponse result = asyncSqlClient.result(config, jobId, uiLog, requestId);
        boolean exists = resolveExistence(status, result, requestId);
        uiLog.log("INFO", "Existence check result: " + (exists ? "EXISTS" : "NOT_EXISTS")
                + ", jobId=" + jobId + ", requestId=" + requestId);
        return exists;
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

    private void logSuccessSummary(AsyncSqlClient.StatusResponse status, String jobId, String requestId) throws Exception {
        boolean hasRowsSummary = status.getRowsAffected() != null
                || status.getUpdatedRows() != null
                || status.getAffectedRows() != null
                || status.getActualRows() != null;
        if (!hasRowsSummary) {
            AsyncSqlClient.ResultResponse result = asyncSqlClient.result(config, jobId, uiLog, requestId);
            uiLog.log("INFO", "Job succeeded. jobId=" + jobId
                    + ", rowsAffected=" + result.getRowsAffected()
                    + ", actualRows=" + result.getActualRows()
                    + ", columns=" + result.getColumnsCount()
                    + ", requestId=" + requestId);
        } else {
            uiLog.log("INFO", "Job succeeded. jobId=" + jobId
                    + ", rowsAffected=" + status.getRowsAffected()
                    + ", updatedRows=" + status.getUpdatedRows()
                    + ", affectedRows=" + status.getAffectedRows()
                    + ", actualRows=" + status.getActualRows()
                    + ", elapsed=" + status.getElapsedMillis()
                    + ", requestId=" + requestId);
        }
    }

    private boolean resolveExistence(AsyncSqlClient.StatusResponse status,
                                     AsyncSqlClient.ResultResponse result,
                                     String requestId) {
        Integer count = firstNonNull(status.getRowsAffected(),
                status.getUpdatedRows(),
                status.getAffectedRows(),
                status.getActualRows(),
                result.getRowsAffected(),
                result.getActualRows());
        if (count != null) {
            return count > 0;
        }
        String rawJson = result.getRawJson();
        if (rawJson == null || rawJson.isBlank()) {
            return false;
        }
        try {
            JsonNode root = OBJECT_MAPPER.readTree(rawJson);
            Integer rowCount = extractNumeric(root, "rowCount", "rowsAffected", "actualRows", "count");
            if (rowCount != null) {
                return rowCount > 0;
            }
            if (hasRowsArray(root)) {
                return true;
            }
        } catch (Exception ex) {
            uiLog.log("WARN", "Failed to parse existence check result JSON: " + ex.getMessage()
                    + ", requestId=" + requestId);
        }
        return false;
    }

    private boolean hasRowsArray(JsonNode node) {
        if (node == null || node.isMissingNode()) {
            return false;
        }
        JsonNode rows = node.path("rows");
        if (rows.isArray() && rows.size() > 0) {
            return true;
        }
        JsonNode data = node.path("data");
        if (data.isArray() && data.size() > 0) {
            return true;
        }
        if (data.isObject()) {
            JsonNode dataRows = data.path("rows");
            if (dataRows.isArray() && dataRows.size() > 0) {
                return true;
            }
            JsonNode dataData = data.path("data");
            return dataData.isArray() && dataData.size() > 0;
        }
        return false;
    }

    private Integer extractNumeric(JsonNode node, String... fields) {
        for (String field : fields) {
            JsonNode value = node.path(field);
            if (value.isNumber()) {
                return value.asInt();
            }
            if (value.isTextual()) {
                try {
                    return Integer.parseInt(value.asText().trim());
                } catch (NumberFormatException ignored) {
                    // continue
                }
            }
        }
        return null;
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

    private String encryptSql(String sql) throws Exception {
        byte[] keyBytes = CryptoUtil.decodeKey(config.getAesKey(), config.getKeyFormat());
        byte[] ivBytes = CryptoUtil.decodeKey(config.getAesIv(), config.getKeyFormat());
        return CryptoUtil.encrypt(sql, keyBytes, ivBytes);
    }

    private void logPlainSql(String sql,
                             String requestId,
                             GroupKey key,
                             String contextTag,
                             int records,
                             int totalSegments) {
        String targetTable = sqlBuilder.getTargetTable();
        uiLog.log("INFO", "Prepared SQL for submit. requestId=" + requestId
                + ", context=" + contextTag
                + ", groupKey=" + key
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
        String safeGroup = sanitizeFilePart(key.getDateNo() + "_" + key.getDatetime());
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

    private String safeString(String value) {
        return value == null ? "" : value;
    }
}
