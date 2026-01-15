package com.transdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncSqlClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSqlClient.class);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Timeout connectTimeout;
    private final Timeout responseTimeout;

    public AsyncSqlClient(CloseableHttpClient httpClient, Timeout connectTimeout, Timeout responseTimeout) {
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
        this.connectTimeout = connectTimeout;
        this.responseTimeout = responseTimeout;
    }

    public SubmitResponse submit(AppConfig config, String encryptedSql) throws Exception {
        return submit(config, encryptedSql, null, null);
    }

    public SubmitResponse submit(AppConfig config, String encryptedSql, UiLogSink uiLog, String requestId) throws Exception {
        URI uri = RemoteSqlConfig.buildUri(config.getAsyncBaseUrl(), "/jobs/submit");
        Map<String, Object> payload = new HashMap<>();
        payload.put("dbUser", config.getAsyncDbUser());
        payload.put("encryptedSql", encryptedSql);
        payload.put("keyFormat", config.getKeyFormat());
        String json = objectMapper.writeValueAsString(payload);

        ResponsePayload responsePayload = executePost(uri, config, json);
        JsonNode response = responsePayload.json();
        SubmitResponse submitResponse = new SubmitResponse();
        submitResponse.setJobId(textOrEmpty(response, "jobId"));
        submitResponse.setStatus(textOrEmpty(response, "status"));
        submitResponse.setErrorMessage(textOrEmpty(response, "errorMessage"));
        submitResponse.setErrorPosition(textOrEmpty(response, "errorPosition"));
        submitResponse.setServerTime(textOrEmpty(response, "serverTime"));
        submitResponse.setExecuteTime(textOrEmpty(response, "executeTime"));
        submitResponse.setRawJson(responsePayload.rawJson());
        if (uiLog != null) {
            String truncated = JsonUtil.safeTruncate(responsePayload.rawJson(), config.getLoggingSqlMaxChars());
            uiLog.log("INFO", "Async SQL submit response: jobId=" + submitResponse.getJobId()
                    + ", status=" + submitResponse.getStatus()
                    + ", serverTime=" + submitResponse.getServerTime()
                    + ", executeTime=" + submitResponse.getExecuteTime()
                    + ", requestId=" + safe(requestId)
                    + ", rawJson=" + truncated);
        }
        return submitResponse;
    }

    public StatusResponse pollStatus(AppConfig config,
                                     String jobId,
                                     UiLogSink uiLog,
                                     ProgressListener progressListener,
                                     AtomicBoolean cancelled,
                                     String requestId) throws Exception {
        long delay = 500;
        long maxDelay = 2000;
        Instant startedAt = Instant.now();
        String lastLoggedStatus = null;
        int maxWaitSeconds = Math.max(1, config.getAsyncMaxWaitSeconds());
        while (!cancelled.get()) {
            StatusResponse status = status(config, jobId);
            status.setElapsedMillis(Duration.between(startedAt, Instant.now()).toMillis());
            updatePolling(progressListener, status);
            if (lastLoggedStatus == null) {
                if (uiLog != null) {
                    uiLog.log("INFO", "Polling started: jobId=" + jobId
                            + ", initialStatus=" + status.getStatus()
                            + ", requestId=" + safe(requestId)
                            + ", startedAt=" + TIME_FORMATTER.format(startedAt));
                }
                lastLoggedStatus = status.getStatus();
            } else if (!equalsIgnoreCase(lastLoggedStatus, status.getStatus())) {
                if (uiLog != null) {
                    uiLog.log("INFO", "Polling status changed: jobId=" + jobId
                            + ", status=" + status.getStatus()
                            + ", elapsed=" + formatMillis(status.getElapsedMillis())
                            + ", progress=" + formatProgress(status.getProgressPercent()));
                }
                lastLoggedStatus = status.getStatus();
            }
            if (status.isTerminal()) {
                logPollingFinished(uiLog, status, requestId, config.getLoggingSqlMaxChars());
                return status;
            }
            if (status.getElapsedMillis() > maxWaitSeconds * 1000L) {
                StatusResponse timeout = new StatusResponse();
                timeout.setJobId(jobId);
                timeout.setStatus("TIMEOUT");
                timeout.setErrorMessage("Polling exceeded maxWaitSeconds=" + maxWaitSeconds);
                timeout.setElapsedMillis(status.getElapsedMillis());
                logPollingFinished(uiLog, timeout, requestId, config.getLoggingSqlMaxChars());
                return timeout;
            }
            try {
                Thread.sleep(delay);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                StatusResponse cancelledResponse = new StatusResponse();
                cancelledResponse.setJobId(jobId);
                cancelledResponse.setStatus("CANCELLED_LOCAL");
                cancelledResponse.setErrorMessage("Polling interrupted by stop()");
                cancelledResponse.setElapsedMillis(Duration.between(startedAt, Instant.now()).toMillis());
                logPollingFinished(uiLog, cancelledResponse, requestId, config.getLoggingSqlMaxChars());
                return cancelledResponse;
            }
            delay = Math.min(maxDelay, Math.round(delay * 1.5));
        }
        StatusResponse cancelledResponse = new StatusResponse();
        cancelledResponse.setJobId(jobId);
        cancelledResponse.setStatus("CANCELLED_LOCAL");
        cancelledResponse.setErrorMessage("Cancelled by user");
        cancelledResponse.setElapsedMillis(Duration.between(startedAt, Instant.now()).toMillis());
        logPollingFinished(uiLog, cancelledResponse, requestId, config.getLoggingSqlMaxChars());
        return cancelledResponse;
    }

    public StatusResponse status(AppConfig config, String jobId) throws Exception {
        URI uri = RemoteSqlConfig.buildUri(config.getAsyncBaseUrl(), "/jobs/status");
        Map<String, Object> payload = new HashMap<>();
        payload.put("jobId", jobId);
        String json = objectMapper.writeValueAsString(payload);
        ResponsePayload responsePayload = executePost(uri, config, json);
        JsonNode response = responsePayload.json();
        StatusResponse statusResponse = new StatusResponse();
        statusResponse.setJobId(jobId);
        statusResponse.setStatus(textOrEmpty(response, "status"));
        statusResponse.setErrorMessage(textOrEmpty(response, "errorMessage", "message"));
        statusResponse.setErrorPosition(textOrEmpty(response, "errorPosition", "position"));
        statusResponse.setSqlState(textOrEmpty(response, "sqlState"));
        statusResponse.setTraceId(textOrEmpty(response, "traceId", "traceID"));
        statusResponse.setStackTrace(textOrEmpty(response, "stackTrace", "stacktrace"));
        statusResponse.setRowsAffected(intOrNull(response, "rowsAffected", "updatedRows", "affectedRows"));
        statusResponse.setActualRows(intOrNull(response, "actualRows"));
        statusResponse.setUpdatedRows(intOrNull(response, "updatedRows"));
        statusResponse.setAffectedRows(intOrNull(response, "affectedRows"));
        statusResponse.setProgressPercent(intOrNull(response, "progressPercent", "progress"));
        statusResponse.setElapsedMillis(longOrNull(response, "elapsedMillis", "elapsedMs"));
        statusResponse.setServerTime(textOrEmpty(response, "serverTime"));
        statusResponse.setExecuteTime(textOrEmpty(response, "executeTime"));
        statusResponse.setRawJson(responsePayload.rawJson());
        return statusResponse;
    }

    public ResultResponse result(AppConfig config, String jobId) throws Exception {
        return result(config, jobId, null, null);
    }

    public ResultResponse result(AppConfig config, String jobId, UiLogSink uiLog, String requestId) throws Exception {
        URI uri = RemoteSqlConfig.buildUri(config.getAsyncBaseUrl(), "/jobs/result");
        Map<String, Object> payload = new HashMap<>();
        payload.put("jobId", jobId);
        String json = objectMapper.writeValueAsString(payload);
        ResponsePayload responsePayload = executePost(uri, config, json);
        JsonNode response = responsePayload.json();
        ResultResponse resultResponse = new ResultResponse();
        resultResponse.setJobId(jobId);
        resultResponse.setRowsAffected(intOrNull(response, "rowsAffected", "updatedRows", "affectedRows", "rowCount"));
        resultResponse.setActualRows(intOrNull(response, "actualRows"));
        resultResponse.setErrorMessage(textOrEmpty(response, "errorMessage", "message"));
        resultResponse.setErrorPosition(textOrEmpty(response, "errorPosition", "position"));
        resultResponse.setSqlState(textOrEmpty(response, "sqlState"));
        resultResponse.setTraceId(textOrEmpty(response, "traceId", "traceID"));
        resultResponse.setStackTrace(textOrEmpty(response, "stackTrace", "stacktrace"));
        resultResponse.setColumnsCount(columnsCount(response));
        resultResponse.setRawJson(responsePayload.rawJson());
        if (uiLog != null) {
            String truncated = JsonUtil.safeTruncate(responsePayload.rawJson(), config.getLoggingSqlMaxChars());
            uiLog.log("INFO", "Async SQL result response: jobId=" + jobId
                    + ", rowsAffected=" + valueOrDash(resultResponse.getRowsAffected())
                    + ", actualRows=" + valueOrDash(resultResponse.getActualRows())
                    + ", columns=" + valueOrDash(resultResponse.getColumnsCount())
                    + ", requestId=" + safe(requestId)
                    + ", rawJson=" + truncated);
        }
        return resultResponse;
    }

    private ResponsePayload executePost(URI uri, AppConfig config, String json) throws Exception {
        HttpPost post = new HttpPost(uri);
        post.setHeader("Content-Type", "application/json;charset=UTF-8");
        if (!config.getAsyncToken().isBlank()) {
            post.setHeader("X-Request-Token", config.getAsyncToken());
        }
        post.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setResponseTimeout(responseTimeout)
                .build();
        post.setConfig(requestConfig);

        try (CloseableHttpResponse response = httpClient.execute(post)) {
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            LOGGER.info("Async SQL response received from {}", uri.getPath());
            JsonNode jsonNode = readJson(responseBody);
            return new ResponsePayload(responseBody, jsonNode);
        }
    }

    private JsonNode readJson(String responseBody) {
        try {
            return objectMapper.readTree(responseBody);
        } catch (Exception ex) {
            LOGGER.warn("Failed to parse async SQL response JSON: {}", ex.getMessage());
            return objectMapper.createObjectNode();
        }
    }

    private void updatePolling(ProgressListener progressListener, StatusResponse status) {
        if (progressListener != null) {
            progressListener.updatePolling(status.getStatus(), status.getElapsedMillis(), status.getProgressPercent());
        }
    }

    private void logPollingFinished(UiLogSink uiLog, StatusResponse status, String requestId, int maxChars) {
        if (uiLog == null) {
            return;
        }
        StringBuilder builder = new StringBuilder();
        builder.append("Polling finished: jobId=").append(status.getJobId())
                .append(", status=").append(status.getStatus())
                .append(", elapsed=").append(formatMillis(status.getElapsedMillis()));
        if (status.isSucceeded()) {
            builder.append(", rowsAffected=").append(valueOrDash(status.getRowsAffected()))
                    .append(", progress=").append(formatProgress(status.getProgressPercent()));
        } else {
            String errorMessage = safeString(status.getErrorMessage());
            String errorPosition = safeString(status.getErrorPosition());
            String sqlState = safeString(status.getSqlState());
            String traceId = safeString(status.getTraceId());
            if (!errorMessage.isBlank()) {
                builder.append(", errorMessage=").append(errorMessage);
            }
            if (!errorPosition.isBlank()) {
                builder.append(", errorPosition=").append(errorPosition);
            }
            if (!sqlState.isBlank()) {
                builder.append(", sqlState=").append(sqlState);
            }
            if (!traceId.isBlank()) {
                builder.append(", traceId=").append(traceId);
            }
        }
        builder.append(", requestId=").append(safe(requestId));
        uiLog.log("INFO", builder.toString());
        if (status.getRawJson() != null && !status.getRawJson().isBlank()) {
            uiLog.log("INFO", "Async SQL status response: jobId=" + status.getJobId()
                    + ", rawJson=" + JsonUtil.safeTruncate(status.getRawJson(), maxChars));
        }
    }

    private static boolean equalsIgnoreCase(String left, String right) {
        if (left == null && right == null) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        return left.equalsIgnoreCase(right);
    }

    private static String formatMillis(Long elapsedMillis) {
        if (elapsedMillis == null) {
            return "-";
        }
        return String.format("%.1fs", elapsedMillis / 1000.0);
    }

    private static String formatProgress(Integer progress) {
        if (progress == null || progress < 0) {
            return "-";
        }
        return progress + "%";
    }

    private static String safe(String value) {
        return value == null ? "" : value;
    }

    private static String safeString(String value) {
        return value == null ? "" : value;
    }

    private static String valueOrDash(Number value) {
        return value == null ? "-" : value.toString();
    }

    private static String textOrEmpty(JsonNode node, String... fields) {
        for (String field : fields) {
            JsonNode value = node.get(field);
            if (value != null && !value.isNull()) {
                String text = value.asText("");
                if (!text.isBlank()) {
                    return text;
                }
            }
        }
        return "";
    }

    private static Integer intOrNull(JsonNode node, String... fields) {
        for (String field : fields) {
            JsonNode value = node.get(field);
            if (value != null && value.isNumber()) {
                return value.asInt();
            }
            if (value != null && value.isTextual()) {
                try {
                    return Integer.parseInt(value.asText().trim());
                } catch (NumberFormatException ignored) {
                    // ignore invalid
                }
            }
        }
        return null;
    }

    private static Long longOrNull(JsonNode node, String... fields) {
        for (String field : fields) {
            JsonNode value = node.get(field);
            if (value != null && value.isNumber()) {
                return value.asLong();
            }
            if (value != null && value.isTextual()) {
                try {
                    return Long.parseLong(value.asText().trim());
                } catch (NumberFormatException ignored) {
                    // ignore invalid
                }
            }
        }
        return null;
    }

    private static Integer columnsCount(JsonNode node) {
        JsonNode columns = node.get("columns");
        if (columns != null && columns.isArray()) {
            return columns.size();
        }
        return null;
    }

    private record ResponsePayload(String rawJson, JsonNode json) {
    }

    public static class SubmitResponse {
        private String jobId;
        private String status;
        private String errorMessage;
        private String errorPosition;
        private String serverTime;
        private String executeTime;
        private String rawJson;

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public String getErrorPosition() {
            return errorPosition;
        }

        public void setErrorPosition(String errorPosition) {
            this.errorPosition = errorPosition;
        }

        public String getServerTime() {
            return serverTime;
        }

        public void setServerTime(String serverTime) {
            this.serverTime = serverTime;
        }

        public String getExecuteTime() {
            return executeTime;
        }

        public void setExecuteTime(String executeTime) {
            this.executeTime = executeTime;
        }

        public String getRawJson() {
            return rawJson;
        }

        public void setRawJson(String rawJson) {
            this.rawJson = rawJson;
        }
    }

    public static class StatusResponse {
        private String jobId;
        private String status;
        private String errorMessage;
        private String errorPosition;
        private Integer rowsAffected;
        private Integer actualRows;
        private Integer updatedRows;
        private Integer affectedRows;
        private Integer progressPercent;
        private Long elapsedMillis;
        private String sqlState;
        private String traceId;
        private String stackTrace;
        private String serverTime;
        private String executeTime;
        private String rawJson;

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public String getErrorPosition() {
            return errorPosition;
        }

        public void setErrorPosition(String errorPosition) {
            this.errorPosition = errorPosition;
        }

        public Integer getRowsAffected() {
            return rowsAffected;
        }

        public void setRowsAffected(Integer rowsAffected) {
            this.rowsAffected = rowsAffected;
        }

        public Integer getActualRows() {
            return actualRows;
        }

        public void setActualRows(Integer actualRows) {
            this.actualRows = actualRows;
        }

        public Integer getUpdatedRows() {
            return updatedRows;
        }

        public void setUpdatedRows(Integer updatedRows) {
            this.updatedRows = updatedRows;
        }

        public Integer getAffectedRows() {
            return affectedRows;
        }

        public void setAffectedRows(Integer affectedRows) {
            this.affectedRows = affectedRows;
        }

        public Integer getProgressPercent() {
            return progressPercent;
        }

        public void setProgressPercent(Integer progressPercent) {
            this.progressPercent = progressPercent;
        }

        public Long getElapsedMillis() {
            return elapsedMillis;
        }

        public void setElapsedMillis(Long elapsedMillis) {
            this.elapsedMillis = elapsedMillis;
        }

        public String getSqlState() {
            return sqlState;
        }

        public void setSqlState(String sqlState) {
            this.sqlState = sqlState;
        }

        public String getTraceId() {
            return traceId;
        }

        public void setTraceId(String traceId) {
            this.traceId = traceId;
        }

        public String getStackTrace() {
            return stackTrace;
        }

        public void setStackTrace(String stackTrace) {
            this.stackTrace = stackTrace;
        }

        public String getServerTime() {
            return serverTime;
        }

        public void setServerTime(String serverTime) {
            this.serverTime = serverTime;
        }

        public String getExecuteTime() {
            return executeTime;
        }

        public void setExecuteTime(String executeTime) {
            this.executeTime = executeTime;
        }

        public String getRawJson() {
            return rawJson;
        }

        public void setRawJson(String rawJson) {
            this.rawJson = rawJson;
        }

        public boolean isSucceeded() {
            return "SUCCEEDED".equalsIgnoreCase(status);
        }

        public boolean isTerminal() {
            return "SUCCEEDED".equalsIgnoreCase(status)
                    || "FAILED".equalsIgnoreCase(status)
                    || "CANCELLED".equalsIgnoreCase(status)
                    || "CANCELLED_LOCAL".equalsIgnoreCase(status)
                    || "TIMEOUT".equalsIgnoreCase(status);
        }
    }

    public static class ResultResponse {
        private String jobId;
        private Integer rowsAffected;
        private Integer actualRows;
        private String errorMessage;
        private String errorPosition;
        private String sqlState;
        private String traceId;
        private String stackTrace;
        private Integer columnsCount;
        private String rawJson;

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public Integer getRowsAffected() {
            return rowsAffected;
        }

        public void setRowsAffected(Integer rowsAffected) {
            this.rowsAffected = rowsAffected;
        }

        public Integer getActualRows() {
            return actualRows;
        }

        public void setActualRows(Integer actualRows) {
            this.actualRows = actualRows;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public String getErrorPosition() {
            return errorPosition;
        }

        public void setErrorPosition(String errorPosition) {
            this.errorPosition = errorPosition;
        }

        public String getSqlState() {
            return sqlState;
        }

        public void setSqlState(String sqlState) {
            this.sqlState = sqlState;
        }

        public String getTraceId() {
            return traceId;
        }

        public void setTraceId(String traceId) {
            this.traceId = traceId;
        }

        public String getStackTrace() {
            return stackTrace;
        }

        public void setStackTrace(String stackTrace) {
            this.stackTrace = stackTrace;
        }

        public Integer getColumnsCount() {
            return columnsCount;
        }

        public void setColumnsCount(Integer columnsCount) {
            this.columnsCount = columnsCount;
        }

        public String getRawJson() {
            return rawJson;
        }

        public void setRawJson(String rawJson) {
            this.rawJson = rawJson;
        }
    }
}
