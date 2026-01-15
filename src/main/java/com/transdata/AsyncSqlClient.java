package com.transdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.CloseableHttpClient;
import org.apache.hc.client5.http.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.StringEntity;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncSqlClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSqlClient.class);
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
        URI uri = RemoteSqlConfig.buildUri(config.getAsyncBaseUrl(), "/jobs/submit");
        Map<String, Object> payload = new HashMap<>();
        payload.put("dbUser", config.getAsyncDbUser());
        payload.put("encryptedSql", encryptedSql);
        payload.put("keyFormat", config.getKeyFormat());
        String json = objectMapper.writeValueAsString(payload);

        JsonNode response = executePost(uri, config, json);
        SubmitResponse submitResponse = new SubmitResponse();
        submitResponse.setJobId(response.path("jobId").asText(""));
        submitResponse.setStatus(response.path("status").asText(""));
        submitResponse.setErrorMessage(response.path("errorMessage").asText(""));
        submitResponse.setErrorPosition(response.path("errorPosition").asText(""));
        return submitResponse;
    }

    public StatusResponse pollStatus(AppConfig config, String jobId, UiLogSink uiLog, AtomicBoolean cancelled) throws Exception {
        long delay = 500;
        long maxDelay = 2000;
        while (!cancelled.get()) {
            StatusResponse status = status(config, jobId);
            uiLog.log("INFO", "Polling jobId=" + jobId + ", status=" + status.getStatus());
            if (status.isTerminal()) {
                return status;
            }
            Thread.sleep(delay);
            delay = Math.min(maxDelay, Math.round(delay * 1.5));
        }
        StatusResponse cancelledResponse = new StatusResponse();
        cancelledResponse.setJobId(jobId);
        cancelledResponse.setStatus("CANCELLED");
        return cancelledResponse;
    }

    public StatusResponse status(AppConfig config, String jobId) throws Exception {
        URI uri = RemoteSqlConfig.buildUri(config.getAsyncBaseUrl(), "/jobs/status");
        Map<String, Object> payload = new HashMap<>();
        payload.put("jobId", jobId);
        String json = objectMapper.writeValueAsString(payload);
        JsonNode response = executePost(uri, config, json);
        StatusResponse statusResponse = new StatusResponse();
        statusResponse.setJobId(jobId);
        statusResponse.setStatus(response.path("status").asText(""));
        statusResponse.setErrorMessage(response.path("errorMessage").asText(""));
        statusResponse.setErrorPosition(response.path("errorPosition").asText(""));
        statusResponse.setRowsAffected(response.path("rowsAffected").asInt(-1));
        statusResponse.setActualRows(response.path("actualRows").asInt(-1));
        return statusResponse;
    }

    public ResultResponse result(AppConfig config, String jobId) throws Exception {
        URI uri = RemoteSqlConfig.buildUri(config.getAsyncBaseUrl(), "/jobs/result");
        Map<String, Object> payload = new HashMap<>();
        payload.put("jobId", jobId);
        String json = objectMapper.writeValueAsString(payload);
        JsonNode response = executePost(uri, config, json);
        ResultResponse resultResponse = new ResultResponse();
        resultResponse.setJobId(jobId);
        resultResponse.setRowsAffected(response.path("rowsAffected").asInt(-1));
        resultResponse.setActualRows(response.path("actualRows").asInt(-1));
        resultResponse.setErrorMessage(response.path("errorMessage").asText(""));
        resultResponse.setErrorPosition(response.path("errorPosition").asText(""));
        return resultResponse;
    }

    private JsonNode executePost(URI uri, AppConfig config, String json) throws Exception {
        HttpPost post = new HttpPost(uri);
        post.setHeader("Content-Type", "application/json;charset=UTF-8");
        if (!config.getAsyncToken().isBlank()) {
            post.setHeader("X-Request-Token", config.getAsyncToken());
        }
        post.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
        post.setConnectTimeout(connectTimeout);
        post.setResponseTimeout(responseTimeout);

        try (CloseableHttpResponse response = httpClient.execute(post)) {
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            LOGGER.info("Async SQL response received from {}", uri.getPath());
            return objectMapper.readTree(responseBody);
        }
    }

    public static class SubmitResponse {
        private String jobId;
        private String status;
        private String errorMessage;
        private String errorPosition;

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
    }

    public static class StatusResponse {
        private String jobId;
        private String status;
        private String errorMessage;
        private String errorPosition;
        private int rowsAffected;
        private int actualRows;

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

        public int getRowsAffected() {
            return rowsAffected;
        }

        public void setRowsAffected(int rowsAffected) {
            this.rowsAffected = rowsAffected;
        }

        public int getActualRows() {
            return actualRows;
        }

        public void setActualRows(int actualRows) {
            this.actualRows = actualRows;
        }

        public boolean isTerminal() {
            return "SUCCEEDED".equalsIgnoreCase(status)
                    || "FAILED".equalsIgnoreCase(status)
                    || "CANCELLED".equalsIgnoreCase(status);
        }
    }

    public static class ResultResponse {
        private String jobId;
        private int rowsAffected;
        private int actualRows;
        private String errorMessage;
        private String errorPosition;

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public int getRowsAffected() {
            return rowsAffected;
        }

        public void setRowsAffected(int rowsAffected) {
            this.rowsAffected = rowsAffected;
        }

        public int getActualRows() {
            return actualRows;
        }

        public void setActualRows(int actualRows) {
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
    }
}
