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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SourceClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceClient.class);
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Timeout connectTimeout;
    private final Timeout responseTimeout;

    public SourceClient(CloseableHttpClient httpClient, Timeout connectTimeout, Timeout responseTimeout) {
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
        this.connectTimeout = connectTimeout;
        this.responseTimeout = responseTimeout;
    }

    public List<SourceRecord> fetch(AppConfig config, UiLogSink uiLog) throws Exception {
        HttpPost post = buildPost(config.getSourceUrl(), config.getSourceBody(), config);

        try (CloseableHttpResponse response = httpClient.execute(post)) {
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            JsonNode root = objectMapper.readTree(responseBody);
            int code = root.path("code").asInt(-1);
            if (code != 1) {
                String message = root.path("message").asText("未知");
                uiLog.log("ERROR", "源接口返回异常：code=" + code + "，message=" + message);
                throw new IllegalStateException("源接口返回 code " + code + ": " + message);
            }
            JsonNode data = root.path("data");
            if (!data.isArray()) {
                uiLog.log("ERROR", "源接口响应缺少 data 数组。");
                throw new IllegalStateException("源接口 data 不是数组");
            }
            List<SourceRecord> records = new ArrayList<>();
            for (JsonNode node : data) {
                SourceRecord record = new SourceRecord();
                record.setOrderItemId(node.path("order_item_id").asText("").trim());
                record.setDateNo(node.path("date_no").asText("").trim());
                record.setObjId(node.path("obj_id").asText("").trim());
                record.setIndType(node.path("ind_type").asText("").trim());
                record.setAcceptStaffId(node.path("accept_staff_id").asText("").trim());
                record.setAcceptChannelId(node.path("accept_channel_id").asText("").trim());
                record.setFirstStaffId(node.path("first_staff_id").asText("").trim());
                record.setSecondStaffId(node.path("second_staff_id").asText("").trim());
                record.setDevStaffId(node.path("dev_staff_id").asText("").trim());
                record.setLevel5Id(node.path("level5_id").asText("").trim());
                record.setDatetime(node.path("datetime").asText("").trim());
                record.setAcceptDate(node.path("ACCEPT_DATE").asText("").trim());
                records.add(record);
            }
            LOGGER.info("已从源接口获取 {} 条记录。", records.size());
            return records;
        }
    }

    public SourceFetchResult fetchJson(TaskDefinition task, AppConfig config, UiLogSink uiLog) throws Exception {
        String body = task.getSourcePostBody();
        if (body == null || body.isBlank()) {
            body = config.getSourceBody();
        }
        HttpPost post = buildPost(task.getSourcePostUrl(), body, config);
        long start = System.currentTimeMillis();
        try (CloseableHttpResponse response = httpClient.execute(post)) {
            long elapsed = System.currentTimeMillis() - start;
            int statusCode = response.getCode();
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            int bytes = responseBody.getBytes(StandardCharsets.UTF_8).length;
            JsonNode root = objectMapper.readTree(responseBody);
            String pretty = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
            if (root.has("code")) {
                int code = root.path("code").asInt(-1);
                if (code != 1) {
                    String message = root.path("message").asText("未知");
                    uiLog.log("ERROR", "源接口返回异常：code=" + code + "，message=" + message);
                    throw new IllegalStateException("源接口返回 code " + code + ": " + message);
                }
            }
            List<JsonNode> records = JsonRecordLocator.locateRecords(root);
            return new SourceFetchResult(statusCode, elapsed, bytes, responseBody, pretty, records);
        }
    }

    public SourceFetchResult testPost(TaskDefinition task, AppConfig config) throws Exception {
        String body = task.getSourcePostBody();
        if (body == null || body.isBlank()) {
            body = config.getSourceBody();
        }
        HttpPost post = buildPost(task.getSourcePostUrl(), body, config);
        long start = System.currentTimeMillis();
        try (CloseableHttpResponse response = httpClient.execute(post)) {
            long elapsed = System.currentTimeMillis() - start;
            int statusCode = response.getCode();
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            int bytes = responseBody.getBytes(StandardCharsets.UTF_8).length;
            String pretty = responseBody;
            List<JsonNode> records = List.of();
            try {
                JsonNode root = objectMapper.readTree(responseBody);
                pretty = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
                records = JsonRecordLocator.locateRecords(root);
            } catch (Exception ignored) {
                // keep raw
            }
            return new SourceFetchResult(statusCode, elapsed, bytes, responseBody, pretty, records);
        }
    }

    private HttpPost buildPost(String url, String body, AppConfig config) {
        HttpPost post = new HttpPost(url);
        post.setHeader("x-app-id", config.getSourceAppId());
        if (!config.getSourceAppKey().isBlank()) {
            post.setHeader("EASY-APP-KEY", config.getSourceAppKey());
        }
        post.setEntity(new StringEntity(body == null ? "{}" : body, ContentType.APPLICATION_JSON));
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setResponseTimeout(responseTimeout)
                .build();
        post.setConfig(requestConfig);
        return post;
    }
}
