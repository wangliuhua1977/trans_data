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
        HttpPost post = new HttpPost(config.getSourceUrl());
        post.setHeader("x-app-id", config.getSourceAppId());
        if (!config.getSourceAppKey().isBlank()) {
            post.setHeader("EASY-APP-KEY", config.getSourceAppKey());
        }
        post.setEntity(new StringEntity(config.getSourceBody(), ContentType.APPLICATION_JSON));
        post.setConnectTimeout(connectTimeout);
        post.setResponseTimeout(responseTimeout);

        try (CloseableHttpResponse response = httpClient.execute(post)) {
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            JsonNode root = objectMapper.readTree(responseBody);
            int code = root.path("code").asInt(-1);
            if (code != 1) {
                String message = root.path("message").asText("unknown");
                uiLog.log("ERROR", "Source responded with code=" + code + ", message=" + message);
                throw new IllegalStateException("Source returned code " + code + ": " + message);
            }
            JsonNode data = root.path("data");
            if (!data.isArray()) {
                uiLog.log("ERROR", "Source response missing data array.");
                throw new IllegalStateException("Source data is not array");
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
            LOGGER.info("Fetched {} records from source.", records.size());
            return records;
        }
    }
}
