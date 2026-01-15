package com.transdata;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public class SourceFetchResult {
    private final int statusCode;
    private final long elapsedMillis;
    private final int responseBytes;
    private final String rawJson;
    private final String prettyJson;
    private final List<JsonNode> records;

    public SourceFetchResult(int statusCode,
                             long elapsedMillis,
                             int responseBytes,
                             String rawJson,
                             String prettyJson,
                             List<JsonNode> records) {
        this.statusCode = statusCode;
        this.elapsedMillis = elapsedMillis;
        this.responseBytes = responseBytes;
        this.rawJson = rawJson;
        this.prettyJson = prettyJson;
        this.records = records;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public long getElapsedMillis() {
        return elapsedMillis;
    }

    public int getResponseBytes() {
        return responseBytes;
    }

    public String getRawJson() {
        return rawJson;
    }

    public String getPrettyJson() {
        return prettyJson;
    }

    public List<JsonNode> getRecords() {
        return records;
    }
}
