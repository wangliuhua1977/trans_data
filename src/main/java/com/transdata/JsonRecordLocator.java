package com.transdata;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

public final class JsonRecordLocator {
    private JsonRecordLocator() {
    }

    public static List<JsonNode> locateRecords(JsonNode root) {
        if (root == null || root.isNull()) {
            return List.of();
        }
        List<JsonNode> direct = asArray(root);
        if (!direct.isEmpty()) {
            return direct;
        }
        JsonNode data = root.path("data");
        List<JsonNode> dataArray = asArray(data);
        if (!dataArray.isEmpty()) {
            return dataArray;
        }
        List<JsonNode> rowsArray = asArray(root.path("rows"));
        if (!rowsArray.isEmpty()) {
            return rowsArray;
        }
        List<JsonNode> resultRows = asArray(root.path("resultRows"));
        if (!resultRows.isEmpty()) {
            return resultRows;
        }
        List<JsonNode> dataRows = asArray(data.path("rows"));
        if (!dataRows.isEmpty()) {
            return dataRows;
        }
        List<JsonNode> dataData = asArray(data.path("data"));
        if (!dataData.isEmpty()) {
            return dataData;
        }
        List<JsonNode> resultRowsAlt = asArray(root.path("result").path("rows"));
        if (!resultRowsAlt.isEmpty()) {
            return resultRowsAlt;
        }
        List<JsonNode> resultDataAlt = asArray(root.path("result").path("data"));
        if (!resultDataAlt.isEmpty()) {
            return resultDataAlt;
        }
        return searchFirstArray(root);
    }

    private static List<JsonNode> asArray(JsonNode node) {
        if (node != null && node.isArray()) {
            List<JsonNode> records = new ArrayList<>();
            for (JsonNode item : node) {
                if (!item.isNull()) {
                    records.add(item);
                }
            }
            return records;
        }
        return List.of();
    }

    private static List<JsonNode> searchFirstArray(JsonNode root) {
        Deque<JsonNode> queue = new ArrayDeque<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            JsonNode current = queue.removeFirst();
            if (current == null || current.isNull()) {
                continue;
            }
            if (current.isArray()) {
                List<JsonNode> items = new ArrayList<>();
                for (JsonNode node : current) {
                    if (!node.isNull()) {
                        items.add(node);
                    }
                }
                if (!items.isEmpty() && items.get(0).isObject()) {
                    return items;
                }
            }
            if (current.isObject()) {
                for (Map.Entry<String, JsonNode> entry : iterable(current.fields())) {
                    queue.add(entry.getValue());
                }
            }
        }
        return List.of();
    }

    private static <T> Iterable<T> iterable(java.util.Iterator<T> iterator) {
        return () -> iterator;
    }
}
