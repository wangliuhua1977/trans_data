package com.transdata;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class JsonSchemaInfer {
    private static final Set<String> RESERVED = Set.of(
            "user", "order", "select", "where", "group", "table", "from", "to", "by",
            "insert", "update", "delete", "limit", "offset", "timestamp"
    );

    public List<FieldSpec> infer(List<JsonNode> records) {
        Map<String, TypeStats> statsMap = new HashMap<>();
        int total = records == null ? 0 : records.size();
        if (records != null) {
            for (JsonNode record : records) {
                if (record == null || record.isNull() || !record.isObject()) {
                    continue;
                }
                for (var it = record.fields(); it.hasNext(); ) {
                    var entry = it.next();
                    String key = entry.getKey();
                    JsonNode value = entry.getValue();
                    TypeStats stats = statsMap.computeIfAbsent(key, k -> new TypeStats());
                    stats.observe(value);
                }
                for (String key : statsMap.keySet()) {
                    TypeStats stats = statsMap.get(key);
                    stats.observeMissing(record.get(key));
                }
            }
        }
        List<FieldSpec> specs = new ArrayList<>();
        Set<String> used = new HashSet<>();
        used.add("task_id");
        used.add("task_run_id");
        used.add("created_at");
        used.add("payload");
        for (Map.Entry<String, TypeStats> entry : statsMap.entrySet()) {
            String key = entry.getKey();
            TypeStats stats = entry.getValue();
            if (stats.hasNested()) {
                continue;
            }
            PgType type = stats.inferType();
            String column = sanitizeColumnName(key, used);
            used.add(column);
            boolean nullable = stats.isNullable(total);
            specs.add(new FieldSpec(column, key, "$." + key, type, nullable));
        }
        return specs;
    }

    private String sanitizeColumnName(String key, Set<String> used) {
        String snake = toSnakeCase(key);
        if (snake.isBlank()) {
            snake = "col";
        }
        if (Character.isDigit(snake.charAt(0))) {
            snake = "col_" + snake;
        }
        if (RESERVED.contains(snake)) {
            snake = snake + "_col";
        }
        String base = snake;
        int suffix = 1;
        while (used.contains(snake)) {
            snake = base + "_" + suffix;
            suffix++;
        }
        return snake;
    }

    private String toSnakeCase(String key) {
        if (key == null) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        char prev = 0;
        for (int i = 0; i < key.length(); i++) {
            char ch = key.charAt(i);
            if (Character.isUpperCase(ch)) {
                if (i > 0 && prev != '_' && prev != '-') {
                    builder.append('_');
                }
                builder.append(Character.toLowerCase(ch));
            } else if (Character.isLetterOrDigit(ch)) {
                builder.append(Character.toLowerCase(ch));
            } else {
                if (builder.length() > 0 && builder.charAt(builder.length() - 1) != '_') {
                    builder.append('_');
                }
            }
            prev = ch;
        }
        String result = builder.toString().replaceAll("_+", "_");
        if (result.endsWith("_")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private static final class TypeStats {
        private int presentCount;
        private boolean nullable;
        private boolean hasNested;
        private boolean seenString;
        private boolean seenBoolean;
        private boolean seenNumber;
        private boolean seenDecimal;
        private boolean seenInteger;
        private boolean integerOverflow;

        void observe(JsonNode value) {
            if (value == null || value.isNull()) {
                nullable = true;
                return;
            }
            presentCount++;
            if (value.isObject() || value.isArray()) {
                hasNested = true;
                return;
            }
            if (value.isBoolean()) {
                seenBoolean = true;
                return;
            }
            if (value.isNumber()) {
                seenNumber = true;
                if (value.isIntegralNumber()) {
                    seenInteger = true;
                    if (!value.canConvertToLong()) {
                        integerOverflow = true;
                    }
                } else {
                    seenDecimal = true;
                }
                return;
            }
            seenString = true;
        }

        void observeMissing(JsonNode value) {
            if (value == null || value.isNull()) {
                nullable = true;
            }
        }

        boolean hasNested() {
            return hasNested;
        }

        boolean isNullable(int total) {
            return nullable || presentCount < total;
        }

        PgType inferType() {
            if (seenBoolean && !seenNumber && !seenString) {
                return PgType.BOOLEAN;
            }
            if (seenNumber && !seenString && !seenBoolean) {
                if (seenDecimal || integerOverflow) {
                    return PgType.NUMERIC;
                }
                if (seenInteger) {
                    return PgType.BIGINT;
                }
            }
            return PgType.TEXT;
        }
    }
}
