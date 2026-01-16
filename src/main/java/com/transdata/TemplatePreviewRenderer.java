package com.transdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TemplatePreviewRenderer {
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([a-zA-Z0-9_]+)(\\?([a-zA-Z]+))?}");
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PreviewResult render(String insertTemplate,
                                List<JsonNode> records,
                                String taskId,
                                String runId,
                                String targetTable) {
        List<String> warnings = new ArrayList<>();
        if (insertTemplate == null || insertTemplate.isBlank()) {
            return new PreviewResult("", List.of("插入模板为空，无法预览"));
        }
        String[] parts = insertTemplate.split("=>", 2);
        if (parts.length < 2) {
            return new PreviewResult("", List.of("插入模板格式错误，无法预览"));
        }
        String[] columns = parts[0].trim().split("\\s*,\\s*");
        String[] values = parts[1].trim().split("\\s*,\\s*");
        if (columns.length != values.length) {
            warnings.add("列数量与值数量不一致，预览仅供参考");
        }
        int rows = Math.min(3, records == null ? 0 : records.size());
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ").append(targetTable).append(" (")
                .append(String.join(",", columns)).append(") VALUES\n");
        for (int i = 0; i < rows; i++) {
            if (i > 0) {
                builder.append(",\n");
            }
            JsonNode record = records.get(i);
            builder.append("(");
            for (int j = 0; j < values.length; j++) {
                if (j > 0) {
                    builder.append(",");
                }
                String expr = values[j];
                builder.append(renderValue(expr, record, taskId, runId, warnings));
            }
            builder.append(")");
        }
        if (rows == 0) {
            builder.append("(/* 无样本数据，无法生成示例值 */)");
            warnings.add("样本记录为空，预览无法展示真实数据");
        }
        builder.append(";");
        return new PreviewResult(builder.toString(), warnings);
    }

    private String renderValue(String expr, JsonNode record, String taskId, String runId, List<String> warnings) {
        String trimmed = expr.trim();
        if ("task_id".equalsIgnoreCase(trimmed)) {
            return quoteText(taskId);
        }
        if ("run_id".equalsIgnoreCase(trimmed)) {
            return quoteText(runId);
        }
        if ("payload".equalsIgnoreCase(trimmed)) {
            return toJsonbLiteral(record, warnings);
        }
        if ("now()".equalsIgnoreCase(trimmed)) {
            return "now()";
        }
        Matcher matcher = PLACEHOLDER.matcher(trimmed);
        if (!matcher.matches()) {
            return trimmed;
        }
        String field = matcher.group(1);
        String modifier = matcher.group(3);
        JsonNode value = record == null ? null : record.get(field);
        if (value == null || value.isNull()) {
            warnings.add("字段缺失或为空: " + field);
            return "NULL";
        }
        if (modifier == null || modifier.isBlank() || "text".equalsIgnoreCase(modifier)) {
            return quoteText(value.asText());
        }
        if ("num".equalsIgnoreCase(modifier)) {
            if (value.isNumber()) {
                return value.asText();
            }
            String text = value.asText();
            if (text.matches("-?\\d+(\\.\\d+)?")) {
                return text;
            }
            warnings.add("数值字段无法解析: " + field + "=" + text);
            return "NULL";
        }
        if ("bool".equalsIgnoreCase(modifier)) {
            if (value.isBoolean()) {
                return Boolean.toString(value.asBoolean());
            }
            String text = value.asText("").toLowerCase();
            if ("true".equals(text) || "false".equals(text)) {
                return text;
            }
            warnings.add("布尔字段无法解析: " + field + "=" + text);
            return "NULL";
        }
        if ("jsonb".equalsIgnoreCase(modifier)) {
            return toJsonbLiteral(value, warnings);
        }
        return quoteText(value.asText());
    }

    private String quoteText(String value) {
        if (value == null || value.isBlank()) {
            return "NULL";
        }
        return "'" + value.replace("'", "''") + "'";
    }

    private String toJsonbLiteral(JsonNode value, List<String> warnings) {
        if (value == null || value.isNull()) {
            warnings.add("JSONB 字段为空");
            return "NULL";
        }
        try {
            String json = objectMapper.writeValueAsString(value);
            return "'" + json.replace("'", "''") + "'::jsonb";
        } catch (Exception ex) {
            warnings.add("JSONB 序列化失败: " + ex.getMessage());
            return "NULL";
        }
    }

    public record PreviewResult(String previewSql, List<String> warnings) {
    }
}
