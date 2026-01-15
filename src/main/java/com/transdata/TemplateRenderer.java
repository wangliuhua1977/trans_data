package com.transdata;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TemplateRenderer {
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([a-zA-Z0-9_]+)(\\?([a-zA-Z]+))?}");

    public String renderValueExpression(String template) {
        if (template == null) {
            return "";
        }
        Matcher matcher = PLACEHOLDER.matcher(template);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String field = matcher.group(1);
            String modifier = matcher.group(3);
            String replacement = buildExpression(field, modifier);
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private String buildExpression(String field, String modifier) {
        String safeField = sanitizeField(field);
        if (modifier == null || modifier.isBlank() || "text".equalsIgnoreCase(modifier)) {
            return "NULLIF(payload->>'" + safeField + "','')";
        }
        if ("num".equalsIgnoreCase(modifier)) {
            return "NULLIF(payload->>'" + safeField + "','')::numeric";
        }
        if ("raw".equalsIgnoreCase(modifier)) {
            return "payload->>'" + safeField + "'";
        }
        return "NULLIF(payload->>'" + safeField + "','')";
    }

    private String sanitizeField(String field) {
        if (field == null || field.isBlank()) {
            throw new IllegalArgumentException("字段占位符为空");
        }
        if (!field.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("字段占位符不合法：" + field);
        }
        return field;
    }
}
