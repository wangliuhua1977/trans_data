package com.transdata;

public final class JsonUtil {
    private JsonUtil() {
    }

    public static String safeTruncate(String value, int maxChars) {
        if (value == null) {
            return "";
        }
        if (maxChars <= 0 || value.length() <= maxChars) {
            return value;
        }
        int head = Math.max(1, maxChars / 2);
        int tail = Math.max(1, maxChars - head);
        if (head + tail > value.length()) {
            return value;
        }
        String prefix = value.substring(0, head);
        String suffix = value.substring(value.length() - tail);
        int truncated = value.length() - head - tail;
        return prefix + "...(已截断 " + truncated + " 个字符)..." + suffix;
    }
}
