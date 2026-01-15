package com.transdata;

import java.net.URI;

public final class RemoteSqlConfig {
    private RemoteSqlConfig() {
    }

    public static URI buildUri(String baseUrl, String path) {
        String configured = baseUrl == null ? "" : baseUrl.trim();
        String base = configured.endsWith("/") ? configured.substring(0, configured.length() - 1) : configured;
        String normalized = path.startsWith("/") ? path : "/" + path;
        return URI.create(base + normalized);
    }
}
