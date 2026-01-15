package com.transdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class AppConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppConfig.class);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final String USER_CONFIG_DIR = ".trans_data";
    private static final String USER_CONFIG_FILE = "config.properties";

    private final Properties properties;
    private final Path userConfigPath;

    private AppConfig(Properties properties, Path userConfigPath) {
        this.properties = properties;
        this.userConfigPath = userConfigPath;
    }

    public static AppConfig load() {
        Properties properties = new Properties();
        try (InputStream inputStream = AppConfig.class.getResourceAsStream("/config.properties")) {
            if (inputStream != null) {
                try (InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
                    properties.load(reader);
                }
            }
        } catch (IOException ex) {
            LOGGER.warn("加载默认配置失败: {}", ex.getMessage());
        }

        Path userConfigPath = resolveUserConfigPath();
        if (Files.exists(userConfigPath)) {
            try (InputStream inputStream = Files.newInputStream(userConfigPath);
                 InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
                properties.load(reader);
            } catch (IOException ex) {
                LOGGER.warn("加载用户配置失败: {}", ex.getMessage());
            }
        }

        return new AppConfig(properties, userConfigPath);
    }

    private static Path resolveUserConfigPath() {
        String home = System.getProperty("user.home");
        Path base = home == null || home.isBlank() ? Paths.get(".") : Paths.get(home);
        return base.resolve(USER_CONFIG_DIR).resolve(USER_CONFIG_FILE);
    }

    public synchronized void save() {
        try {
            Files.createDirectories(userConfigPath.getParent());
            try (OutputStream outputStream = Files.newOutputStream(userConfigPath);
                 OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
                properties.store(writer, "trans_data 用户配置");
            }
        } catch (IOException ex) {
            LOGGER.warn("保存用户配置失败: {}", ex.getMessage());
        }
    }

    public synchronized String getString(String key) {
        return properties.getProperty(key, "").trim();
    }

    public synchronized void setString(String key, String value) {
        properties.setProperty(key, value == null ? "" : value.trim());
    }

    public synchronized boolean getBoolean(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.trim());
    }

    public synchronized void setBoolean(String key, boolean value) {
        properties.setProperty(key, Boolean.toString(value));
    }

    public synchronized int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    public synchronized void setInt(String key, int value) {
        properties.setProperty(key, Integer.toString(value));
    }

    public synchronized LocalTime getTime(String key, LocalTime defaultValue) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return LocalTime.parse(value.trim(), TIME_FORMATTER);
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    public synchronized void setTime(String key, LocalTime value) {
        properties.setProperty(key, value.format(TIME_FORMATTER));
    }

    public synchronized String getMasked(String key) {
        String raw = getString(key);
        if (raw.isBlank()) {
            return "";
        }
        if (raw.length() <= 4) {
            return "****";
        }
        return raw.substring(0, 2) + "****" + raw.substring(raw.length() - 2);
    }

    public synchronized String getSourceUrl() {
        return getString("source.url");
    }

    public synchronized String getSourceBody() {
        String body = getString("source.body");
        return body.isBlank() ? "{}" : body;
    }

    public synchronized String getSourceAppId() {
        return getString("source.header.x-app-id");
    }

    public synchronized String getSourceAppKey() {
        return getString("source.header.easy-app-key");
    }

    public synchronized String getAsyncBaseUrl() {
        return getString("asyncsql.baseUrl");
    }

    public synchronized String getAsyncToken() {
        return getString("asyncsql.token");
    }

    public synchronized String getAsyncDbUser() {
        return getString("asyncsql.dbUser");
    }

    public synchronized String getAesKey() {
        return getString("crypto.aesKey");
    }

    public synchronized String getAesIv() {
        return getString("crypto.aesIv");
    }

    public synchronized String getKeyFormat() {
        return getString("crypto.keyFormat");
    }

    public synchronized boolean isScheduleEnabled() {
        return getBoolean("schedule.enabled", true);
    }

    public synchronized void setScheduleEnabled(boolean enabled) {
        setBoolean("schedule.enabled", enabled);
    }

    public synchronized int getIntervalSeconds() {
        return getInt("schedule.intervalSeconds", 180);
    }

    public synchronized void setIntervalSeconds(int seconds) {
        setInt("schedule.intervalSeconds", seconds);
    }

    public synchronized LocalTime getWindowStart() {
        return getTime("schedule.windowStart", LocalTime.MIDNIGHT);
    }

    public synchronized void setWindowStart(LocalTime value) {
        setTime("schedule.windowStart", value);
    }

    public synchronized LocalTime getWindowEnd() {
        return getTime("schedule.windowEnd", LocalTime.of(23, 59, 59));
    }

    public synchronized void setWindowEnd(LocalTime value) {
        setTime("schedule.windowEnd", value);
    }

    public synchronized int getBatchSize() {
        return getInt("schedule.batchSize", 100);
    }

    public synchronized void setBatchSize(int batchSize) {
        setInt("schedule.batchSize", batchSize);
    }

    public synchronized String getLockName() {
        return getString("lock.name");
    }

    public synchronized int getLockLeaseSeconds() {
        return getInt("lock.leaseSeconds", 300);
    }

    public synchronized int getJobMaxRetries() {
        return getInt("job.maxRetries", 3);
    }

    public synchronized boolean isHttpsInsecure() {
        return getBoolean("https.insecure", true);
    }

    public synchronized void setHttpsInsecure(boolean insecure) {
        setBoolean("https.insecure", insecure);
    }

    public synchronized int getLoggingSqlMaxChars() {
        return getInt("logging.sql.maxChars", 20000);
    }

    public synchronized String getLoggingSqlDumpDir() {
        String value = getString("logging.sql.dumpDir");
        return value.isBlank() ? "logs/sql" : value;
    }

    public synchronized int getAsyncMaxWaitSeconds() {
        return getInt("asyncsql.maxWaitSeconds", 900);
    }
}
