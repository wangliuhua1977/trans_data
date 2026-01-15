package com.transdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.LocalTime;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SchedulePolicy {
    private int intervalSeconds = 180;
    private String windowStart = "00:00:00";
    private String windowEnd = "23:59:59";
    private int maxRetries = 3;
    private int leaseSeconds = 300;

    public int getIntervalSeconds() {
        return intervalSeconds;
    }

    public void setIntervalSeconds(int intervalSeconds) {
        this.intervalSeconds = Math.max(1, intervalSeconds);
    }

    public String getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart == null ? "00:00:00" : windowStart;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd == null ? "23:59:59" : windowEnd;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = Math.max(1, maxRetries);
    }

    public int getLeaseSeconds() {
        return leaseSeconds;
    }

    public void setLeaseSeconds(int leaseSeconds) {
        this.leaseSeconds = Math.max(30, leaseSeconds);
    }

    public LocalTime resolveWindowStart() {
        try {
            return LocalTime.parse(windowStart);
        } catch (Exception ex) {
            return LocalTime.MIDNIGHT;
        }
    }

    public LocalTime resolveWindowEnd() {
        try {
            return LocalTime.parse(windowEnd);
        } catch (Exception ex) {
            return LocalTime.of(23, 59, 59);
        }
    }
}
