package com.transdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskDefinition {
    private String taskId;
    private String taskName;
    private boolean enabled = true;
    private String sourcePostUrl;
    private String sourcePostBody;
    private SchedulePolicy schedulePolicy = new SchedulePolicy();
    private TargetConfig targetConfig = new TargetConfig();
    private int batchSize = 100;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getSourcePostUrl() {
        return sourcePostUrl;
    }

    public void setSourcePostUrl(String sourcePostUrl) {
        this.sourcePostUrl = sourcePostUrl == null ? "" : sourcePostUrl.trim();
    }

    public String getSourcePostBody() {
        return sourcePostBody;
    }

    public void setSourcePostBody(String sourcePostBody) {
        this.sourcePostBody = sourcePostBody;
    }

    public SchedulePolicy getSchedulePolicy() {
        return schedulePolicy;
    }

    public void setSchedulePolicy(SchedulePolicy schedulePolicy) {
        this.schedulePolicy = schedulePolicy == null ? new SchedulePolicy() : schedulePolicy;
    }

    public TargetConfig getTargetConfig() {
        return targetConfig;
    }

    public void setTargetConfig(TargetConfig targetConfig) {
        this.targetConfig = targetConfig == null ? new TargetConfig() : targetConfig;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = Math.max(1, batchSize);
    }

    public String displayName() {
        String name = taskName == null || taskName.isBlank() ? "未命名任务" : taskName;
        return name + (enabled ? "" : "（已停用）");
    }
}
