package com.transdata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TaskStore {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AppConfig config;

    public TaskStore(AppConfig config) {
        this.config = config;
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public List<TaskDefinition> loadTasks(UiLogSink uiLog) {
        Path path = resolveTasksPath();
        if (Files.exists(path)) {
            try {
                String json = Files.readString(path, StandardCharsets.UTF_8);
                if (json.isBlank()) {
                    return new ArrayList<>();
                }
                return objectMapper.readValue(json, new TypeReference<>() {
                });
            } catch (IOException ex) {
                if (uiLog != null) {
                    uiLog.log("ERROR", "加载任务配置失败：" + ex.getMessage());
                }
            }
        }
        List<TaskDefinition> defaults = new ArrayList<>();
        defaults.add(defaultTask());
        saveTasks(defaults, uiLog);
        return defaults;
    }

    public void saveTasks(List<TaskDefinition> tasks, UiLogSink uiLog) {
        Path path = resolveTasksPath();
        try {
            Files.createDirectories(path.getParent());
            String json = objectMapper.writeValueAsString(tasks);
            Files.writeString(path, json, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            if (uiLog != null) {
                uiLog.log("ERROR", "保存任务配置失败：" + ex.getMessage());
            }
        }
    }

    public Path resolveTasksPath() {
        Path configPath = config.getUserConfigPath();
        Path base = configPath == null ? Path.of(System.getProperty("user.home", "."), ".trans_data")
                : configPath.getParent();
        return base.resolve("tasks.json");
    }

    private TaskDefinition defaultTask() {
        TaskDefinition task = new TaskDefinition();
        task.setTaskId(UUID.randomUUID().toString());
        task.setTaskName("默认任务");
        task.setEnabled(config.isScheduleEnabled());
        task.setSourcePostUrl(config.getSourceUrl());
        task.setSourcePostBody(config.getSourceBody());
        task.setBatchSize(config.getBatchSize());

        SchedulePolicy schedule = new SchedulePolicy();
        schedule.setIntervalSeconds(config.getIntervalSeconds());
        schedule.setWindowStart(config.getWindowStart().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")));
        schedule.setWindowEnd(config.getWindowEnd().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")));
        schedule.setMaxRetries(config.getJobMaxRetries());
        schedule.setLeaseSeconds(config.getLockLeaseSeconds());
        task.setSchedulePolicy(schedule);

        TargetConfig target = new TargetConfig();
        target.setTargetSchema("leshan");
        target.setTargetTable("dm_prod_offer_ind_list_leshan");
        target.setInsertMode(InsertMode.TRUNCATE_THEN_APPEND);
        target.setInsertTemplate("order_item_id,date_no,obj_id,ind_type,accept_staff_id,accept_channel_id,first_staff_id,second_staff_id,dev_staff_id,level5_id,datetime,accept_date => "
                + "${order_item_id?text},${date_no?num},${obj_id?text},${ind_type?num},${accept_staff_id?text},${accept_channel_id?text},${first_staff_id?text},${second_staff_id?text},${dev_staff_id?text},${level5_id?text},${datetime?num},${ACCEPT_DATE?text}");
        AuditSettings audit = new AuditSettings();
        List<String> scopeKeys = new ArrayList<>();
        scopeKeys.add("date_no");
        scopeKeys.add("datetime");
        audit.setScopeKeyFields(scopeKeys);
        audit.setDistinctKeyJsonField("order_item_id");
        target.setAuditSettings(audit);
        task.setTargetConfig(target);
        return task;
    }
}
