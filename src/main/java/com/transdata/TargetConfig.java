package com.transdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TargetConfig {
    private String targetSchema = "leshan";
    private String targetTable;
    private String createTableSql;
    private InsertMode insertMode = InsertMode.TRUNCATE_THEN_APPEND;
    private String insertTemplate;
    private String conflictTarget;
    private AuditSettings auditSettings = new AuditSettings();

    public String getTargetSchema() {
        return targetSchema;
    }

    public void setTargetSchema(String targetSchema) {
        this.targetSchema = targetSchema == null ? "leshan" : targetSchema.trim();
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable == null ? "" : targetTable.trim();
    }

    public String getCreateTableSql() {
        return createTableSql;
    }

    public void setCreateTableSql(String createTableSql) {
        this.createTableSql = createTableSql;
    }

    public InsertMode getInsertMode() {
        return insertMode;
    }

    public void setInsertMode(InsertMode insertMode) {
        this.insertMode = insertMode == null ? InsertMode.APPEND : insertMode;
    }

    public String getInsertTemplate() {
        return insertTemplate;
    }

    public void setInsertTemplate(String insertTemplate) {
        this.insertTemplate = insertTemplate;
    }

    public String getConflictTarget() {
        return conflictTarget;
    }

    public void setConflictTarget(String conflictTarget) {
        this.conflictTarget = conflictTarget;
    }

    public AuditSettings getAuditSettings() {
        return auditSettings;
    }

    public void setAuditSettings(AuditSettings auditSettings) {
        this.auditSettings = auditSettings == null ? new AuditSettings() : auditSettings;
    }
}
