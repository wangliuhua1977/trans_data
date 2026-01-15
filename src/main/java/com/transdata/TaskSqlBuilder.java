package com.transdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class TaskSqlBuilder {
    private static final String LOCK_TABLE = "leshan.trans_data_job_lock";
    private static final String STAGING_TABLE = "leshan.stg_custom_task_rows";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TemplateRenderer templateRenderer = new TemplateRenderer();

    public String getLockTable() {
        return LOCK_TABLE;
    }

    public String getStagingTable() {
        return STAGING_TABLE;
    }

    public String buildCreateLockTableSql() {
        return "CREATE TABLE IF NOT EXISTS " + LOCK_TABLE + " ("
                + "lock_name text PRIMARY KEY,"
                + "owner_id text,"
                + "lease_until timestamptz,"
                + "updated_at timestamptz"
                + ");";
    }

    public String buildCreateStagingTableSql() {
        return "CREATE TABLE IF NOT EXISTS " + STAGING_TABLE + " ("
                + "task_id text,"
                + "run_id text,"
                + "row_no int,"
                + "payload jsonb,"
                + "created_at timestamptz"
                + ");";
    }

    public String buildCreateStagingIndexSql() {
        return "CREATE INDEX IF NOT EXISTS idx_stg_custom_task_run_id ON " + STAGING_TABLE
                + " (task_id, run_id);";
    }

    public String buildLockAcquireSql(String lockName, String ownerId, int leaseSeconds) {
        String lock = stringLiteral(lockName);
        String owner = stringLiteral(ownerId);
        return "INSERT INTO " + LOCK_TABLE + " (lock_name, owner_id, lease_until, updated_at) VALUES ("
                + lock + "," + owner + ", now() + interval '" + leaseSeconds + " seconds', now()) "
                + "ON CONFLICT (lock_name) DO UPDATE SET "
                + "owner_id = EXCLUDED.owner_id, "
                + "lease_until = EXCLUDED.lease_until, "
                + "updated_at = EXCLUDED.updated_at "
                + "WHERE " + LOCK_TABLE + ".lease_until < now() OR " + LOCK_TABLE + ".owner_id = EXCLUDED.owner_id;";
    }

    public String buildLockReleaseSql(String lockName, String ownerId) {
        String lock = stringLiteral(lockName);
        String owner = stringLiteral(ownerId);
        return "UPDATE " + LOCK_TABLE + " SET lease_until = now(), owner_id = NULL, updated_at = now() "
                + "WHERE lock_name = " + lock + " AND owner_id = " + owner + ";";
    }

    public String buildClearStagingSql(String taskId, String runId) {
        return "DELETE FROM " + STAGING_TABLE + " WHERE task_id = " + stringLiteral(taskId)
                + " AND run_id = " + stringLiteral(runId) + ";";
    }

    public List<String> buildInsertStagingSqlSegments(List<JsonNode> records,
                                                      String taskId,
                                                      String runId,
                                                      int batchSize) throws Exception {
        List<String> statements = new ArrayList<>();
        int total = records.size();
        int start = 0;
        int rowNo = 1;
        while (start < total) {
            int end = Math.min(start + batchSize, total);
            List<JsonNode> batch = records.subList(start, end);
            statements.add(buildInsertStagingBatch(batch, taskId, runId, rowNo));
            rowNo += batch.size();
            start = end;
        }
        return statements;
    }

    public String buildApplyToTargetSql(TaskDefinition task, String runId) {
        TargetConfig target = task.getTargetConfig();
        String targetTable = qualifyTarget(target);
        String insertTemplate = target.getInsertTemplate();
        InsertTemplate parsed = parseInsertTemplate(insertTemplate);
        String selectExpr = templateRenderer.renderValueExpression(parsed.values());
        String conflictTarget = target.getConflictTarget();
        String insertSql = "INSERT INTO " + targetTable + " (" + parsed.columns() + ") SELECT " + selectExpr
                + " FROM " + STAGING_TABLE
                + " WHERE task_id = " + stringLiteral(task.getTaskId())
                + " AND run_id = " + stringLiteral(runId);
        if (target.getInsertMode() == InsertMode.SKIP_DUPLICATES) {
            if (conflictTarget == null || conflictTarget.isBlank()) {
                insertSql += " ON CONFLICT DO NOTHING";
            } else {
                insertSql += " ON CONFLICT (" + conflictTarget + ") DO NOTHING";
            }
        }
        insertSql += ";";
        if (target.getInsertMode() == InsertMode.TRUNCATE_THEN_APPEND) {
            String deleteSql = buildDeleteTargetByScopeSql(task, runId);
            return "BEGIN;" + deleteSql + insertSql + "COMMIT;";
        }
        return insertSql;
    }

    public String buildDeleteTargetByScopeSql(TaskDefinition task, String runId) {
        TargetConfig target = task.getTargetConfig();
        String targetTable = qualifyTarget(target);
        List<String> scopeFields = target.getAuditSettings().getScopeKeyFields();
        if (scopeFields.isEmpty()) {
            return "TRUNCATE TABLE " + targetTable + ";";
        }
        String scopeCte = buildScopeCte(task.getTaskId(), runId, scopeFields);
        String join = buildScopeJoin("t", "s", scopeFields);
        return scopeCte + " DELETE FROM " + targetTable + " t USING scope s WHERE " + join + ";";
    }

    public String buildAuditTotalSql(TaskDefinition task, String runId) {
        TargetConfig target = task.getTargetConfig();
        String targetTable = qualifyTarget(target);
        List<String> scopeFields = target.getAuditSettings().getScopeKeyFields();
        if (scopeFields.isEmpty()) {
            return "SELECT count(*) FROM " + targetTable + ";";
        }
        String scopeCte = buildScopeCte(task.getTaskId(), runId, scopeFields);
        String join = buildScopeJoin("t", "s", scopeFields);
        return scopeCte + " SELECT count(*) FROM " + targetTable + " t JOIN scope s ON " + join + ";";
    }

    public String buildAuditDistinctSql(TaskDefinition task, String runId) {
        TargetConfig target = task.getTargetConfig();
        AuditSettings audit = target.getAuditSettings();
        String targetTable = qualifyTarget(target);
        List<String> scopeFields = audit.getScopeKeyFields();
        String scopeCte = scopeFields.isEmpty() ? "" : buildScopeCte(task.getTaskId(), runId, scopeFields);
        String join = scopeFields.isEmpty() ? "" : " JOIN scope s ON " + buildScopeJoin("t", "s", scopeFields);
        if (!audit.getNaturalKeyFields().isEmpty()) {
            String cols = String.join(",", audit.getNaturalKeyFields());
            String rowExpr = "(" + cols + ")";
            return scopeCte + " SELECT count(DISTINCT " + rowExpr + ") FROM " + targetTable + " t" + join + ";";
        }
        if (audit.getDistinctKeyJsonField() != null && !audit.getDistinctKeyJsonField().isBlank()) {
            String field = sanitizeField(audit.getDistinctKeyJsonField());
            return scopeCte + " SELECT count(DISTINCT t." + field + ") FROM " + targetTable + " t" + join + ";";
        }
        return scopeCte + " SELECT count(*) FROM " + targetTable + " t" + join + ";";
    }

    public String buildExpectedDistinctSql(TaskDefinition task, String runId) {
        AuditSettings audit = task.getTargetConfig().getAuditSettings();
        if (!audit.getNaturalKeyFields().isEmpty()) {
            String exprs = buildJsonRowExpr(audit.getNaturalKeyFields());
            return "SELECT count(DISTINCT " + exprs + ") FROM " + STAGING_TABLE
                    + " WHERE task_id = " + stringLiteral(task.getTaskId())
                    + " AND run_id = " + stringLiteral(runId) + ";";
        }
        if (audit.getDistinctKeyJsonField() != null && !audit.getDistinctKeyJsonField().isBlank()) {
            String field = sanitizeField(audit.getDistinctKeyJsonField());
            return "SELECT count(DISTINCT payload->>'" + field + "') FROM " + STAGING_TABLE
                    + " WHERE task_id = " + stringLiteral(task.getTaskId())
                    + " AND run_id = " + stringLiteral(runId) + ";";
        }
        return "SELECT count(*) FROM " + STAGING_TABLE
                + " WHERE task_id = " + stringLiteral(task.getTaskId())
                + " AND run_id = " + stringLiteral(runId) + ";";
    }

    private String buildScopeCte(String taskId, String runId, List<String> fields) {
        StringBuilder builder = new StringBuilder();
        builder.append("WITH scope AS (SELECT DISTINCT ");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            String field = sanitizeField(fields.get(i));
            builder.append("payload->>'").append(field).append("' AS ").append(field);
        }
        builder.append(" FROM ").append(STAGING_TABLE)
                .append(" WHERE task_id = ").append(stringLiteral(taskId))
                .append(" AND run_id = ").append(stringLiteral(runId))
                .append(")");
        return builder.toString();
    }

    private String buildScopeJoin(String targetAlias, String scopeAlias, List<String> fields) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                builder.append(" AND ");
            }
            String field = sanitizeField(fields.get(i));
            builder.append(targetAlias).append(".").append(field)
                    .append("::text = ")
                    .append(scopeAlias).append(".").append(field);
        }
        return builder.toString();
    }

    private String buildJsonRowExpr(List<String> fields) {
        StringBuilder builder = new StringBuilder("(");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            String field = sanitizeField(fields.get(i));
            builder.append("payload->>'").append(field).append("'");
        }
        builder.append(")");
        return builder.toString();
    }

    private InsertTemplate parseInsertTemplate(String template) {
        if (template == null || template.isBlank()) {
            throw new IllegalArgumentException("插入模板为空");
        }
        String[] parts = template.split("=>", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("插入模板格式错误，需要使用 '列列表 => 值表达式列表' 结构");
        }
        String columns = parts[0].trim();
        String values = parts[1].trim();
        if (columns.isBlank() || values.isBlank()) {
            throw new IllegalArgumentException("插入模板缺少列或值");
        }
        return new InsertTemplate(columns, values);
    }

    private String buildInsertStagingBatch(List<JsonNode> records, String taskId, String runId, int startRowNo)
            throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ").append(STAGING_TABLE)
                .append(" (task_id, run_id, row_no, payload, created_at) VALUES ");
        int rowNo = startRowNo;
        for (int i = 0; i < records.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            String json = objectMapper.writeValueAsString(records.get(i));
            builder.append("(")
                    .append(stringLiteral(taskId)).append(",")
                    .append(stringLiteral(runId)).append(",")
                    .append(rowNo).append(",")
                    .append(jsonLiteral(json)).append(", now())");
            rowNo++;
        }
        builder.append(";");
        return builder.toString();
    }

    private String stringLiteral(String value) {
        if (value == null) {
            return "''";
        }
        return "'" + value.replace("'", "''") + "'";
    }

    private String jsonLiteral(String json) {
        String escaped = json == null ? "" : json.replace("'", "''");
        return "'" + escaped + "'::jsonb";
    }

    private String sanitizeField(String field) {
        if (field == null || field.isBlank()) {
            throw new IllegalArgumentException("字段为空");
        }
        if (!field.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("字段不合法：" + field);
        }
        return field;
    }

    private String qualifyTarget(TargetConfig target) {
        String schema = target.getTargetSchema();
        String table = target.getTargetTable();
        if (schema == null || schema.isBlank()) {
            return table;
        }
        return schema + "." + table;
    }

    private record InsertTemplate(String columns, String values) {
    }
}
