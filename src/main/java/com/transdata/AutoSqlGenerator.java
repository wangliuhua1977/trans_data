package com.transdata;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class AutoSqlGenerator {
    private static final String DEFAULT_SCHEMA = "leshan";
    private static final Set<String> ORDER_ITEM_HINTS = Set.of("order_item_id", "orderitemid", "order_itemid");
    private final JsonSchemaInfer infer = new JsonSchemaInfer();
    private final TemplatePreviewRenderer previewRenderer = new TemplatePreviewRenderer();

    public AutoSqlResult generate(TaskDefinition task,
                                  List<JsonNode> records,
                                  boolean suggestUniqueConstraint) {
        List<String> warnings = new ArrayList<>();
        List<JsonNode> safeRecords = records == null ? List.of() : records;
        List<FieldSpec> specs = infer.infer(safeRecords);
        if (safeRecords.isEmpty()) {
            warnings.add("No data array records detected; generated generic payload structure; please edit manually.");
        }
        String schema = normalize(task.getTargetConfig().getTargetSchema(), DEFAULT_SCHEMA);
        String table = task.getTargetConfig().getTargetTable();
        if (table == null || table.isBlank()) {
            table = deriveTableName(task);
        }
        String qualified = schema == null || schema.isBlank() ? table : schema + "." + table;
        String ddl = buildCreateTableSql(schema, table, specs, task.getTargetConfig().getInsertMode(),
                suggestUniqueConstraint, warnings);
        String template = buildInsertTemplate(specs);
        String mappingPreview = buildMappingPreview(specs);
        TemplatePreviewRenderer.PreviewResult preview = previewRenderer.render(template, safeRecords,
                task.getTaskId(), "RUN_ID_PREVIEW", qualified);
        warnings.addAll(preview.warnings());
        return new AutoSqlResult(schema, table, ddl, template, mappingPreview, preview.previewSql(), warnings);
    }

    private String buildCreateTableSql(String schema,
                                       String table,
                                       List<FieldSpec> specs,
                                       InsertMode insertMode,
                                       boolean suggestUniqueConstraint,
                                       List<String> warnings) {
        StringBuilder builder = new StringBuilder();
        String qualified = schema == null || schema.isBlank() ? table : schema + "." + table;
        builder.append("CREATE TABLE IF NOT EXISTS ").append(qualified).append(" (\n");
        List<String> columns = new ArrayList<>();
        for (FieldSpec spec : specs) {
            columns.add("  " + spec.columnName() + " " + spec.pgType().ddl());
        }
        columns.add("  payload jsonb");
        columns.add("  task_id text");
        columns.add("  task_run_id text");
        columns.add("  created_at timestamptz DEFAULT now()");
        builder.append(String.join(",\n", columns));
        builder.append("\n);");
        builder.append("\nCREATE INDEX IF NOT EXISTS idx_")
                .append(table).append("_task_run ON ")
                .append(qualified).append(" (task_id, task_run_id);");
        String orderItemColumn = findOrderItemColumn(specs);
        if (orderItemColumn != null) {
            builder.append("\nCREATE INDEX IF NOT EXISTS idx_")
                    .append(table).append("_").append(orderItemColumn)
                    .append(" ON ").append(qualified)
                    .append(" (").append(orderItemColumn).append(");");
            if (insertMode == InsertMode.SKIP_DUPLICATES && suggestUniqueConstraint) {
                builder.append("\nCREATE UNIQUE INDEX IF NOT EXISTS ux_")
                        .append(table).append("_").append(orderItemColumn)
                        .append(" ON ").append(qualified)
                        .append(" (").append(orderItemColumn).append(");");
            }
        } else if (insertMode == InsertMode.SKIP_DUPLICATES && suggestUniqueConstraint) {
            warnings.add("未识别到 order_item_id 类字段，无法生成唯一约束建议");
        }
        return builder.toString();
    }

    private String buildInsertTemplate(List<FieldSpec> specs) {
        List<String> columns = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (FieldSpec spec : specs) {
            columns.add(spec.columnName());
            values.add(buildPlaceholder(spec));
        }
        columns.add("payload");
        values.add("payload");
        columns.add("task_id");
        values.add("task_id");
        columns.add("task_run_id");
        values.add("run_id");
        columns.add("created_at");
        values.add("now()");
        return String.join(",", columns) + " => " + String.join(",", values);
    }

    private String buildPlaceholder(FieldSpec spec) {
        String field = spec.jsonFieldName();
        return switch (spec.pgType()) {
            case BIGINT, NUMERIC -> "${" + field + "?num}";
            case BOOLEAN -> "${" + field + "?bool}";
            case JSONB -> "${" + field + "?jsonb}";
            case TEXT -> "${" + field + "?text}";
        };
    }

    private String buildMappingPreview(List<FieldSpec> specs) {
        if (specs.isEmpty()) {
            return "(无可展开字段，仅使用 payload)";
        }
        return specs.stream()
                .map(spec -> spec.columnName() + " | " + spec.jsonFieldPath() + " | " + spec.pgType().ddl()
                        + (spec.nullable() ? " | nullable" : ""))
                .collect(Collectors.joining("\n"));
    }

    private String findOrderItemColumn(List<FieldSpec> specs) {
        for (FieldSpec spec : specs) {
            String name = spec.columnName().toLowerCase(Locale.ROOT);
            if (ORDER_ITEM_HINTS.contains(name)) {
                return spec.columnName();
            }
        }
        return null;
    }

    private String deriveTableName(TaskDefinition task) {
        String name = task.getTaskName();
        if (name == null || name.isBlank()) {
            String taskId = task.getTaskId();
            if (taskId != null && taskId.length() >= 8) {
                return "task_" + taskId.substring(0, 8).toLowerCase(Locale.ROOT);
            }
            return "task_data";
        }
        String snake = name.trim().toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("_+", "_");
        if (snake.startsWith("_")) {
            snake = snake.substring(1);
        }
        if (snake.endsWith("_")) {
            snake = snake.substring(0, snake.length() - 1);
        }
        if (snake.isBlank()) {
            return "task_data";
        }
        return snake;
    }

    private String normalize(String value, String fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value.trim();
    }

    public record AutoSqlResult(String targetSchema,
                                String targetTable,
                                String createTableSql,
                                String insertTemplate,
                                String mappingPreview,
                                String previewSql,
                                List<String> warnings) {
    }
}
