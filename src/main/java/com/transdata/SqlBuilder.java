package com.transdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class SqlBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlBuilder.class);
    private static final String TABLE_NAME = "leshan.dm_prod_offer_ind_list_leshan";
    private static final String STAGING_TABLE = "leshan.stg_dm_prod_offer_ind_list_leshan";
    private static final String LOCK_TABLE = "leshan.trans_data_job_lock";
    private static final String[] COLUMNS = {
            "order_item_id",
            "date_no",
            "obj_id",
            "ind_type",
            "accept_staff_id",
            "accept_channel_id",
            "first_staff_id",
            "second_staff_id",
            "dev_staff_id",
            "level5_id",
            "datetime",
            "accept_date"
    };

    public String getTargetTable() {
        return TABLE_NAME;
    }

    public String getStagingTable() {
        return STAGING_TABLE;
    }

    public String getLockTable() {
        return LOCK_TABLE;
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
                + "run_id text,"
                + "order_item_id text,"
                + "date_no numeric,"
                + "obj_id text,"
                + "ind_type numeric,"
                + "accept_staff_id text,"
                + "accept_channel_id text,"
                + "first_staff_id text,"
                + "second_staff_id text,"
                + "dev_staff_id text,"
                + "level5_id text,"
                + "datetime numeric,"
                + "accept_date text"
                + ");";
    }

    public String buildCreateStagingIndexSql() {
        return "CREATE INDEX IF NOT EXISTS idx_stg_dm_prod_offer_run_id ON " + STAGING_TABLE + " (run_id);";
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

    public String buildClearStagingSql(String runId) {
        return "DELETE FROM " + STAGING_TABLE + " WHERE run_id = " + stringLiteral(runId) + ";";
    }

    public String buildDeleteTargetByKeysSql(List<GroupKey> keys, UiLogSink uiLog) {
        return "DELETE FROM " + TABLE_NAME + " WHERE " + buildKeyFilter(keys, uiLog) + ";";
    }

    public String buildAuditByGroupSql(List<GroupKey> keys, UiLogSink uiLog) {
        return "SELECT date_no, datetime, count(*) cnt, count(distinct order_item_id) dcnt FROM " + TABLE_NAME
                + " WHERE " + buildKeyFilter(keys, uiLog)
                + " GROUP BY date_no, datetime ORDER BY date_no, datetime;";
    }

    public String buildAuditTotalsSql(List<GroupKey> keys, UiLogSink uiLog) {
        return "SELECT count(*) total_cnt, count(distinct order_item_id) total_dcnt FROM " + TABLE_NAME
                + " WHERE " + buildKeyFilter(keys, uiLog) + ";";
    }

    public String buildApplyFromStagingSql(List<GroupKey> keys, String runId, UiLogSink uiLog) {
        StringBuilder builder = new StringBuilder();
        builder.append("BEGIN;");
        builder.append("DELETE FROM ").append(TABLE_NAME).append(" WHERE ").append(buildKeyFilter(keys, uiLog)).append(";");
        builder.append("INSERT INTO ").append(TABLE_NAME).append(" (");
        appendColumns(builder, COLUMNS);
        builder.append(") SELECT ");
        appendColumns(builder, COLUMNS);
        builder.append(" FROM ").append(STAGING_TABLE).append(" WHERE run_id = ").append(stringLiteral(runId)).append(";");
        builder.append("COMMIT;");
        return builder.toString();
    }

    public List<String> buildInsertSqlSegments(List<SourceRecord> records, int batchSize, UiLogSink uiLog) {
        List<String> statements = new ArrayList<>();
        int total = records.size();
        int start = 0;
        while (start < total) {
            int end = Math.min(start + batchSize, total);
            List<SourceRecord> batch = records.subList(start, end);
            statements.add(buildInsertBatch(batch, uiLog));
            start = end;
        }
        return statements;
    }

    public List<String> buildInsertStagingSqlSegments(List<SourceRecord> records,
                                                      String runId,
                                                      int batchSize,
                                                      UiLogSink uiLog) {
        List<String> statements = new ArrayList<>();
        int total = records.size();
        int start = 0;
        while (start < total) {
            int end = Math.min(start + batchSize, total);
            List<SourceRecord> batch = records.subList(start, end);
            statements.add(buildInsertStagingBatch(batch, runId, uiLog));
            start = end;
        }
        return statements;
    }

    private String buildInsertBatch(List<SourceRecord> records, UiLogSink uiLog) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ").append(TABLE_NAME).append(" (");
        appendColumns(builder, COLUMNS);
        builder.append(") VALUES ");
        for (int i = 0; i < records.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(buildValues(records.get(i), uiLog));
        }
        builder.append(";");
        return builder.toString();
    }

    private String buildInsertStagingBatch(List<SourceRecord> records, String runId, UiLogSink uiLog) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ").append(STAGING_TABLE).append(" (run_id,");
        appendColumns(builder, COLUMNS);
        builder.append(") VALUES ");
        for (int i = 0; i < records.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(buildStagingValues(records.get(i), runId, uiLog));
        }
        builder.append(";");
        return builder.toString();
    }

    private String buildValues(SourceRecord record, UiLogSink uiLog) {
        return "(" + stringLiteral(record.getOrderItemId()) + ","
                + numericLiteral(record.getDateNo(), "date_no", uiLog) + ","
                + stringLiteral(record.getObjId()) + ","
                + numericLiteral(record.getIndType(), "ind_type", uiLog) + ","
                + stringLiteral(record.getAcceptStaffId()) + ","
                + stringLiteral(record.getAcceptChannelId()) + ","
                + stringLiteral(record.getFirstStaffId()) + ","
                + stringLiteral(record.getSecondStaffId()) + ","
                + stringLiteral(record.getDevStaffId()) + ","
                + stringLiteral(record.getLevel5Id()) + ","
                + numericLiteral(record.getDatetime(), "datetime", uiLog) + ","
                + stringLiteral(record.getAcceptDate())
                + ")";
    }

    private String buildStagingValues(SourceRecord record, String runId, UiLogSink uiLog) {
        return "(" + stringLiteral(runId) + ","
                + stringLiteral(record.getOrderItemId()) + ","
                + numericLiteral(record.getDateNo(), "date_no", uiLog) + ","
                + stringLiteral(record.getObjId()) + ","
                + numericLiteral(record.getIndType(), "ind_type", uiLog) + ","
                + stringLiteral(record.getAcceptStaffId()) + ","
                + stringLiteral(record.getAcceptChannelId()) + ","
                + stringLiteral(record.getFirstStaffId()) + ","
                + stringLiteral(record.getSecondStaffId()) + ","
                + stringLiteral(record.getDevStaffId()) + ","
                + stringLiteral(record.getLevel5Id()) + ","
                + numericLiteral(record.getDatetime(), "datetime", uiLog) + ","
                + stringLiteral(record.getAcceptDate())
                + ")";
    }

    private String stringLiteral(String value) {
        if (value == null || value.isBlank()) {
            return "NULL";
        }
        String escaped = value.replace("'", "''");
        return "'" + escaped + "'";
    }

    private String numericLiteral(String value, String field) {
        return numericLiteral(value, field, null);
    }

    private String numericLiteral(String value, String field, UiLogSink uiLog) {
        if (value == null || value.isBlank()) {
            return "NULL";
        }
        try {
            BigDecimal number = new BigDecimal(value.trim());
            return number.toPlainString();
        } catch (NumberFormatException ex) {
            String message = "Invalid numeric value for " + field + ": " + value + ", using NULL.";
            LOGGER.warn(message);
            if (uiLog != null) {
                uiLog.log("WARN", message);
            }
        }
        return "NULL";
    }

    private String buildKeyFilter(List<GroupKey> keys, UiLogSink uiLog) {
        if (keys == null || keys.isEmpty()) {
            return "FALSE";
        }
        StringBuilder builder = new StringBuilder();
        builder.append("(date_no, datetime) IN (");
        for (int i = 0; i < keys.size(); i++) {
            GroupKey key = keys.get(i);
            if (i > 0) {
                builder.append(",");
            }
            builder.append("(")
                    .append(numericLiteral(key.getDateNo(), "date_no", uiLog))
                    .append(",")
                    .append(numericLiteral(key.getDatetime(), "datetime", uiLog))
                    .append(")");
        }
        builder.append(")");
        return builder.toString();
    }

    private void appendColumns(StringBuilder builder, String[] columns) {
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(columns[i]);
        }
    }
}
