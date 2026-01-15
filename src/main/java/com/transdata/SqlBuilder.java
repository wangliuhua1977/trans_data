package com.transdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class SqlBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlBuilder.class);
    private static final String TABLE_NAME = "leshan.dm_prod_offer_ind_list_leshan";
    private static final int SEGMENT_SIZE = 100;
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

    public String buildExistenceCheckSql(GroupKey key, UiLogSink uiLog) {
        String dateNo = numericLiteral(key.getDateNo(), "date_no", uiLog);
        String datetime = numericLiteral(key.getDatetime(), "datetime", uiLog);
        return "SELECT 1 FROM " + TABLE_NAME + " WHERE date_no = " + dateNo
                + " AND datetime = " + datetime + " LIMIT 1;";
    }

    public List<String> buildInsertSqlSegments(List<SourceRecord> records, UiLogSink uiLog) {
        List<String> statements = new ArrayList<>();
        int total = records.size();
        int start = 0;
        while (start < total) {
            int end = Math.min(start + SEGMENT_SIZE, total);
            List<SourceRecord> batch = records.subList(start, end);
            statements.add(buildInsertBatch(batch, uiLog));
            start = end;
        }
        return statements;
    }

    private String buildInsertBatch(List<SourceRecord> records, UiLogSink uiLog) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ").append(TABLE_NAME).append(" (");
        for (int i = 0; i < COLUMNS.length; i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(COLUMNS[i]);
        }
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
            return "NULL";
        }
    }
}
