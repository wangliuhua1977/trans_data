package com.transdata;

public record FieldSpec(String columnName,
                        String jsonFieldName,
                        String jsonFieldPath,
                        PgType pgType,
                        boolean nullable) {
}
