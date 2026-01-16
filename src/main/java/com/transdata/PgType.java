package com.transdata;

public enum PgType {
    TEXT("text"),
    BIGINT("bigint"),
    NUMERIC("numeric"),
    BOOLEAN("boolean"),
    JSONB("jsonb");

    private final String ddl;

    PgType(String ddl) {
        this.ddl = ddl;
    }

    public String ddl() {
        return ddl;
    }
}
