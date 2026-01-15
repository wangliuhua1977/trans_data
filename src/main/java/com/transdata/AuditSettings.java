package com.transdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AuditSettings {
    private List<String> scopeKeyFields = new ArrayList<>();
    private List<String> naturalKeyFields = new ArrayList<>();
    private String distinctKeyJsonField;

    public List<String> getScopeKeyFields() {
        return scopeKeyFields;
    }

    public void setScopeKeyFields(List<String> scopeKeyFields) {
        this.scopeKeyFields = scopeKeyFields == null ? new ArrayList<>() : new ArrayList<>(scopeKeyFields);
    }

    public List<String> getNaturalKeyFields() {
        return naturalKeyFields;
    }

    public void setNaturalKeyFields(List<String> naturalKeyFields) {
        this.naturalKeyFields = naturalKeyFields == null ? new ArrayList<>() : new ArrayList<>(naturalKeyFields);
    }

    public String getDistinctKeyJsonField() {
        return distinctKeyJsonField;
    }

    public void setDistinctKeyJsonField(String distinctKeyJsonField) {
        this.distinctKeyJsonField = distinctKeyJsonField;
    }
}
