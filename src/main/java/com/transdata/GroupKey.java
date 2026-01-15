package com.transdata;

import java.util.Objects;

public class GroupKey {
    private final String dateNo;
    private final String datetime;

    public GroupKey(String dateNo, String datetime) {
        this.dateNo = dateNo;
        this.datetime = datetime;
    }

    public String getDateNo() {
        return dateNo;
    }

    public String getDatetime() {
        return datetime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupKey groupKey = (GroupKey) o;
        return Objects.equals(dateNo, groupKey.dateNo) && Objects.equals(datetime, groupKey.datetime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dateNo, datetime);
    }

    @Override
    public String toString() {
        return "(" + dateNo + ", " + datetime + ")";
    }
}
