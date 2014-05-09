package com.sequenceiq.hbase.client;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class HBaseRow implements Serializable {

    private String rowKey;
    private Map<Family, Map<Column, String>> columnFamilies;

    public HBaseRow() {
    }

    public HBaseRow(String rowKey, String family, String qualifier, String value) {
        this.rowKey = rowKey;
        this.columnFamilies = createValuesMap(family, qualifier, value);
    }

    public HBaseRow(String rowKey, Map<Family, Map<Column, String>> columnFamilies) {
        this.rowKey = rowKey;
        this.columnFamilies = columnFamilies;
    }

    public String getRowKey() {
        return rowKey;
    }

    public byte[] getRowKeyBytes() {
        return rowKey == null ? null : rowKey.getBytes();
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public Map<Family, Map<Column, String>> getColumnFamilies() {
        return columnFamilies;
    }

    public void setColumnFamilies(Map<Family, Map<Column, String>> columnFamilies) {
        this.columnFamilies = columnFamilies;
    }

    public void addValue(String family, String column, String value) {
        if (columnFamilies == null) {
            columnFamilies = createValuesMap(family, column, value);
        } else {
            Family fam = new Qualifier(family);
            Map<Column, String> values = columnFamilies.get(fam);
            if (values == null) {
                values = new HashMap<>();
            }
            values.put(new Qualifier(column), value);
            columnFamilies.put(fam, values);
        }
    }

    private Map<Family, Map<Column, String>> createValuesMap(String family, String column, String value) {
        Map<Family, Map<Column, String>> families = new HashMap<>();
        Map<Column, String> values = new HashMap<>();
        values.put(new Qualifier(column), value);
        families.put(new Qualifier(family), values);
        return families;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HBaseRow{");
        sb.append("rowKey='").append(rowKey).append('\'');
        sb.append(", columnFamilies=").append(columnFamilies);
        sb.append('}');
        return sb.toString();
    }

}
