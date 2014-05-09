package com.sequenceiq.hbase.client;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public final class HBaseRowUtil {

    private HBaseRowUtil() {
        throw new IllegalStateException();
    }

    public static Put convert(HBaseRow hBaseRow) {
        return isValid(hBaseRow) ? convert0(hBaseRow) : null;
    }

    public static boolean isValid(HBaseRow row) {
        boolean result = true;
        if (row == null || isBlank(row.getRowKey())) {
            result = false;
        } else {
            for (Map.Entry<Family, Map<Column, String>> family : row.getColumnFamilies().entrySet()) {
                if (!result || isBlank(family.getKey().getName())) {
                    result = false;
                    break;
                }
                for (Map.Entry<Column, String> column : family.getValue().entrySet()) {
                    if (isBlank(column.getKey().getName()) || isBlank(column.getValue())) {
                        result = false;
                        break;
                    }
                }
            }
        }
        return result;
    }

    public static HBaseRow convert(Result result) {
        return result != null && result.getMap() != null ? convert0(result) : null;
    }

    private static Put convert0(HBaseRow hBaseRow) {
        Put put = new Put(hBaseRow.getRowKeyBytes());
        for (Map.Entry<Family, Map<Column, String>> family : hBaseRow.getColumnFamilies().entrySet()) {
            byte[] familyBytes = family.getKey().getNameBytes();
            for (Map.Entry<Column, String> qualifier : family.getValue().entrySet()) {
                put.add(familyBytes, qualifier.getKey().getNameBytes(), Bytes.toBytes(qualifier.getValue()));
            }
        }
        return put;
    }

    private static HBaseRow convert0(Result result) {
        Map<Family, Map<Column, String>> families = new HashMap<>();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> family : result.getMap().entrySet()) {
            String familyName = Bytes.toString(family.getKey());
            Map<Column, String> values = new HashMap<>();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifier : family.getValue().entrySet()) {
                String columnName = Bytes.toString(qualifier.getKey());
                String value = Bytes.toString(qualifier.getValue().lastEntry().getValue());
                values.put(new Qualifier(columnName), value);
            }
            families.put(new Qualifier(familyName), values);
        }
        return new HBaseRow(Bytes.toString(result.getRow()), families);
    }

}
