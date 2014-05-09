package com.sequenceiq.hbase.admin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseManager.class);
    private final HBaseAdmin hBaseAdmin;

    public HBaseManager(Configuration hBaseConfiguration) throws IOException {
        this(new HBaseAdmin(hBaseConfiguration));
    }

    public HBaseManager(HBaseAdmin hBaseAdmin) {
        this.hBaseAdmin = hBaseAdmin;
    }

    public boolean isAvailable(String table) {
        boolean result = false;
        try {
            if (hBaseAdmin.isTableAvailable(table)) {
                result = isEnabled(table);
            }
        } catch (IOException e) {
            LOGGER.warn("Cannot determine table's ({}) state", table, e);
        }
        return result;
    }

    public boolean isEnabled(String table) {
        boolean result = false;
        try {
            result = hBaseAdmin.isTableEnabled(table);
        } catch (IOException e) {
            LOGGER.warn("Cannot determine table: ({}) state", table, e);
        }
        return result;
    }

    public boolean create(String tableName, String... families) {
        boolean result = true;
        try {
            byte[] tableNameBytes = tableName.getBytes();
            TableName.isLegalTableQualifierName(tableNameBytes);
            TableName tableNameObject = TableName.valueOf(tableNameBytes);
            HTableDescriptor table = new HTableDescriptor(tableNameObject);
            if (families != null) {
                for (String family : families) {
                    table.addFamily(new HColumnDescriptor(family));
                }
            }
            hBaseAdmin.createTable(table);
        } catch (Exception e) {
            LOGGER.warn("Cannot create table: {}", tableName, e);
            result = false;
        }
        return result;
    }

    public boolean delete(String table) {
        boolean result = false;
        try {
            boolean disabled = true;
            if (isEnabled(table)) {
                disabled = disable(table);
            }
            if (disabled) {
                hBaseAdmin.deleteTable(table.getBytes());
                result = true;
            }
        } catch (IOException e) {
            LOGGER.warn("Cannot delete table {}", table, e);
        }
        return result;
    }

    public boolean disable(String table) {
        boolean result = true;
        try {
            hBaseAdmin.disableTable(table.getBytes());
        } catch (IOException e) {
            LOGGER.warn("Cannot disable table {}", table, e);
            result = false;
        }
        return result;
    }

}
