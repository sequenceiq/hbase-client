package com.sequenceiq.hbase.client;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTablePool {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseTablePool.class);
    private static ThreadLocal<Map<String, HTableInterface>> resources = new ThreadLocal<>();
    private HBaseTableFactory tableFactory;

    public HBaseTablePool(Configuration hbaseConfiguration) {
        tableFactory = new HBaseTableFactory(hbaseConfiguration);
    }

    public HTableInterface getHTable(String tableName) {
        LOGGER.info("Get HBASE table: {}", tableName);
        Map<String, HTableInterface> tables = getResource();
        HTableInterface table = tables.get(tableName);
        if (table == null) {
            table = tableFactory.createHTable(tableName);
            tables.put(tableName, table);
        }
        return table;
    }

    public void releaseHTable(String tableName) throws IOException {
        Map<String, HTableInterface> tables = getResource();
        HTableInterface table = tables.get(tableName);
        if (table != null) {
            tableFactory.releaseHTable(table);
            tables.remove(tableName);
        }
    }

    private Map<String, HTableInterface> getResource() {
        Map<String, HTableInterface> map = resources.get();
        if (map == null) {
            map = new LinkedHashMap<>();
            resources.set(map);
            LOGGER.info("Adding new table map for thread: {}", Thread.currentThread().getName());
        }
        return map;
    }

    private class HBaseTableFactory {

        private Configuration hbaseConfiguration;
        private HTableInterfaceFactory tableFactory;

        private HBaseTableFactory(Configuration configuration) {
            hbaseConfiguration = HBaseConfiguration.create(configuration);
            tableFactory = new HTableFactory();
        }

        private HTableInterface createHTable(String tableName) {
            LOGGER.info("Create HBASE table: {}", tableName);
            return tableFactory.createHTableInterface(hbaseConfiguration, tableName.getBytes());
        }

        private void releaseHTable(HTableInterface table) throws IOException {
            LOGGER.info("Releasing HBASE table: {}", table.getName().getNameAsString());
            tableFactory.releaseHTableInterface(table);
        }
    }

}
