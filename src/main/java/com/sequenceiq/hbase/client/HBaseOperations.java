package com.sequenceiq.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseOperations.class);
    private HBaseTablePool tablePool;

    public HBaseOperations(HBaseTablePool tablePool) {
        this.tablePool = tablePool;
    }

    public void put(String table, final String rowKey, final String family, final String qualifier, final String value, boolean release) {
        put(table, new HBaseRow(rowKey, family, qualifier, value), release);
    }

    public void put(String table, final HBaseRow row, boolean release) {
        put(table, Collections.singletonList(row), release);
    }

    public void put(String table, final Collection<HBaseRow> rows, boolean release) {
        LOGGER.info("Execute put request: {}", rows);
        execute(table, new TableCallback<Void>() {
            @Override
            public Void doInTable(HTableInterface table) throws Exception {
                List<Put> puts = new ArrayList<>(rows.size());
                for (HBaseRow row : rows) {
                    Put put = HBaseRowUtil.convert(row);
                    if (put != null) {
                        puts.add(put);
                    }
                }
                table.put(puts);
                return null;
            }
        }, release);
    }

    public HBaseRow get(String table, final String rowKey, boolean release) {
        return get(table, rowKey, new RowMapper<HBaseRow>() {
                    @Override
                    public HBaseRow mapRow(Result result) throws Exception {
                        return HBaseRowUtil.convert(result);
                    }
                }, release
        );
    }

    public <T> T get(String table, final String rowKey, RowMapper<T> mapper, boolean release) {
        return get(table, rowKey, null, null, mapper, release);
    }

    public <T> T get(String table, final String rowKey, final String family, RowMapper<T> mapper, boolean release) {
        return get(table, rowKey, family, null, mapper, release);
    }

    public <T> T get(String table, final String rowKey, final String familyName, final String qualifier, final RowMapper<T> mapper, boolean release) {
        return execute(table, new TableCallback<T>() {
            @Override
            public T doInTable(HTableInterface table) throws Exception {
                Get get = new Get(rowKey.getBytes());
                if (familyName != null) {
                    byte[] family = familyName.getBytes();
                    if (qualifier != null) {
                        get.addColumn(family, qualifier.getBytes());
                    } else {
                        get.addFamily(family);
                    }
                }
                Result result = table.get(get);
                return mapper.mapRow(result);
            }
        }, release);
    }

    public <T> T scan(String tableName, String family, String column, final ResultExtractor<T> extractor, boolean release) {
        Scan scan = new Scan();
        scan.addColumn(family.getBytes(), column.getBytes());
        return scan(tableName, scan, extractor, release);
    }

    public <T> T scan(String tableName, final Scan scan, final ResultExtractor<T> extractor, boolean release) {
        return execute(tableName, new TableCallback<T>() {
            @Override
            public T doInTable(HTableInterface table) throws Exception {
                try (ResultScanner scanner = table.getScanner(scan)) {
                    return extractor.extractData(scanner);
                }
            }
        }, release);
    }

    public <T> T execute(String tableName, TableCallback<T> action, boolean release) {
        HTableInterface table = tablePool.getHTable(tableName);
        T result = null;
        try {
            result = action.doInTable(table);
        } catch (Exception e) {
            LOGGER.error("Error during HBASE operation", e);
            throw new HBaseOperationException(e);
        } finally {
            if (release) {
                releaseHBaseTable(tableName);
            }
        }
        return result;
    }

    private void releaseHBaseTable(String tableName) {
        try {
            tablePool.releaseHTable(tableName);
        } catch (IOException e) {
            throw new HBaseOperationException(e);
        }
    }

}
