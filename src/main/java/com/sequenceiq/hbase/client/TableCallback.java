package com.sequenceiq.hbase.client;

import org.apache.hadoop.hbase.client.HTableInterface;

public interface TableCallback<T> {
    T doInTable(HTableInterface table) throws Exception;
}
