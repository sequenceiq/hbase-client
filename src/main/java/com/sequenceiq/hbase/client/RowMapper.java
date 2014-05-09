package com.sequenceiq.hbase.client;

import org.apache.hadoop.hbase.client.Result;

public interface RowMapper<T> {
    T mapRow(Result result) throws Exception;
}
