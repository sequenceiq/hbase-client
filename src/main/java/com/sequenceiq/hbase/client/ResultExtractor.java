package com.sequenceiq.hbase.client;

import org.apache.hadoop.hbase.client.ResultScanner;

public interface ResultExtractor<T> {
    T extractData(ResultScanner result) throws Exception;
}
