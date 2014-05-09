package com.sequenceiq.hbase.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sequenceiq.hbase.client.HBaseOperations;
import com.sequenceiq.hbase.client.HBaseRow;
import com.sequenceiq.hbase.client.HBaseTablePool;

public class LocalTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTest.class);

    private LocalTest() {
        throw new IllegalStateException();
    }

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        config.set("hadoop.socks.server", "127.0.0.1:1099");
        config.set("hadoop.rpc.socket.factory.class.default", "org.apache.hadoop.net.SocksSocketFactory");
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        HBaseTablePool tablePool = new HBaseTablePool(config);
        final HBaseOperations hBaseOperations = new HBaseOperations(tablePool);
        (new Thread(new Runnable() {
            @Override
            public void run() {
                HBaseRow hBaseRow = hBaseOperations.get("test", "row1", false);
                LOGGER.info(hBaseRow.toString());
            }
        })).start();
    }
}
