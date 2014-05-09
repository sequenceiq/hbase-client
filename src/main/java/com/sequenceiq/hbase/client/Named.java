package com.sequenceiq.hbase.client;

import java.io.Serializable;

public interface Named extends Serializable {
    String getName();

    byte[] getNameBytes();
}
