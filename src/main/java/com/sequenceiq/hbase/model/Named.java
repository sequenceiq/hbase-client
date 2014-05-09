package com.sequenceiq.hbase.model;

import java.io.Serializable;

public interface Named extends Serializable {
    String getName();

    byte[] getNameBytes();
}
