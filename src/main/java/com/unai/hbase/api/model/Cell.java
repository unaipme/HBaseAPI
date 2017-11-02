package com.unai.hbase.api.model;

import java.util.TreeMap;

public class Cell extends TreeMap<Long, byte []> {

    public byte [] getLatestValue() {
        return this.firstEntry().getValue();
    }

    public Long getLatestTimestamp() {
        return this.firstKey();
    }

}
