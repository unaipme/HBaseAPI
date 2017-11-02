package com.unai.hbase.api.model;

import com.unai.hbase.api.exception.QualifierUndefinedException;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class ColumnFamily {

    private String name;
    private Map<String, Cell> columns;

    protected ColumnFamily(String name) {
        this.name = name;
        this.columns = new HashMap<>();
    }

    public String getName() {
        return this.name;
    }

    public Map<String, Cell> getColumns() {
        return this.columns;
    }

    public void addColumn(byte [] qualifier) {
        addColumn(Bytes.toString(qualifier));
    }

    public void addColumn(String qualifier) {
        columns.put(qualifier, new Cell());
    }

    public void addValue(byte [] qual, long timestamp, byte [] value) {
        addValue(Bytes.toString(qual), timestamp, value);
    }

    public void addValue(String qual, long timestamp, byte [] value) {
        if (columns.get(qual) == null) addColumn(qual);
        columns.get(qual).put(timestamp, value);
    }

    public String getStringValue(String qual, Long timestamp) {
        return Bytes.toString(getValue(qual, timestamp));
    }

    public int getIntValue(String qual, Long timestamp) {
        return Bytes.toInt(getValue(qual, timestamp));
    }

    public byte [] getValue(String qual, Long timestamp) {
        return Optional.ofNullable(columns.get(qual))
                .orElseThrow(() -> new QualifierUndefinedException(qual))
                .get(timestamp);
    }

    public String getLatestStringValue(String qual) {
        return Bytes.toString(getLatestValue(qual));
    }

    public int getLatestIntValue(String qual) {
        return Bytes.toInt(getLatestValue(qual));
    }

    public byte [] getLatestValue(String qual) {
        return Optional.ofNullable(columns.get(qual))
                .orElseThrow(() -> new QualifierUndefinedException(qual))
                .entrySet()
                .stream()
                .findFirst()
                .get()
                .getValue();
    }

    public long getLatestTimestamp(String qual) {
        return Optional.ofNullable(columns.get(qual))
                .orElseThrow(() -> new QualifierUndefinedException(qual))
                .getLatestTimestamp();
    }

}
