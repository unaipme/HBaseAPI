package com.unai.hbase.api.model;

import com.unai.hbase.api.exception.CFUndefined;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

public class Row {

    private String rowId;
    private Set<ColumnFamily> cfs;

    private Row(String rowId) {
        this.rowId = rowId;
        this.cfs = new HashSet<>();
    }

    public boolean cfExists(byte [] name) {
        return cfExists(Bytes.toString(name));
    }

    public boolean cfExists(String name) {
        return cfs.stream().anyMatch(cf -> name.equals(cf.getName()));
    }

    public ColumnFamily getCFByName(byte [] name) {
        return getCFByName(Bytes.toString(name));
    }

    public ColumnFamily getCFByName(String name) {
        return cfs.stream().filter(cf -> name.equals(cf.getName())).findFirst().orElseThrow(() -> new CFUndefined(name));
    }

    public void createCF(byte [] name) {
        createCF(Bytes.toString(name));
    }

    public void createCF(String name) {
        cfs.add(new ColumnFamily(name));
    }

    public String getRowId() {
        return this.rowId;
    }

    public Set<ColumnFamily> getCFs() {
        return this.cfs;
    }

    public static Row fromResult(Result result) throws IOException {
        Row row = new Row(Bytes.toString(result.getRow()));
        while (result.advance()) {
            NavigableMap<byte [], NavigableMap<byte [], NavigableMap<Long, byte []>>> map = result.getMap();
            map.forEach((cfb, m1) -> {
                if (!row.cfExists(cfb)) row.createCF(cfb);
                final ColumnFamily cf = row.getCFByName(cfb);
                m1.forEach((qual, m2) -> m2.forEach((ts, value) -> {
                    cf.addValue(qual, ts, value);
                }));
            });
        }
        return row;
    }

}
