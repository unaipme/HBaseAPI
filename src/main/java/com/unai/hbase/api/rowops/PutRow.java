package com.unai.hbase.api.rowops;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PutRow {

    private String tableName;
    private String rowId;
    private Map<String, byte []> values;
    private Connection connection;

    public PutRow(String tableName, String rowId, Connection connection) {
        this.tableName = tableName;
        this.rowId = rowId;
        this.values = new HashMap<>();
        this.connection = connection;
    }

    public PutRowCF in(String cf, String qualifier) {
        return new PutRowCF(String.format("%s:%s", cf, qualifier), this);
    }

    private PutRow add(String cfq, byte [] value) {
        values.put(cfq, value);
        return this;
    }

    public void execute() throws IOException {
        Put put = new Put(Bytes.toBytes(rowId));
        values.forEach((c, v) -> {
            put.addImmutable(Bytes.toBytes(c.split(":")[0]), Bytes.toBytes(c.split(":")[1]), v);
        });
        connection.getTable(TableName.valueOf(this.tableName)).put(put);
    }

    public static class PutRowCF {

        private String cf;
        private PutRow put;

        private PutRowCF(String cf, PutRow put) {
            this.cf = cf;
            this.put = put;
        }

        public PutRow value(String s) {
            return put.add(cf, Bytes.toBytes(s));
        }

        public PutRow value(int i) {
            return put.add(cf, Bytes.toBytes(i));
        }

        public PutRow value(double d) {
            return put.add(cf, Bytes.toBytes(d));
        }

        public PutRow value(boolean b) {
            return put.add(cf, Bytes.toBytes(b));
        }

    }

}
