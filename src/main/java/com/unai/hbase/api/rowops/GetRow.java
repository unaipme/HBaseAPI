package com.unai.hbase.api.rowops;

import com.unai.hbase.api.model.Row;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GetRow {

    private String tableName;
    private String rowId;
    private Connection connection;

    public GetRow(String tableName, String rowId, Connection connection) {
        this.tableName = tableName;
        this.rowId = rowId;
        this.connection = connection;
    }

    public Row execute() throws IOException {
        Get get = new Get(Bytes.toBytes(rowId));
        Result result = connection.getTable(TableName.valueOf(this.tableName)).get(get);
        return Row.fromResult(result);
    }

}
