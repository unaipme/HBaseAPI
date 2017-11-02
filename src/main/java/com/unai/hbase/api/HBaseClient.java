package com.unai.hbase.api;

import com.unai.hbase.api.model.Cell;
import com.unai.hbase.api.rowops.GetRow;
import com.unai.hbase.api.rowops.PutRow;
import com.unai.hbase.api.tableops.CreateTable;
import com.unai.hbase.api.model.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Map.Entry;

public class HBaseClient implements AutoCloseable {

    private Logger log = LoggerFactory.getLogger(HBaseClient.class);

    private static final int DEFAULT_PORT = 2181;

    private Connection connection;
    private Admin admin;

    public HBaseClient() throws IOException {
        this("localhost");
    }

    public HBaseClient(String... quorum) throws IOException {
        this(DEFAULT_PORT, quorum);
    }

    public HBaseClient(int port, String... quorum) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", Arrays.asList(quorum).stream().collect(Collectors.joining(",")));
        conf.setInt("hbase.zookeeper.property.clientPort", port);
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
    }

    public CreateTable createTable(String tableName) {
        return new CreateTable(tableName, this.admin);
    }

    public boolean tableExists(String tableName) throws IOException {
        log.info("Checking if table {} exists", tableName);
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public boolean disableTable(String tableName) {
        log.info("Dropping table {}", tableName);
        try {
            admin.disableTable(TableName.valueOf(tableName));
            return true;
        } catch (IOException e) {
            log.error(e.getLocalizedMessage());
            return false;
        }
    }

    public boolean dropTable(String tableName, boolean disableFirst) {
        if (disableFirst) disableTable(tableName);
        return dropTable(tableName);
    }

    public boolean dropTable(String tableName) {
        log.info("Dropping table {}", tableName);
        try {
            admin.deleteTable(TableName.valueOf(tableName));
            return true;
        } catch (IOException e) {
            log.error(e.getLocalizedMessage());
            return false;
        }
    }

    public Set<String> listTables() throws IOException {
        log.info("Listing tables in HBase");
        return Arrays.stream(admin.listTableNames()).map(TableName::getNameAsString).collect(Collectors.toSet());
    }

    public PutRow put(String tableName, String rowId) {
        return new PutRow(tableName, rowId, this.connection);
    }

    public List<Row> scan(String tableName) throws IOException {
        List<Row> results = new ArrayList<>();
        try (ResultScanner scanner = connection.getTable(TableName.valueOf(tableName)).getScanner(new Scan())) {
            for (Result result : scanner) results.add(Row.fromResult(result));
        }
        return results;
    }

    public GetRow get(String tableName, String rowId) {
        return new GetRow(tableName, rowId, this.connection);
    }

    public void printRows(List<Row> rows) {
        //int max = rows.stream().map(r -> r.getRowId().length()).max(Comparator.comparingInt(Integer::intValue)).get();
        if (rows.isEmpty()) {
            System.out.println("Empty result set");
            return;
        }
        rows.get(0).getCFs().forEach(c -> System.out.println(String.format("\t%s", c.getName())));
        for (Row row : rows) printRow(row);
    }

    private void printRow(Row row) {
        System.out.println(String.format("\u001B[32m%s", row.getRowId()));
        row.getCFs().forEach(cf -> {
            Iterator<Entry<String, Cell>> it = cf.getColumns().entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, Cell> e = it.next();
                StringBuilder sb = new StringBuilder("\t\u001B[36m");
                sb.append(e.getKey()).append("\u001B[0m=\u001B[35m").append(cf.getLatestStringValue(e.getKey()));
                sb.append("\t\u001B[36mtimestamp\u001B[0m=\u001B[35m").append(cf.getLatestTimestamp(e.getKey()));
                System.out.println(sb.toString());
            }
        });
        System.out.println();
    }

    @Override
    public void close() {
        try {
            admin.close();
            connection.close();
        } catch (IOException e) {
            log.error(e.getLocalizedMessage());
        }
    }

}
