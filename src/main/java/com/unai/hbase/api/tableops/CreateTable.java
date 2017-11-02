package com.unai.hbase.api.tableops;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class CreateTable {

    private Logger log = LoggerFactory.getLogger(CreateTable.class);

    private String tableName;
    private Set<String> cfs;
    private Admin admin;

    public CreateTable(String tableName, Admin admin) {
        log.debug("Preparing creation of table {}", tableName);
        this.tableName = tableName;
        this.cfs = new HashSet<>();
        this.admin = admin;
    }

    public CreateTable withCF(String cf) {
        log.debug("Adding column family {} to table {}", cf, this.tableName);
        cfs.add(cf);
        return this;
    }

    public void execute() throws IOException {
        log.info("Creating table with name {} and {} column families",
                this.tableName,
                this.cfs.stream().collect(Collectors.joining(",")));
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        cfs.forEach(cf -> desc.addFamily(new HColumnDescriptor(Bytes.toBytes(cf))));
        admin.createTable(desc);
    }

}
