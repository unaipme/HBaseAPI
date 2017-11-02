package com.unai.hbase;

import com.unai.hbase.api.HBaseClient;
import com.unai.hbase.api.model.Row;

import java.io.IOException;

public class Main {

    private final static String TABLE = "test_table";
    private final static String CF = "data";

    public static void main(String [] args) {
        try (HBaseClient client = new HBaseClient("ubuntuI", "ubuntuII", "ubuntuIII")) {
            if (!client.tableExists(TABLE)) client.createTable(TABLE).withCF(CF).execute();
            client.put(TABLE, "row1")
                    .in(CF, "info1").value("hola")
                    .in(CF, "info2").value(28)
                    .execute();
            client.put(TABLE, "row2")
                    .in(CF, "info1").value("kaixo")
                    .in(CF, "info3").value(41)
                    .execute();
            client.printRows(client.scan(TABLE));
            Row row = client.get(TABLE, "row2").execute();
            System.out.println(row.getCFByName(CF).getLatestIntValue("info3"));
        } catch (IOException e) {
            System.out.println("Habió un error");
            e.printStackTrace();
        }
    }

}
