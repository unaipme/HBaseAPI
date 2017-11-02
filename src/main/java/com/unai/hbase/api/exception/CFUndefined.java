package com.unai.hbase.api.exception;

public class CFUndefined extends RuntimeException {

    public CFUndefined() {
        super("No column family was defined");
    }

    public CFUndefined(String s) {
        super(String.format("Column family %s does no exist", s));
    }

}
