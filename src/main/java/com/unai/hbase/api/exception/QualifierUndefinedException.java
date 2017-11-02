package com.unai.hbase.api.exception;

public class QualifierUndefinedException extends RuntimeException {

    public QualifierUndefinedException() {
        super("No qualifier was defined");
    }

    public QualifierUndefinedException(String s) {
        super(String.format("Qualifier %s does not exist", s));
    }

}
