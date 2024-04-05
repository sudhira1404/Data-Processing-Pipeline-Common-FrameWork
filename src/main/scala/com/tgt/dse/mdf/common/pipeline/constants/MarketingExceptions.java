package com.tgt.dse.mdf.common.pipeline.constants;

public enum MarketingExceptions {

    CONTRACT_VALIDATION ("validating schema contract"),
    LOCK_FILE ("creating lock file"),
    DATA_QUALITY ("data quality checks"),
    DATE_FORMAT ("date formatting"),
    FILE_MOVE ("moving files"),
    FILE_COPY ("copying files"),
    SCHEMA ("applying schema"),
    INVALID_PIPELINE_OPTION ("pipeline option"),
    HIVE_WRITE ("writing to hive"),
    DATE_SEQUENCE ("date sequence not ordered"),
    GENERAL ("general exception catch");

    private final String error;

    MarketingExceptions(String error) { this.error = error; }

    public String getError() { return this.error; }
}
