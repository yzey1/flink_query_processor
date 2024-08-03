package com.yzey1.DataTuple;

public class Customer extends DataTuple {
    private static final String[] FIELD_NAMES = {
            "C_CUSTKEY",
            "C_NAME",
            "C_ADDRESS",
            "C_NATIONKEY",
            "C_PHONE",
            "C_ACCTBAL",
            "C_MKTSEGMENT",
            "C_COMMENT"
    };
    private static final String[] PRIMARY_KEY = {"C_CUSTKEY"};
    private static final String[] FOREIGN_KEYS = {"C_NATIONKEY"};

    public Customer(int C_CUSTKEY, String C_NAME, String C_ADDRESS, int C_NATIONKEY, String C_PHONE, double C_ACCTBAL, String C_MKTSEGMENT, String C_COMMENT) {
        super(PRIMARY_KEY, FOREIGN_KEYS);
        setField("C_CUSTKEY", C_CUSTKEY);
        setField("C_NAME", C_NAME);
        setField("C_ADDRESS", C_ADDRESS);
        setField("C_NATIONKEY", C_NATIONKEY);
        setField("C_PHONE", C_PHONE);
        setField("C_ACCTBAL", C_ACCTBAL);
        setField("C_MKTSEGMENT", C_MKTSEGMENT);
        setField("C_COMMENT", C_COMMENT);
    }

    @Override
    public String[] getFieldNames() {
        return FIELD_NAMES;
    }

}