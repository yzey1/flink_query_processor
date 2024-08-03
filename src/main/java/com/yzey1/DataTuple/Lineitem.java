package com.yzey1.DataTuple;

public class Lineitem extends DataTuple {
    private static final String[] FIELD_NAMES = {
            "L_ORDERKEY",
            "L_PARTKEY",
            "L_SUPPKEY",
            "L_LINENUMBER",
            "L_QUANTITY",
            "L_EXTENDEDPRICE",
            "L_DISCOUNT",
            "L_TAX",
            "L_RETURNFLAG",
            "L_LINESTATUS",
            "L_SHIPDATE",
            "L_COMMITDATE",
            "L_RECEIPTDATE",
            "L_SHIPINSTRUCT",
            "L_SHIPMODE",
            "L_COMMENT"
    };
    private static final String[] PRIMARY_KEY = {"L_ORDERKEY","L_LINENUMBER"};
    private static final String[] FOREIGN_KEYS = {"L_ORDERKEY"};

    public Lineitem(int L_ORDERKEY, int L_PARTKEY, int L_SUPPKEY, int L_LINENUMBER, double L_QUANTITY, double L_EXTENDEDPRICE, double L_DISCOUNT, double L_TAX, String L_RETURNFLAG, String L_LINESTATUS, String L_SHIPDATE, String L_COMMITDATE, String L_RECEIPTDATE, String L_SHIPINSTRUCT, String L_SHIPMODE, String L_COMMENT) {
        super(PRIMARY_KEY, FOREIGN_KEYS);
        setField("L_ORDERKEY", L_ORDERKEY);
        setField("L_PARTKEY", L_PARTKEY);
        setField("L_SUPPKEY", L_SUPPKEY);
        setField("L_LINENUMBER", L_LINENUMBER);
        setField("L_QUANTITY", L_QUANTITY);
        setField("L_EXTENDEDPRICE", L_EXTENDEDPRICE);
        setField("L_DISCOUNT", L_DISCOUNT);
        setField("L_TAX", L_TAX);
        setField("L_RETURNFLAG", L_RETURNFLAG);
        setField("L_LINESTATUS", L_LINESTATUS);
        setField("L_SHIPDATE", L_SHIPDATE);
        setField("L_COMMITDATE", L_COMMITDATE);
        setField("L_RECEIPTDATE", L_RECEIPTDATE);
        setField("L_SHIPINSTRUCT", L_SHIPINSTRUCT);
        setField("L_SHIPMODE", L_SHIPMODE);
        setField("L_COMMENT", L_COMMENT);
    }

    @Override
    public String[] getFieldNames() {
        return FIELD_NAMES;
    }
}