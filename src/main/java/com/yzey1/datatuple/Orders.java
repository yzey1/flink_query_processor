package com.yzey1.datatuple;

public class Orders extends DataTuple {
    private static final String[] FIELD_NAMES = {
            "O_ORDERKEY",
            "O_CUSTKEY",
            "O_ORDERSTATUS",
            "O_TOTALPRICE",
            "O_ORDERDATE",
            "O_ORDERPRIORITY",
            "O_CLERK",
            "O_SHIPPRIORITY",
            "O_COMMENT"
    };
    private static final String[] PRIMARY_KEY = {"O_ORDERKEY"};
    private static final String[] FOREIGN_KEYS = {"O_CUSTKEY"};

    public Orders(int O_ORDERKEY, int O_CUSTKEY, String O_ORDERSTATUS, double O_TOTALPRICE, String O_ORDERDATE, String O_ORDERPRIORITY, String O_CLERK, int O_SHIPPRIORITY, String O_COMMENT) {
        super(PRIMARY_KEY, FOREIGN_KEYS);
        setField("O_ORDERKEY", O_ORDERKEY);
        setField("O_CUSTKEY", O_CUSTKEY);
        setField("O_ORDERSTATUS", O_ORDERSTATUS);
        setField("O_TOTALPRICE", O_TOTALPRICE);
        setField("O_ORDERDATE", O_ORDERDATE);
        setField("O_ORDERPRIORITY", O_ORDERPRIORITY);
        setField("O_CLERK", O_CLERK);
        setField("O_SHIPPRIORITY", O_SHIPPRIORITY);
        setField("O_COMMENT", O_COMMENT);
    }

    @Override
    public String[] getFieldNames() {
        return FIELD_NAMES;
    }
}