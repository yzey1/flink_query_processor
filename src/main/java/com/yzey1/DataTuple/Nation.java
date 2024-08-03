package com.yzey1.DataTuple;

public class Nation extends DataTuple {
    private static final String[] FIELD_NAMES = {
            "N_NATIONKEY",
            "N_NAME",
            "N_REGIONKEY",
            "N_COMMENT"
    };
    private static final String[] PRIMARY_KEY = {"N_NATIONKEY"};
    private static final String[] FOREIGN_KEYS = {"N_REGIONKEY"};

    public Nation(int N_NATIONKEY, String N_NAME, int N_REGIONKEY, String N_COMMENT) {
        super(PRIMARY_KEY, FOREIGN_KEYS);
        setField("N_NATIONKEY", N_NATIONKEY);
        setField("N_NAME", N_NAME);
        setField("N_REGIONKEY", N_REGIONKEY);
        setField("N_COMMENT", N_COMMENT);
    }

    @Override
    public String[] getFieldNames() {
        return FIELD_NAMES;
    }
}