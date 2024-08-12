package com.yzey1.DataTuple;

public class order extends DataTuple {
    public static final String[] FIELD_NAMES = {
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
    public static final String PRIMARY_KEY = "O_ORDERKEY";
    public static final String FOREIGN_KEY = "O_CUSTKEY";

//    public order(){
//        super(PRIMARY_KEY, FOREIGN_KEY);
//    }

    public order(Object[] data){
        super(PRIMARY_KEY, FOREIGN_KEY);
        for (int i = 0; i < FIELD_NAMES.length; i++) {
            setField(FIELD_NAMES[i], data[i]);
        }
        setPk_value(getField(PRIMARY_KEY).toString());
        setFk_value(getField(FOREIGN_KEY).toString());
    }

    @Override
    public String[] getFieldNames() {
        return FIELD_NAMES;
    }

    @Override
    public String getTableName() {
        return "Orders";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof order) {
            return pk_value.equals(((order) obj).pk_value);
        }
        return false;
    }
}