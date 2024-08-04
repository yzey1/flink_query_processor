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
    public static final String[] PRIMARY_KEY = {"O_ORDERKEY"};
    public static final String[] FOREIGN_KEYS = {"O_CUSTKEY"};
    public String fk_value;

    public order(){
        super(PRIMARY_KEY, FOREIGN_KEYS);
    }

    public order(Object[] data){
        super(PRIMARY_KEY, FOREIGN_KEYS);
        for (int i = 0; i < FIELD_NAMES.length; i++) {
            setField(FIELD_NAMES[i], data[i]);
        }
        this.fk_value = "";
        for (String key : FOREIGN_KEYS) {
            this.fk_value += getField(key);
        }
    }

    @Override
    public String[] getFieldNames() {
        return FIELD_NAMES;
    }

    @Override
    public String getTableName() {
        return "Orders";
    }

}