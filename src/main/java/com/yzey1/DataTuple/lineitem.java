package com.yzey1.DataTuple;

public class lineitem extends DataTuple {
    public static final String[] FIELD_NAMES = {
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
    public static final String PRIMARY_KEY = "L_ORDERKEY";
    public static final String FOREIGN_KEY = "L_ORDERKEY";

    public lineitem(Object[] data) {
        super(PRIMARY_KEY, FOREIGN_KEY);
        for (int i = 0; i < FIELD_NAMES.length; i++) {
            setField(FIELD_NAMES[i], data[i]);
        }
        this.pk_value = getField(PRIMARY_KEY).toString();
        this.fk_value = getField(FOREIGN_KEY).toString();
    }

    @Override
    public String[] getFieldNames() {
        return FIELD_NAMES;
    }

    @Override
    public String getTableName() {
        return "Lineitem";
    }


}