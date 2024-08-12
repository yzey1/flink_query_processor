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
        setPk_value(getField(PRIMARY_KEY).toString());
        setFk_value(getField(FOREIGN_KEY).toString());
    }

    @Override
    public String[] getFieldNames() {
        return FIELD_NAMES;
    }

    @Override
    public String getTableName() {
        return "Lineitem";
    }

    @Override
    public int hashCode() {
        return Long.hashCode(Long.parseLong(pk_value + getField("L_PARTKEY").toString()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        lineitem lineitem = (lineitem) o;
        return pk_value.equals(lineitem.pk_value) && getField("L_PARTKEY").equals(lineitem.getField("L_PARTKEY"));
    }
}
