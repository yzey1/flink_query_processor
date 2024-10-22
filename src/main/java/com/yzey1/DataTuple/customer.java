package com.yzey1.DataTuple;

public class customer extends DataTuple {
    public static final String[] FIELD_NAMES = {
            "C_CUSTKEY",
            "C_NAME",
            "C_ADDRESS",
            "C_NATIONKEY",
            "C_PHONE",
            "C_ACCTBAL",
            "C_MKTSEGMENT",
            "C_COMMENT"
    };
    public static final String PRIMARY_KEY = "C_CUSTKEY";
    public static final String FOREIGN_KEY = "C_NATIONKEY";
//    public String pk_value;
//    public String fk_value;

    public customer(Object[] data){
        super(PRIMARY_KEY, FOREIGN_KEY);
        for (int i = 0; i < FIELD_NAMES.length; i++) {
            setField(FIELD_NAMES[i], data[i]);
        }
        setPk_value(getField(PRIMARY_KEY).toString());
        setFk_value(getField(FOREIGN_KEY).toString());
    }

    public customer(customer c){
        super(PRIMARY_KEY, FOREIGN_KEY);
        for (String field : FIELD_NAMES) {
            setField(field, c.getField(field));
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
        return "Customer";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof customer) {
            return pk_value.equals(((customer) obj).pk_value);
        }
        return false;
    }
}