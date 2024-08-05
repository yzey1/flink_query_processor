package com.yzey1.DataTuple;

public class nation extends DataTuple {
    public static final String[] FIELD_NAMES = {
            "N_NATIONKEY",
            "N_NAME",
            "N_REGIONKEY",
            "N_COMMENT"
    };
    public static final String PRIMARY_KEY = "N_NATIONKEY";
    public static final String FOREIGN_KEY = "N_REGIONKEY";

    public nation(Object[] data){
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
        return "Nation";
    }


}