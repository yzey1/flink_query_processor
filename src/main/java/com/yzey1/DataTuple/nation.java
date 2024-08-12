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
        setPk_value(getField(PRIMARY_KEY).toString());
        setFk_value(getField(FOREIGN_KEY).toString());
    }

    @Override
    public String[] getFieldNames() {
        return FIELD_NAMES;
    }

    @Override
    public String getTableName() {
        return "Nation";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof nation) {
            return pk_value.equals(((nation) obj).pk_value);
        }
        return false;
    }

}