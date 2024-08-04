package com.yzey1.DataTuple;

public class nation extends DataTuple {
    public static final String[] FIELD_NAMES = {
            "N_NATIONKEY",
            "N_NAME",
            "N_REGIONKEY",
            "N_COMMENT"
    };
    public static final String[] PRIMARY_KEY = {"N_NATIONKEY"};
    public static final String[] FOREIGN_KEYS = {"N_REGIONKEY"};

    public nation(Object[] data){
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
        return "Nation";
    }


}