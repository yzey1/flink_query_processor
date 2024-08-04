package com.yzey1.DataTuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class DataTuple {
    public String[] primaryKey;
    public String[] foreignKeys;
    public Map<String, Object> fields;
    public String fk_value;

    public DataTuple(String[] primary_key, String[] foreign_keys) {
        this.primaryKey = primary_key;
        this.foreignKeys = foreign_keys;
        this.fields = new HashMap<>();
    }

    public Object getField(String fieldName) {
        return fields.get(fieldName);
    }

    public void setField(String fieldName, Object value) {
        fields.put(fieldName, value);
    }

    public abstract String[] getFieldNames() ;
    public abstract String getTableName() ;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataTuple dataTuple = (DataTuple) o;
        return primaryKey == dataTuple.primaryKey;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(primaryKey);
    }
}