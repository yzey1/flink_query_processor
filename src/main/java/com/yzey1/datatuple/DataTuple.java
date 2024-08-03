package com.yzey1.datatuple;

import java.util.HashMap;
import java.util.Map;

public abstract class DataTuple {
    private String[] primaryKey;
    private String[] foreignKeys;
    private Map<String, Object> fields;

    public DataTuple(String[] primary_key, String[] foreign_keys) {
        this.primaryKey = primary_key;
        this.foreignKeys = foreign_keys;
        this.fields = new HashMap<>();
    }

    public String[] getPrimaryKey() {
        return primaryKey;
    }

    public String[] getForeignKeys() {
        return foreignKeys;
    }

    public Object getField(String fieldName) {
        return fields.get(fieldName);
    }

    public void setField(String fieldName, Object value) {
        fields.put(fieldName, value);
    }

    public abstract String[] getFieldNames() ;

//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        DataTuple dataTuple = (DataTuple) o;
//        return id == dataTuple.id;
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(id);
//    }
}