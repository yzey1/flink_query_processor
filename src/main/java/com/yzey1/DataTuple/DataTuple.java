package com.yzey1.DataTuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public abstract class DataTuple {
    public String PRIMARY_KEY;
    public String FOREIGN_KEY;
    public Map<String, Object> fields;
    public String pk_value;
    public String fk_value;

    public DataTuple(String primary_key, String foreign_key) {
        this.PRIMARY_KEY = primary_key;
        this.FOREIGN_KEY = foreign_key;
        this.fields = new HashMap<>();
    }

    public Object getField(String fieldName) {
        return fields.get(fieldName);
    }

    public void setField(String fieldName, Object value) {
        fields.put(fieldName, value);
    }

    public void setPk_value(String pk_value){
        this.pk_value = pk_value;
    }

    public void setFk_value(String fk_value){
        this.fk_value = fk_value;
    }

    public abstract String[] getFieldNames() ;
    public abstract String getTableName() ;

    @Override
    public int hashCode() {
        return Integer.parseInt(pk_value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataTuple dataTuple = (DataTuple) o;
        return Objects.equals(PRIMARY_KEY, dataTuple.PRIMARY_KEY);
    }


}