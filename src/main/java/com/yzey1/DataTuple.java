package com.yzey1;

import java.util.Objects;

public abstract class DataTuple {
    private int id;

    public DataTuple(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataTuple dataTuple = (DataTuple) o;
        return id == dataTuple.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}