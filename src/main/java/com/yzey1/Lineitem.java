package com.yzey1;

public class Lineitem extends DataTuple {
    private int L_ORDERKEY; // Foreign key
    private int L_PARTKEY; // Foreign key
    private int L_SUPPKEY; // Foreign key
    private int L_LINENUMBER;
    private int L_QUANTITY;
    private double L_EXTENDEDPRICE;
    private double L_DISCOUNT;
    private double L_TAX;
    private String L_RETURNFLAG;
    private String L_LINESTATUS;
    private String L_SHIPDATE;
    private String L_COMMITDATE;
    private String L_RECEIPTDATE;
    private String L_SHIPINSTRUCT;
    private String L_SHIPMODE;
    private String L_COMMENT;

    public Lineitem(int id, int L_ORDERKEY, int L_PARTKEY, int L_SUPPKEY, int L_LINENUMBER, int L_QUANTITY, double L_EXTENDEDPRICE, double L_DISCOUNT, double L_TAX, String L_RETURNFLAG, String L_LINESTATUS, String L_SHIPDATE, String L_COMMITDATE, String L_RECEIPTDATE, String L_SHIPINSTRUCT, String L_SHIPMODE, String L_COMMENT) {
        super(id);
        this.L_ORDERKEY = L_ORDERKEY;
        this.L_PARTKEY = L_PARTKEY;
        this.L_SUPPKEY = L_SUPPKEY;
        this.L_LINENUMBER = L_LINENUMBER;
        this.L_QUANTITY = L_QUANTITY;
        this.L_EXTENDEDPRICE = L_EXTENDEDPRICE;
        this.L_DISCOUNT = L_DISCOUNT;
        this.L_TAX = L_TAX;
        this.L_RETURNFLAG = L_RETURNFLAG;
        this.L_LINESTATUS = L_LINESTATUS;
        this.L_SHIPDATE = L_SHIPDATE;
        this.L_COMMITDATE = L_COMMITDATE;
        this.L_RECEIPTDATE = L_RECEIPTDATE;
        this.L_SHIPINSTRUCT = L_SHIPINSTRUCT;
        this.L_SHIPMODE = L_SHIPMODE;
        this.L_COMMENT = L_COMMENT;
    }

    // Getters and setters
}