package com.yzey1;

public class Customer extends DataTuple {
    private String C_NAME;
    private String C_ADDRESS;
    private int C_NATIONKEY; // Foreign key
    private String C_PHONE;
    private double C_ACCTBAL;
    private String C_MKTSEGMENT;
    private String C_COMMENT;

    public Customer(int C_CUSTKEY, String C_NAME, String C_ADDRESS, int C_NATIONKEY, String C_PHONE, double C_ACCTBAL, String C_MKTSEGMENT, String C_COMMENT) {
        super(C_CUSTKEY);
        this.C_NAME = C_NAME;
        this.C_ADDRESS = C_ADDRESS;
        this.C_NATIONKEY = C_NATIONKEY;
        this.C_PHONE = C_PHONE;
        this.C_ACCTBAL = C_ACCTBAL;
        this.C_MKTSEGMENT = C_MKTSEGMENT;
        this.C_COMMENT = C_COMMENT;
    }

    // Getters and setters
}